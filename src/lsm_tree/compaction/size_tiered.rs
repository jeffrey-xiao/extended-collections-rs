use bincode::{deserialize, serialize};
use entry::Entry;
use lsm_tree::{SSTable, SSTableBuilder, SSTableDataIter, SSTableValue, Result};
use lsm_tree::compaction::{CompactionIter, CompactionStrategy};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::collections::{BinaryHeap, Bound, HashSet};
use std::cell::RefCell;
use std::cmp;
use std::fs;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::FromIterator;
use std::marker::Send;
use std::mem;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "T: DeserializeOwned, U: DeserializeOwned"))]
struct SizeTieredMetadata<T, U> {
    min_sstable_count: usize,
    min_sstable_size: u64,
    bucket_low: f64,
    bucket_high: f64,
    max_in_memory_size: u64,
    sstables: Vec<Arc<SSTable<T, U>>>,
}

impl<T, U> SizeTieredMetadata<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    pub fn new(
        min_sstable_count: usize,
        min_sstable_size: u64,
        bucket_low: f64,
        bucket_high: f64,
        max_in_memory_size: u64,
    ) -> Self {
        SizeTieredMetadata {
            min_sstable_count,
            min_sstable_size,
            bucket_low,
            bucket_high,
            max_in_memory_size,
            sstables: Vec::new(),
        }
    }
}

/// A compaction strategy based on bucketing SSTables by their sizes and then compacting buckets
/// when they become too full.
///
/// # Configuration Parameters
///  - `min_sstable_count`: The minimum number of SSTables in a bucket for a compaction to trigger.
///  - `min_sstable_size`: The size threshold for the first bucket. All SSTables with size
///  smaller than `min_sstable_size` will be bucketed into the first bucket.
///  - `bucket_low`: SSTables in a bucket other than the first must have size greater than or equal
///  to `bucket_low * bucket_average` where `bucket_average` is the average of the bucket.
///  - `bucket_high`: SSTables in a bucket other than the first must have size smaller than or equal
///  to `bucket_high * bucket_average` where `bucket_average` is the average of the bucket.
///  - `max_in_memory_size`: The maximum size of the in-memory tree before it must be flushed onto
///  disk as a SSTable.
pub struct SizeTieredStrategy<T, U>
where
    T: DeserializeOwned,
    U: DeserializeOwned,
{
    db_path: PathBuf,
    compaction_thread_join_handle: Option<thread::JoinHandle<()>>,
    is_compacting: Arc<AtomicBool>,
    metadata_lock_count: Rc<RefCell<u64>>,
    metadata_file: fs::File,
    curr_metadata: Arc<Mutex<SizeTieredMetadata<T, U>>>,
    next_metadata: Arc<Mutex<Option<SizeTieredMetadata<T, U>>>>,
}

impl<T, U> SizeTieredStrategy<T, U>
where
    T: 'static + Clone + Hash + DeserializeOwned + Ord + Send + Serialize + Sync,
    U: 'static + Clone + DeserializeOwned + Send + Serialize + Sync,
{
    /// Constructs a new `SizeTieredStrategy<T, U>` with specific configuration parameters.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    ///
    /// let sts: SizeTieredStrategy<u32, u32> = SizeTieredStrategy::new("size_tiered_metadata_new", 4, 50000, 0.5, 1.5, 10000)?;
    /// # fs::remove_dir_all("size_tiered_metadata_new")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn new<P>(
        db_path: P,
        min_sstable_count: usize,
        min_sstable_size: u64,
        bucket_low: f64,
        bucket_high: f64,
        max_in_memory_size: u64,
    ) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        fs::create_dir(db_path.as_ref())?;
        let metadata_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path.as_ref().join("metadata.dat"))?;
        let mut ret = SizeTieredStrategy {
            db_path: PathBuf::from(db_path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            metadata_lock_count: Rc::new(RefCell::new(0)),
            metadata_file,
            curr_metadata: Arc::new(Mutex::new(SizeTieredMetadata::new(
                min_sstable_count,
                min_sstable_size,
                bucket_low,
                bucket_high,
                max_in_memory_size,
            ))),
            next_metadata: Arc::new(Mutex::new(None)),
        };

        {
            let curr_metadata = ret.curr_metadata.lock().unwrap();
            ret.metadata_file.seek(SeekFrom::Start(0))?;
            ret.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
        }

        Ok(ret)
    }

    /// Opens an existing `SizeTieredStrategy<T, U>` from a folder.
    ///
    /// # Examples
    /// ```no run
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    ///
    /// let sts: SizeTieredStrategy<u32, u32> = SizeTieredStrategy::open("size_tiered_metadata_open")?;
    /// # fs::remove_dir_all("size_tiered_metadata_open")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn open<P>(db_path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut metadata_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path.as_ref().join("metadata.dat"))?;
        let mut buffer = Vec::new();
        metadata_file.read_to_end(&mut buffer)?;
        Ok(SizeTieredStrategy {
            db_path: PathBuf::from(db_path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            metadata_lock_count: Rc::new(RefCell::new(0)),
            metadata_file,
            curr_metadata: Arc::new(Mutex::new(deserialize(&buffer)?)),
            next_metadata: Arc::new(Mutex::new(None)),
        })
    }

    fn get_compaction_range(&self, metadata: &SizeTieredMetadata<T, U>) -> Option<(usize, usize)> {
        let mut start = 0;
        let mut curr = 0;
        let mut range_size = 0;
        while curr < metadata.sstables.len() {
            let start_size = metadata.sstables[start].summary.size;
            let curr_size = metadata.sstables[curr].summary.size;
            let curr_avg = (range_size + curr_size) as f64 / (curr - start + 1) as f64;

            let in_min_bucket = curr_size <= metadata.min_sstable_size;
            let in_bucket = curr_avg * metadata.bucket_low <= start_size as f64
                && curr_size as f64 <= curr_avg * metadata.bucket_high;

            curr += 1;
            if in_min_bucket || in_bucket {
                range_size += curr_size;
            } else if curr - start > metadata.min_sstable_count {
                return Some((start, curr));
            } else {
                range_size = 0;
                start = curr;
            }
        }

        if curr - start > metadata.min_sstable_count {
            Some((start, curr))
        } else {
            None
        }
    }

    fn spawn_compaction_thread(
        &mut self,
        mut metadata: SizeTieredMetadata<T, U>,
        range: (usize, usize),
    ) {
        let db_path = self.db_path.clone();
        let curr_metadata = self.curr_metadata.clone();
        let next_metadata = self.next_metadata.clone();
        let is_compacting = self.is_compacting.clone();
        self.compaction_thread_join_handle = Some(thread::spawn(move || {
            println!("Started compacting.");
            let mut old_sstables: Vec<Arc<SSTable<T, U>>> = metadata.sstables
                .drain((Bound::Included(range.0), Bound::Excluded(range.1)))
                .collect();
            old_sstables.sort_by_key(|sstable| sstable.summary.tag);

            let new_tag = {
                match old_sstables.iter().map(|sstable| sstable.summary.tag).max() {
                    Some(new_tag) => new_tag,
                    _ => unreachable!(),
                }
            };

            let newest_tag_opt = metadata.sstables.iter().map(|sstable| sstable.summary.tag).min();

            let mut new_sstable_builder: SSTableBuilder<T, U> = SSTableBuilder::new(
                db_path,
                old_sstables.iter().map(|sstable| sstable.summary.entry_count).sum(),
            ).unwrap();

            let mut old_sstable_data_iters: Vec<SSTableDataIter<T, U>> = old_sstables
                .iter()
                .map(|sstable| sstable.data_iter().unwrap())
                .collect();

            drop(old_sstables);

            let mut entries = BinaryHeap::new();
            let mut last_entry_opt = None;

            for (index, sstable_data_iter) in old_sstable_data_iters.iter_mut().enumerate() {
                if let Some(entry) = sstable_data_iter.next() {
                    let entry = entry.unwrap();
                    entries.push((cmp::Reverse(entry), index));
                }
            }

            while let Some((cmp::Reverse(entry), index)) = entries.pop() {
                if let Some(entry) = old_sstable_data_iters[index].next() {
                    entries.push((cmp::Reverse(entry.unwrap()), index));
                }

                let should_append = {
                    match last_entry_opt {
                        Some(ref last_entry) => *last_entry != entry.key,
                        None => true,
                    }
                } && {
                    match newest_tag_opt {
                        Some(newest_tag) => new_tag > newest_tag || entry.value.is_some(),
                        None => entry.value.is_some(),
                    }
                };

                if should_append {
                    new_sstable_builder.append(entry.key.clone(), entry.value).unwrap();
                }

                last_entry_opt = Some(entry.key);
            }

            new_sstable_builder.summary.tag = new_tag;
            let new_sstable = Arc::new(SSTable::new(new_sstable_builder.flush().unwrap()).unwrap());
            metadata.sstables.push(new_sstable);

            let curr_metadata = curr_metadata.lock().unwrap();
            metadata.sstables.extend(
                curr_metadata.sstables
                    .iter()
                    .filter(|sstable| sstable.summary.tag > new_tag)
                    .map(|sstable| Arc::clone(sstable)),
            );

            *next_metadata.lock().unwrap() = Some(metadata);

            is_compacting.store(false, Ordering::Release);
            println!("Finished compacting");
        }));
        self.is_compacting.store(true, Ordering::Release);
    }

    fn try_replace_metadata(&self, curr_metadata: &mut MutexGuard<SizeTieredMetadata<T, U>>) -> Result<bool> {
        let mut next_metadata = self.next_metadata.lock().unwrap();

        if let Some(next_metadata) = next_metadata.take() {
            let old_sstables = mem::replace(&mut curr_metadata.sstables, next_metadata.sstables);
            let new_sstable_paths: HashSet<&PathBuf> = HashSet::from_iter(
                curr_metadata.sstables
                    .iter()
                    .map(|sstable| &sstable.path),
            );

            for old_sstable in old_sstables {
                if !new_sstable_paths.contains(&old_sstable.path) {
                    fs::remove_dir_all(old_sstable.path.as_path())?;
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<T, U> CompactionStrategy<T, U> for SizeTieredStrategy<T, U>
where
    T: 'static + Clone + Hash + DeserializeOwned + Ord + Send + Serialize + Sync,
    U: 'static + Clone + DeserializeOwned + Send + Serialize + Sync,
{
    fn get_db_path(&self) -> &Path {
        self.db_path.as_path()
    }

    fn get_max_in_memory_size(&self) -> u64 {
        self.curr_metadata.lock().unwrap().max_in_memory_size
    }

    fn try_compact(&mut self, mut sstable: SSTable<T, U>) -> Result<()> {
        let mut metadata_snapshot = {
            let mut curr_metadata = self.curr_metadata.lock().unwrap();
            self.try_replace_metadata(&mut curr_metadata)?;
            sstable.summary.tag = {
                let sstable = curr_metadata.sstables
                    .iter()
                    .max_by_key(|sstable| sstable.summary.tag);
                match sstable {
                    Some(sstable) => sstable.summary.tag + 1,
                    None => 0,
                }
            };
            curr_metadata.sstables.push(Arc::new(sstable));
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
            curr_metadata.clone()
        };

        if self.is_compacting.load(Ordering::Acquire) {
            return Ok(());
        }

        if *self.metadata_lock_count.borrow() != 0 {
            println!("SKipped compaction because of lock");
            return Ok(());
        }

        metadata_snapshot.sstables.sort_by_key(|sstable| sstable.summary.size);
        if let Some(range) = self.get_compaction_range(&metadata_snapshot) {
            // taking snapshot of current metadata
            self.spawn_compaction_thread(metadata_snapshot, range);
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(compaction_thread_join_handle) = self.compaction_thread_join_handle.take() {
            match compaction_thread_join_handle.join() {
                Ok(_) => println!("Child thread terminated successfully."),
                Err(error) => println!("Child thread terminated with error: {:?}", error),
            }

            let mut curr_metadata = self.curr_metadata.lock().unwrap();
            if self.try_replace_metadata(&mut curr_metadata)? {
                self.metadata_file.seek(SeekFrom::Start(0))?;
                self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
            }
        }
        Ok(())
    }

    fn get(&mut self, key: &T) -> Result<Option<U>> {
        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        if self.try_replace_metadata(&mut curr_metadata)? {
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
        }

        for sstable in curr_metadata.sstables.iter().rev() {
            match sstable.get(key)? {
                SSTableValue::Value(value) => return Ok(Some(value)),
                SSTableValue::Tombstone => return Ok(None),
                _ => (),
            }
        }

        Ok(None)
    }

    fn len_hint(&mut self) -> Result<usize> {
        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        if self.try_replace_metadata(&mut curr_metadata)? {
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
        }

        let len_hint = curr_metadata.sstables
            .iter()
            .map(|sstable| sstable.summary.entry_count - sstable.summary.tombstone_count)
            .sum();

        Ok(len_hint)
    }

    fn len(&mut self) -> Result<usize> {
        Ok(self.iter()?.count())
    }

    fn is_empty(&mut self) -> Result<bool> {
        self.len().map(|len| len == 0)
    }

    fn clear(&mut self) -> Result<()> {
        if let Some(compaction_thread_join_handle) = self.compaction_thread_join_handle.take() {
            match compaction_thread_join_handle.join() {
                Ok(_) => println!("Child thread terminated successfully."),
                Err(error) => println!("Child thread terminated with error: {:?}", error),
            }
        }


        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        let mut next_metadata = self.next_metadata.lock().unwrap();
        curr_metadata.sstables.clear();
        *next_metadata = None;

        for dir_entry in fs::read_dir(self.db_path.as_path())? {
            let dir_path = dir_entry?.path();
            if dir_path.is_dir() {
                fs::remove_dir_all(dir_path)?;
            }
        }

        self.metadata_file.seek(SeekFrom::Start(0))?;
        self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;

        Ok(())
    }

    fn min(&mut self) -> Result<Option<T>> {
        match self.iter()?.next() {
            Some(entry) => Ok(Some(entry?.0)),
            None => Ok(None),
        }
    }

    fn max(&mut self) -> Result<Option<T>> {
        match self.iter()?.last() {
            Some(entry) => Ok(Some(entry?.0)),
            None => Ok(None),
        }
    }

    fn iter(&mut self) -> Result<Box<CompactionIter<T, U>>> {
        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        if self.try_replace_metadata(&mut curr_metadata)? {
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
        }

        *self.metadata_lock_count.borrow_mut() += 1;

        let mut sstable_data_iters = Vec::with_capacity(curr_metadata.sstables.len());

        for sstable in &curr_metadata.sstables {
            sstable_data_iters.push(sstable.data_iter()?);
        }
        let mut entries = BinaryHeap::new();

        for (index, sstable_data_iter) in sstable_data_iters.iter_mut().enumerate() {
            if let Some(entry) = sstable_data_iter.next() {
                entries.push((cmp::Reverse(entry?), index));
            }
        }

        Ok(Box::new(SizeTieredIter {
            sstable_lock_count: Rc::clone(&self.metadata_lock_count),
            sstable_data_iters,
            entries,
            last_entry_opt: None,
        }))
    }
}

type EntryIndex<T, U> = (cmp::Reverse<Entry<T, Option<U>>>, usize);
struct SizeTieredIter<T, U> {
    sstable_lock_count: Rc<RefCell<u64>>,
    sstable_data_iters: Vec<SSTableDataIter<T, U>>,
    entries: BinaryHeap<EntryIndex<T, U>>,
    last_entry_opt: Option<T>,
}

impl<T, U> Iterator for SizeTieredIter<T, U>
where
    T: DeserializeOwned + Ord,
    U: DeserializeOwned,
{
    type Item = Result<(T, U)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((cmp::Reverse(entry), index)) = self.entries.pop() {
            if let Some(entry) = self.sstable_data_iters[index].next() {
                match entry {
                    Ok(entry) => self.entries.push((cmp::Reverse(entry), index)),
                    Err(error) => return Some(Err(error)),
                }
            }
            if let Some(value) = entry.value {
                let should_return = {
                    match self.last_entry_opt {
                        Some(ref last_entry) => *last_entry != entry.key,
                        None => true,
                    }
                };

                if should_return {
                    return Some(Ok((entry.key, value)));
                }
            }

            self.last_entry_opt = Some(entry.key);

        }

        None
    }
}

impl<T, U> Drop for SizeTieredIter<T, U> {
    fn drop(&mut self) {
        *self.sstable_lock_count.borrow_mut() -= 1;
    }
}
