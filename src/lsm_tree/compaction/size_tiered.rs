use bincode::{deserialize, serialize};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use entry::Entry;
use lsm_tree::{sstable, SSTable, SSTableBuilder, SSTableDataIter, SSTableValue, Result};
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
    curr_logical_time: u64,
    logical_time_file: fs::File,
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
        let logical_time_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path.as_ref().join("logical_time.dat"))?;
        let mut ret = SizeTieredStrategy {
            db_path: PathBuf::from(db_path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            curr_logical_time: 0,
            logical_time_file,
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
        let mut logical_time_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path.as_ref().join("logical_time.dat"))?;
        let mut buffer = Vec::new();
        metadata_file.read_to_end(&mut buffer)?;
        logical_time_file.seek(SeekFrom::Start(0))?;
        Ok(SizeTieredStrategy {
            db_path: PathBuf::from(db_path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            curr_logical_time: logical_time_file.read_u64::<BigEndian>()?,
            logical_time_file,
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
        let next_metadata = self.next_metadata.clone();
        let is_compacting = self.is_compacting.clone();
        self.is_compacting.store(true, Ordering::Release);
        self.compaction_thread_join_handle = Some(thread::spawn(move || {
            let compaction_result = (|| -> Result<()> {
                println!("Started compacting.");
                let old_sstables: Vec<Arc<SSTable<T, U>>> = metadata.sstables
                    .drain((Bound::Included(range.0), Bound::Excluded(range.1)))
                    .collect();

                let sstable_max_logical_time_range = old_sstables
                    .iter()
                    .fold(None, |max_logical_time, sstable| {
                        cmp::max(
                            max_logical_time,
                            match sstable.summary.logical_time_range {
                                Some((_, r)) => Some(r),
                                None => None,
                            },
                        )
                    });
                let sstable_key_range = old_sstables
                    .iter()
                    .fold(None, |range, sstable| {
                        let sstable_range = sstable.summary.key_range.clone();
                        sstable::merge_ranges(range, sstable_range)
                    });
                let purge_tombstone = metadata.sstables
                    .iter()
                    .all(|sstable| {
                        let is_older_range = {
                            match sstable.summary.logical_time_range {
                                Some((l, _)) => sstable_max_logical_time_range < Some(l),
                                None => true,
                            }
                        };
                        let key_intersecting = sstable::is_intersecting(
                            &sstable_key_range,
                            &sstable.summary.key_range,
                        );
                        is_older_range && !key_intersecting
                    });

                let sstable_logical_time = {
                    match old_sstables.iter().map(|sstable| sstable.summary.logical_time).max() {
                        Some(logical_time) => logical_time,
                        _ => unreachable!(),
                    }
                };

                let mut new_sstable_builder: SSTableBuilder<T, U> = SSTableBuilder::new(
                    db_path,
                    old_sstables.iter().map(|sstable| sstable.summary.entry_count).sum(),
                    sstable_logical_time,
                )?;

                let mut old_sstable_data_iters = Vec::with_capacity(old_sstables.len());
                for sstable in &old_sstables {
                    old_sstable_data_iters.push(sstable.data_iter()?);
                }

                drop(old_sstables);

                let mut entries = BinaryHeap::new();
                let mut last_key_opt = None;

                for (index, sstable_data_iter) in old_sstable_data_iters.iter_mut().enumerate() {
                    if let Some(entry) = sstable_data_iter.next() {
                        let entry = entry?;
                        entries.push(cmp::Reverse((entry.key, entry.value, index)));
                    }
                }

                while let Some(cmp::Reverse((key, value, index))) = entries.pop() {
                    if let Some(entry) = old_sstable_data_iters[index].next() {
                        let Entry { key, value } = entry?;
                        entries.push(cmp::Reverse((key, value, index)));
                    }

                    let should_append = {
                        match last_key_opt {
                            Some(ref last_key) => *last_key != key,
                            None => true,
                        }
                    } && (!purge_tombstone || value.data.is_some());

                    if purge_tombstone && value.data.is_none() {
                        println!("Purged");
                    }

                    if should_append {
                        new_sstable_builder.append(key.clone(), value)?;
                    }

                    last_key_opt = Some(key);
                }

                let new_sstable = Arc::new(SSTable::new(new_sstable_builder.flush()?)?);
                metadata.sstables.push(new_sstable);

                println!("Locking in compaction");
                *next_metadata.lock().unwrap() = Some(metadata);

                is_compacting.store(false, Ordering::Release);
                println!("Finished compacting");
                Ok(())
            })();

            if compaction_result.is_err() {
                is_compacting.store(false, Ordering::Release);
            }
        }));
    }

    fn try_replace_metadata(&self, curr_metadata: &mut MutexGuard<SizeTieredMetadata<T, U>>) -> Result<bool> {
        let mut next_metadata = self.next_metadata.lock().unwrap();

        if let Some(next_metadata) = next_metadata.take() {
            let logical_time_opt = next_metadata.sstables
                .iter()
                .map(|sstable| sstable.summary.logical_time)
                .max();
            let old_sstables = mem::replace(&mut curr_metadata.sstables, next_metadata.sstables);
            curr_metadata.sstables.extend(
                old_sstables
                    .iter()
                    .filter(|sstable| {
                        match logical_time_opt {
                            Some(logical_time) => sstable.summary.logical_time > logical_time,
                            None => true,
                        }
                    })
                    .map(|sstable| Arc::clone(sstable)),
            );
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

    fn get_and_increment_logical_time(&mut self) -> Result<u64> {
        let ret = self.curr_logical_time;
        self.curr_logical_time += 1;
        self.logical_time_file.seek(SeekFrom::Start(0))?;
        self.logical_time_file.write_u64::<BigEndian>(self.curr_logical_time)?;
        Ok(ret)
    }

    fn try_compact(&mut self, sstable: SSTable<T, U>) -> Result<()> {
        // taking snapshot of current metadata
        let mut metadata_snapshot = {
            let mut curr_metadata = self.curr_metadata.lock().unwrap();
            self.try_replace_metadata(&mut curr_metadata)?;
            curr_metadata.sstables.push(Arc::new(sstable));
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
            curr_metadata.clone()
        };

        if self.is_compacting.load(Ordering::Acquire) || *self.metadata_lock_count.borrow() != 0 {
            return Ok(());
        }

        metadata_snapshot.sstables.sort_by_key(|sstable| sstable.summary.size);
        if let Some(range) = self.get_compaction_range(&metadata_snapshot) {
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

    fn get(&mut self, key: &T) -> Result<Option<SSTableValue<U>>> {
        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        if self.try_replace_metadata(&mut curr_metadata)? {
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
        }

        let mut ret = None;

        for sstable in curr_metadata.sstables.iter() {
            let res = sstable.get(key)?;
            if res.is_some() && (ret.is_none() || res < ret) {
                ret = res;
            }
        }

        Ok(ret)
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
                let Entry { key, value } = entry?;
                entries.push(cmp::Reverse((key, value, index)));
            }
        }

        Ok(Box::new(SizeTieredIter {
            sstable_lock_count: Rc::clone(&self.metadata_lock_count),
            sstable_data_iters,
            entries,
            last_key_opt: None,
        }))
    }
}

type EntryIndex<T, U> = cmp::Reverse<(T, SSTableValue<U>, usize)>;
struct SizeTieredIter<T, U> {
    sstable_lock_count: Rc<RefCell<u64>>,
    sstable_data_iters: Vec<SSTableDataIter<T, U>>,
    entries: BinaryHeap<EntryIndex<T, U>>,
    last_key_opt: Option<T>,
}

impl<T, U> Iterator for SizeTieredIter<T, U>
where
    T: Clone + DeserializeOwned + Ord,
    U: DeserializeOwned,
{
    type Item = Result<(T, U)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(cmp::Reverse((key, value, index))) = self.entries.pop() {
            if let Some(entry) = self.sstable_data_iters[index].next() {
                match entry {
                    Ok(entry) => self.entries.push(cmp::Reverse((entry.key, entry.value, index))),
                    Err(error) => return Some(Err(error)),
                }
            }

            if let Some(data) = value.data {
                let should_return = {
                    match self.last_key_opt {
                        Some(ref last_key) => *last_key != key,
                        None => true,
                    }
                };

                self.last_key_opt = Some(key.clone());

                if should_return {
                    return Some(Ok((key, data)));
                }
            } else {
                self.last_key_opt = Some(key.clone());
            }
        }
        None
    }
}

impl<T, U> Drop for SizeTieredIter<T, U> {
    fn drop(&mut self) {
        *self.sstable_lock_count.borrow_mut() -= 1;
    }
}
