use bincode::{deserialize, serialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use entry::Entry;
use lsm_tree::compaction::{CompactionIter, CompactionStrategy};
use lsm_tree::{sstable, Result, SSTable, SSTableBuilder, SSTableDataIter, SSTableValue};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::borrow::Borrow;
use std::cell::Cell;
use std::cmp;
use std::collections::{BinaryHeap, HashSet};
use std::fs;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::FromIterator;
use std::marker::Send;
use std::mem;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "T: DeserializeOwned, U: DeserializeOwned"))]
struct SizeTieredMetadata<T, U> {
    max_in_memory_size: u64,
    max_sstable_count: usize,
    min_sstable_size: u64,
    bucket_low: f64,
    bucket_high: f64,
    sstables: Vec<Arc<SSTable<T, U>>>,
}

impl<T, U> SizeTieredMetadata<T, U> {
    pub fn new(
        max_in_memory_size: u64,
        max_sstable_count: usize,
        min_sstable_size: u64,
        bucket_low: f64,
        bucket_high: f64,
    ) -> Self {
        SizeTieredMetadata {
            max_in_memory_size,
            max_sstable_count,
            min_sstable_size,
            bucket_low,
            bucket_high,
            sstables: Vec::new(),
        }
    }

    pub fn push_sstable(&mut self, sstable: Arc<SSTable<T, U>>) {
        self.sstables.push(sstable);
    }

    pub fn get_compaction_range(&mut self) -> Option<(usize, usize)> {
        self.sstables.sort_by_key(|sstable| sstable.summary.size);

        let mut start = 0;
        let mut curr = 0;
        let mut range_size = 0;
        while curr < self.sstables.len() {
            let start_size = self.sstables[start].summary.size;
            let curr_size = self.sstables[curr].summary.size;
            let curr_avg = (range_size + curr_size) as f64 / (curr - start + 1) as f64;

            let in_min_bucket = curr_size <= self.min_sstable_size;
            let in_bucket = curr_avg * self.bucket_low <= start_size as f64
                && curr_size as f64 <= curr_avg * self.bucket_high;

            curr += 1;
            if in_min_bucket || in_bucket {
                range_size += curr_size;
            } else if curr - start > self.max_sstable_count {
                return Some((start, curr));
            } else {
                range_size = 0;
                start = curr;
            }
        }

        if curr - start > self.max_sstable_count {
            Some((start, curr))
        } else {
            None
        }
    }

    fn compact<P>(&mut self, path: P, range: (usize, usize)) -> Result<()>
    where
        T: Clone + DeserializeOwned + Hash + Ord + Serialize,
        U: DeserializeOwned + Serialize,
        P: AsRef<Path>,
    {
        let old_sstables: Vec<_> = self.sstables
            .drain(range.0..range.1)
            .collect();

        let sstable_max_logical_time_range = old_sstables
            .iter()
            .map(|sstable| sstable.summary.logical_time_range.1)
            .max();
        let sstable_key_range = old_sstables.iter().fold(None, |range, sstable| {
            let sstable_range = sstable.summary.key_range.clone();
            match range {
                Some(range) => Some(sstable::merge_ranges(range, sstable_range)),
                None => Some(sstable_range),
            }
        });
        let purge_tombstone = self.sstables.iter().all(|sstable| {
            let curr_logical_time_range = Some(sstable.summary.logical_time_range.0);
            let is_older_range = sstable_max_logical_time_range < curr_logical_time_range;
            let key_intersecting = match &sstable_key_range {
                Some(ref sstable_key_range) => {
                    sstable::is_intersecting(
                        &sstable_key_range,
                        &sstable.summary.key_range,
                    )
                },
                None => false,
            };
            is_older_range && !key_intersecting
        });

        let mut sstable_builder = SSTableBuilder::new(
            path.as_ref(),
            old_sstables.iter().map(|sstable| sstable.summary.entry_count).sum(),
        )?;

        let old_sstable_data_iters = old_sstables
            .iter()
            .map(|sstable| sstable.data_iter())
            .collect();

        drop(old_sstables);

        let compaction_iter = SizeTieredIter::new(None, old_sstable_data_iters)?;
        for entry in compaction_iter {
            let (key, value) = entry?;

            if !purge_tombstone || value.data.is_some() {
                sstable_builder.append(key, value)?;
            }
        }

        if sstable_builder.key_range.is_some() {
            self.push_sstable(Arc::new(SSTable::new(sstable_builder.flush()?)?));
        }

        Ok(())
    }
}

/// A compaction strategy based on bucketing SSTables by their sizes and then compacting buckets
/// when they become too full.
///
/// # Configuration Parameters
///  - `max_in_memory_size`: The maximum size of the in-memory tree before it must be flushed onto
///  disk as a SSTable.
///  - `max_sstable_count`: The minimum number of SSTables in a bucket before a compaction is
///  triggered.
///  - `min_sstable_size`: The size threshold for the first bucket. All SSTables with size
///  smaller than `min_sstable_size` will be bucketed into the first bucket.
///  - `bucket_low`: SSTables in a bucket other than the first must have size greater than or equal
///  to `bucket_low * bucket_average` where `bucket_average` is the average of the bucket.
///  - `bucket_high`: SSTables in a bucket other than the first must have size smaller than or equal
///  to `bucket_high * bucket_average` where `bucket_average` is the average of the bucket.
pub struct SizeTieredStrategy<T, U> {
    path: PathBuf,
    compaction_thread_join_handle: Option<thread::JoinHandle<()>>,
    is_compacting: Arc<AtomicBool>,
    curr_logical_time: u64,
    logical_time_file: fs::File,
    metadata_lock_count: Rc<Cell<u64>>,
    metadata_file: fs::File,
    curr_metadata: Arc<Mutex<SizeTieredMetadata<T, U>>>,
    next_metadata: Arc<Mutex<Option<SizeTieredMetadata<T, U>>>>,
}

impl<T, U> SizeTieredStrategy<T, U> {
    /// Constructs a new `SizeTieredStrategy<T, U>` with specific configuration parameters.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    ///
    /// let sts: SizeTieredStrategy<u32, u32> = SizeTieredStrategy::new("size_tiered_strategy_new", 10000, 4, 50000, 0.5, 1.5)?;
    /// # fs::remove_dir_all("size_tiered_strategy_new")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn new<P>(
        path: P,
        max_in_memory_size: u64,
        max_sstable_count: usize,
        min_sstable_size: u64,
        bucket_low: f64,
        bucket_high: f64,
    ) -> Result<Self>
    where
        T: Serialize,
        U: Serialize,
        P: AsRef<Path>,
    {
        fs::create_dir(path.as_ref())?;

        let metadata_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref().join("metadata.dat"))?;
        let logical_time_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref().join("logical_time.dat"))?;
        let mut ret = SizeTieredStrategy {
            path: PathBuf::from(path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            curr_logical_time: 0,
            logical_time_file,
            metadata_lock_count: Rc::new(Cell::new(0)),
            metadata_file,
            curr_metadata: Arc::new(Mutex::new(SizeTieredMetadata::new(
                max_in_memory_size,
                max_sstable_count,
                min_sstable_size,
                bucket_low,
                bucket_high,
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
    /// ```no_run
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::SizeTieredStrategy;
    ///
    /// let sts: SizeTieredStrategy<u32, u32> = SizeTieredStrategy::open("size_tiered_strategy_open")?;
    /// # fs::remove_dir_all("size_tiered_strategy_open")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn open<P>(path: P) -> Result<Self>
    where
        T: DeserializeOwned,
        U: DeserializeOwned,
        P: AsRef<Path>,
    {
        let mut metadata_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref().join("metadata.dat"))?;
        let mut logical_time_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref().join("logical_time.dat"))?;
        let mut buffer = Vec::new();
        metadata_file.read_to_end(&mut buffer)?;
        logical_time_file.seek(SeekFrom::Start(0))?;
        Ok(SizeTieredStrategy {
            path: PathBuf::from(path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            curr_logical_time: logical_time_file.read_u64::<BigEndian>()?,
            logical_time_file,
            metadata_lock_count: Rc::new(Cell::new(0)),
            metadata_file,
            curr_metadata: Arc::new(Mutex::new(deserialize(&buffer)?)),
            next_metadata: Arc::new(Mutex::new(None)),
        })
    }

    fn compact<P>(
        path: P,
        is_compacting: &Arc<AtomicBool>,
        mut metadata_snapshot: SizeTieredMetadata<T, U>,
        next_metadata: &Arc<Mutex<Option<SizeTieredMetadata<T, U>>>>,
        range: (usize, usize),
    ) -> Result<()>
    where
        T: Clone + DeserializeOwned + Hash + Ord + Serialize,
        U: DeserializeOwned + Serialize,
        P: AsRef<Path>,
    {
        println!("Started compacting.");

        metadata_snapshot.compact(path, range)?;
        *next_metadata.lock().unwrap() = Some(metadata_snapshot);
        is_compacting.store(false, Ordering::Release);

        println!("Finished compacting");
        Ok(())
    }

    fn spawn_compaction_thread(
        &mut self,
        metadata_snapshot: SizeTieredMetadata<T, U>,
        range: (usize, usize),
    )
    where
        T: 'static + Clone + DeserializeOwned + Hash + Ord + Send + Serialize + Sync,
        U: 'static + DeserializeOwned + Send + Serialize + Sync,
    {
        let path = self.path.clone();
        let next_metadata = self.next_metadata.clone();
        let is_compacting = self.is_compacting.clone();
        self.is_compacting.store(true, Ordering::Release);
        self.compaction_thread_join_handle = Some(thread::spawn(move || {
            let compaction_result = SizeTieredStrategy::compact(
                path,
                &is_compacting,
                metadata_snapshot,
                &next_metadata,
                range,
            );

            match compaction_result {
                Ok(_) => println!("Compaction terminated successfully."),
                Err(error) => {
                    is_compacting.store(false, Ordering::Release);
                    println!("Compaction terminated with error: {:?}", error);
                },
            }
        }));
    }

    fn try_replace_metadata(
        &self,
        curr_metadata: &mut MutexGuard<SizeTieredMetadata<T, U>>,
    ) -> Result<bool> {
        let mut next_metadata = self.next_metadata.lock().unwrap();

        if let Some(next_metadata) = next_metadata.take() {
            let logical_time_opt = next_metadata
                .sstables
                .iter()
                .map(|sstable| sstable.summary.logical_time_range.1)
                .max();
            let old_sstables = mem::replace(&mut curr_metadata.sstables, next_metadata.sstables);
            curr_metadata.sstables.extend(
                old_sstables
                    .iter()
                    .filter(|sstable| Some(sstable.summary.logical_time_range.1) > logical_time_opt)
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
    T: 'static + Clone + DeserializeOwned + Hash + Ord + Send + Serialize + Sync,
    U: 'static + Clone + DeserializeOwned + Send + Serialize + Sync,
{
    fn get_path(&self) -> &Path {
        self.path.as_path()
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
        {
            let mut curr_metadata = self.curr_metadata.lock().unwrap();
            curr_metadata.push_sstable(Arc::new(sstable));
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
        }

        if self.is_compacting.load(Ordering::Acquire) || self.metadata_lock_count.get() != 0 {
            return Ok(());
        }

        // taking snapshot of current metadata
        let mut metadata_snapshot = {
            let mut curr_metadata = self.curr_metadata.lock().unwrap();
            if self.try_replace_metadata(&mut curr_metadata)? {
                self.metadata_file.seek(SeekFrom::Start(0))?;
                self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
            }
            curr_metadata.clone()
        };

        if let Some(range) = metadata_snapshot.get_compaction_range() {
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

    fn get<V>(&mut self, key: &V) -> Result<Option<SSTableValue<U>>>
    where
        T: Borrow<V>,
        V: Ord + Hash + ?Sized,
    {
        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        if self.try_replace_metadata(&mut curr_metadata)? {
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
        }

        let mut ret = None;
        for sstable in &curr_metadata.sstables {
            let res = sstable.get(&key)?;
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

        let len_hint = curr_metadata
            .sstables
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

        for dir_entry in fs::read_dir(self.path.as_path())? {
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
        // should never need to replace metadata as the compaction thread should not be running
        // when yielding calling iter.
        if self.try_replace_metadata(&mut curr_metadata)? {
            self.metadata_file.seek(SeekFrom::Start(0))?;
            self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
        }

        let sstable_data_iters = curr_metadata
            .sstables
            .iter()
            .map(|sstable| sstable.data_iter())
            .collect();
        let metadata_lock_count = Rc::clone(&self.metadata_lock_count);
        let compaction_iter = SizeTieredIter::new(Some(metadata_lock_count), sstable_data_iters)?
            .filter_map(|entry_result| {
                match entry_result {
                    Ok(entry) => {
                        let (key, value) = entry;
                        value.data.map(|value| Ok((key, value)))
                    },
                    Err(error) => Some(Err(error)),
                }
            });

        Ok(Box::new(compaction_iter))
    }
}

type SizeTieredIterEntry<T, U> = cmp::Reverse<(T, SSTableValue<U>, usize)>;

struct SizeTieredIter<T, U> {
    metadata_lock_count: Option<Rc<Cell<u64>>>,
    sstable_data_iters: Vec<SSTableDataIter<T, U>>,
    entries: BinaryHeap<SizeTieredIterEntry<T, U>>,
    last_key_opt: Option<T>,
}

impl<T, U> SizeTieredIter<T, U>
where
    T: Hash + DeserializeOwned + Ord + Serialize,
    U: DeserializeOwned + Serialize,
{
    pub fn new(
        metadata_lock_count: Option<Rc<Cell<u64>>>,
        mut sstable_data_iters: Vec<SSTableDataIter<T, U>>,
    ) -> Result<Self> {
        if let Some(ref metadata_lock_count) = metadata_lock_count {
            metadata_lock_count.set(metadata_lock_count.get() + 1);
        }

        let mut entries = BinaryHeap::new();

        for (index, sstable_data_iter) in sstable_data_iters.iter_mut().enumerate() {
            if let Some(entry) = sstable_data_iter.next() {
                let Entry { key, value } = entry?;
                entries.push(cmp::Reverse((key, value, index)));
            }
        }

        Ok(SizeTieredIter {
            metadata_lock_count,
            sstable_data_iters,
            entries,
            last_key_opt: None,
        })
    }
}

impl<T, U> Iterator for SizeTieredIter<T, U>
where
    T: Clone + DeserializeOwned + Ord,
    U: DeserializeOwned,
{
    type Item = Result<(T, SSTableValue<U>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(cmp::Reverse((key, value, index))) = self.entries.pop() {
            if let Some(entry) = self.sstable_data_iters[index].next() {
                match entry {
                    Ok(entry) => self.entries.push(cmp::Reverse((entry.key, entry.value, index))),
                    Err(error) => return Some(Err(error)),
                }
            }

            let should_return = match self.last_key_opt {
                Some(ref last_key) => *last_key != key,
                None => true,
            };

            self.last_key_opt = Some(key.clone());

            if should_return {
                return Some(Ok((key, value)));
            }
        }
        None
    }
}

impl<T, U> Drop for SizeTieredIter<T, U> {
    fn drop(&mut self) {
        if let Some(ref mut metadata_lock_count) = self.metadata_lock_count {
            metadata_lock_count.set(metadata_lock_count.get() - 1);
        }
    }
}
