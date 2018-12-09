use crate::entry::Entry;
use crate::lsm_tree::compaction::{CompactionIter, CompactionStrategy};
use crate::lsm_tree::{sstable, Result, SSTable, SSTableBuilder, SSTableDataIter, SSTableValue};
use bincode::{deserialize, serialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::cell::Cell;
use std::cmp;
use std::collections::{BTreeMap, BinaryHeap, HashSet, VecDeque};
use std::fmt::{self, Debug};
use std::fs;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::ops::Bound::{Included, Unbounded};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "T: DeserializeOwned, U: DeserializeOwned"))]
struct LeveledMetadata<T, U>
where
    T: Ord,
{
    max_in_memory_size: u64,
    max_sstable_count: usize,
    max_sstable_size: u64,
    max_initial_level_count: usize,
    growth_factor: u64,
    sstables: Vec<Arc<SSTable<T, U>>>,
    levels: Vec<BTreeMap<T, Arc<SSTable<T, U>>>>,
}

impl<T, U> LeveledMetadata<T, U>
where
    T: Ord,
{
    pub fn new(
        max_in_memory_size: u64,
        max_sstable_count: usize,
        max_sstable_size: u64,
        max_initial_level_count: usize,
        growth_factor: u64,
    ) -> Self {
        LeveledMetadata {
            max_in_memory_size,
            max_sstable_count,
            max_sstable_size,
            max_initial_level_count,
            growth_factor,
            sstables: Vec::new(),
            levels: Vec::new(),
        }
    }

    pub fn push_sstable(&mut self, sstable: Arc<SSTable<T, U>>) {
        self.sstables.push(sstable);
    }

    pub fn insert_sstable(&mut self, index: usize, sstable: Arc<SSTable<T, U>>)
    where
        T: Clone,
    {
        while index >= self.levels.len() {
            self.levels.push(BTreeMap::new());
        }

        self.levels[index].insert(sstable.summary.key_range.1.clone(), sstable);
    }
}

impl<T, U> Debug for LeveledMetadata<T, U>
where
    T: Debug + Ord,
    U: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\nsstables:\n{:#?}", self.sstables)?;
        for (index, level) in self.levels.iter().enumerate() {
            writeln!(f, "level {}:", index)?;
            for sstable in level.values() {
                writeln!(f, "{:?}", sstable)?;
            }
        }
        Ok(())
    }
}

/// A compaction strategy based on grouping SSTables into levels of exponential increasing sizes.
/// Each SSTable is a a fixed size and two SSTables are guaranteed to be non-overlapping if they
/// are in the same level. As smaller levels fill up, SSTables are merged into larger levels.
///
/// # Configuration Parameters
///
///  - `max_in_memory_size`: The maximum size of the in-memory tree before it must be flushed onto
///  disk as a SSTable.
///  - `max_sstable_count`: The maximum number of overlapping SSTables before they must be merged
///  into the first level.
///  - `max_sstable_size`: The maximum size of a SSTable in a level.
///  - `max_initial_level_count`: The number of SSTables in the first level.
///  - `growth_factor`: Each successive level will be `growth_factor` times larger than the
///  previous level.
pub struct LeveledStrategy<T, U>
where
    T: Ord,
{
    path: PathBuf,
    compaction_thread_join_handle: Option<thread::JoinHandle<()>>,
    is_compacting: Arc<AtomicBool>,
    curr_logical_time: u64,
    logical_time_file: fs::File,
    metadata_lock_count: Rc<Cell<u64>>,
    metadata_file: fs::File,
    curr_metadata: Arc<Mutex<LeveledMetadata<T, U>>>,
    next_metadata: Arc<Mutex<Option<LeveledMetadata<T, U>>>>,
}

impl<T, U> LeveledStrategy<T, U>
where
    T: Ord,
{
    /// Constructs a new `LeveledStrategy<T, U>` with specific configuration parameters.
    ///
    /// # Examples
    ///
    /// ```
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::LeveledStrategy;
    ///
    /// let sts: LeveledStrategy<u32, u32> =
    ///     LeveledStrategy::new("leveled_strategy_new", 10000, 4, 50000, 10, 10)?;
    /// # fs::remove_dir_all("leveled_strategy_new")?;
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn new<P>(
        path: P,
        max_in_memory_size: u64,
        max_sstable_count: usize,
        max_sstable_size: u64,
        max_initial_level_count: usize,
        growth_factor: u64,
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
        let mut ret = LeveledStrategy {
            path: PathBuf::from(path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            curr_logical_time: 0,
            logical_time_file,
            metadata_lock_count: Rc::new(Cell::new(0)),
            metadata_file,
            curr_metadata: Arc::new(Mutex::new(LeveledMetadata::new(
                max_in_memory_size,
                max_sstable_count,
                max_sstable_size,
                max_initial_level_count,
                growth_factor,
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

    /// Opens an existing `LeveledStrategy<T, U>` from a folder.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use extended_collections::lsm_tree::Result;
    /// # fn foo() -> Result<()> {
    /// # use std::fs;
    /// use extended_collections::lsm_tree::compaction::LeveledStrategy;
    ///
    /// let sts: LeveledStrategy<u32, u32> = LeveledStrategy::open("leveled_strategy_open")?;
    /// # fs::remove_dir_all("leveled_strategy_open")?;
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
        Ok(LeveledStrategy {
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

    fn try_replace_metadata(
        &self,
        curr_metadata: &mut MutexGuard<'_, LeveledMetadata<T, U>>,
    ) -> Result<bool> {
        let mut next_metadata = self.next_metadata.lock().unwrap();

        if let Some(next_metadata) = next_metadata.take() {
            let logical_time_opt = next_metadata
                .levels
                .iter()
                .map(|level_entry| {
                    level_entry
                        .iter()
                        .map(|level_entry| level_entry.1.summary.logical_time_range.1)
                        .max()
                })
                .max()
                .and_then(|max_opt| max_opt);
            let old_sstables = mem::replace(&mut curr_metadata.sstables, next_metadata.sstables);
            let old_levels = mem::replace(&mut curr_metadata.levels, next_metadata.levels);
            curr_metadata.sstables.extend(
                old_sstables
                    .iter()
                    .filter(|sstable| Some(sstable.summary.logical_time_range.0) > logical_time_opt)
                    .map(|sstable| Arc::clone(sstable)),
            );

            let path_iter = curr_metadata.sstables.iter().map(|sstable| &sstable.path);
            let level_path_iter = curr_metadata
                .levels
                .iter()
                .flat_map(|level| level.iter().map(|level_entry| &level_entry.1.path));
            let new_sstable_paths: HashSet<_> = path_iter.chain(level_path_iter).collect();

            let old_path_iter = old_sstables.iter().map(|sstable| &sstable.path);
            let old_level_path_iter = old_levels
                .iter()
                .flat_map(|level| level.iter().map(|level_entry| &level_entry.1.path));

            for path in old_path_iter.chain(old_level_path_iter) {
                if !new_sstable_paths.contains(path) {
                    fs::remove_dir_all(path)?;
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn compact<P>(
        path: P,
        is_compacting: &Arc<AtomicBool>,
        mut metadata_snapshot: LeveledMetadata<T, U>,
        next_metadata: &Arc<Mutex<Option<LeveledMetadata<T, U>>>>,
    ) -> Result<()>
    where
        T: Clone + DeserializeOwned + Hash + Serialize,
        U: DeserializeOwned + Serialize,
        P: AsRef<Path>,
    {
        println!("Started compacting.");

        if metadata_snapshot.levels.is_empty() {
            metadata_snapshot.levels.push(BTreeMap::new());
        }

        // compacting L0
        let mut entry_count_hint = 0;
        let sstable_data_iters: Vec<_> = metadata_snapshot
            .sstables
            .drain(..)
            .map(|sstable| {
                entry_count_hint += sstable.summary.entry_count;
                sstable.data_iter()
            })
            .collect();
        for sstable in metadata_snapshot.levels[0].values() {
            entry_count_hint = cmp::max(entry_count_hint, sstable.summary.entry_count);
        }
        let level_data_iter = mem::replace(&mut metadata_snapshot.levels[0], BTreeMap::new())
            .into_iter()
            .map(|entry| entry.1.data_iter())
            .collect();

        let mut sstable_builder = SSTableBuilder::new(path.as_ref(), entry_count_hint)?;

        let compaction_iter = LeveledIter::new(None, sstable_data_iters, vec![level_data_iter])?;

        for entry in compaction_iter {
            let (key, value) = entry?;

            if metadata_snapshot.levels.len() > 1 || value.data.is_some() {
                sstable_builder.append(key, value)?;
            }

            if sstable_builder.size > metadata_snapshot.max_sstable_size {
                let new_sstable = Arc::new(SSTable::new(sstable_builder.flush()?)?);
                metadata_snapshot.insert_sstable(0, new_sstable);
                sstable_builder = SSTableBuilder::new(path.as_ref(), entry_count_hint)?;
            }
        }

        if sstable_builder.key_range.is_some() {
            let new_sstable = Arc::new(SSTable::new(sstable_builder.flush()?)?);
            metadata_snapshot.insert_sstable(0, new_sstable);
        }

        // compacting L1 and onwards
        for index in 0.. {
            if index == metadata_snapshot.levels.len() {
                break;
            }

            let should_merge = |metadata_snapshot: &LeveledMetadata<T, U>, index: usize| {
                let curr_len = metadata_snapshot.levels[index].len();
                let exponent = metadata_snapshot.growth_factor.pow(index as u32) as usize;
                let max_len = metadata_snapshot.max_initial_level_count * exponent;
                curr_len > max_len as usize
            };

            while should_merge(&metadata_snapshot, index) {
                let sstable = {
                    let sstable_key = metadata_snapshot.levels[index]
                        .iter()
                        .max_by(|x, y| {
                            (x.1.summary.tombstone_count * y.1.summary.entry_count)
                                .cmp(&(y.1.summary.tombstone_count * x.1.summary.entry_count))
                        })
                        .map(|level_entry| level_entry.1.summary.key_range.1.clone())
                        .expect("Expected non-empty level to remove from.");
                    metadata_snapshot.levels[index]
                        .remove(&sstable_key)
                        .expect("Expected SSTable to remove to exist.")
                };

                let mut sstable_builder = SSTableBuilder::new(path.as_ref(), entry_count_hint)?;

                if index + 1 == metadata_snapshot.levels.len() {
                    metadata_snapshot.insert_sstable(index + 1, sstable);
                    continue;
                }

                let sstable_data_iter = sstable.data_iter();
                let level = mem::replace(&mut metadata_snapshot.levels[index + 1], BTreeMap::new());
                let (old_level, new_level): (BTreeMap<_, _>, BTreeMap<_, _>) =
                    level.into_iter().partition(|level_entry| {
                        sstable::is_intersecting(
                            &sstable.summary.key_range,
                            &level_entry.1.summary.key_range,
                        )
                    });

                metadata_snapshot.levels[index + 1] = new_level;

                let compaction_iter = LeveledIter::new(
                    None,
                    vec![sstable_data_iter],
                    vec![old_level
                        .into_iter()
                        .map(|level_entry| level_entry.1.data_iter())
                        .collect()],
                )?;

                for entry in compaction_iter {
                    let (key, value) = entry?;

                    if index + 1 != metadata_snapshot.levels.len() - 1 || value.data.is_some() {
                        sstable_builder.append(key, value)?;
                    }

                    if sstable_builder.size > metadata_snapshot.max_sstable_size {
                        let new_sstable = Arc::new(SSTable::new(sstable_builder.flush()?)?);
                        metadata_snapshot.insert_sstable(index + 1, new_sstable);
                        sstable_builder = SSTableBuilder::new(path.as_ref(), entry_count_hint)?;
                    }
                }

                if sstable_builder.key_range.is_some() {
                    let new_sstable = Arc::new(SSTable::new(sstable_builder.flush()?)?);
                    metadata_snapshot.insert_sstable(index + 1, new_sstable);
                }
            }
        }

        *next_metadata.lock().unwrap() = Some(metadata_snapshot);

        is_compacting.store(false, Ordering::Release);

        println!("Finished compacting");
        Ok(())
    }

    fn spawn_compaction_thread(&mut self, metadata_snapshot: LeveledMetadata<T, U>)
    where
        T: 'static + Clone + DeserializeOwned + Hash + Send + Serialize + Sync,
        U: 'static + DeserializeOwned + Serialize + Send + Sync,
    {
        let path = self.path.clone();
        let next_metadata = self.next_metadata.clone();
        let is_compacting = self.is_compacting.clone();
        self.is_compacting.store(true, Ordering::Release);
        self.compaction_thread_join_handle = Some(thread::spawn(move || {
            let compaction_result =
                LeveledStrategy::compact(path, &is_compacting, metadata_snapshot, &next_metadata);

            match compaction_result {
                Ok(_) => println!("Compaction terminated successfully."),
                Err(error) => {
                    is_compacting.store(false, Ordering::Release);
                    println!("Compaction terminated with error: {:?}", error);
                },
            }
        }))
    }
}

impl<T, U> CompactionStrategy<T, U> for LeveledStrategy<T, U>
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
        self.logical_time_file
            .write_u64::<BigEndian>(self.curr_logical_time)?;
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
        let metadata_snapshot = {
            let mut curr_metadata = self.curr_metadata.lock().unwrap();
            if self.try_replace_metadata(&mut curr_metadata)? {
                self.metadata_file.seek(SeekFrom::Start(0))?;
                self.metadata_file.write_all(&serialize(&*curr_metadata)?)?;
            }
            curr_metadata.clone()
        };

        if metadata_snapshot.sstables.len() > metadata_snapshot.max_sstable_count {
            self.spawn_compaction_thread(metadata_snapshot);
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

        if ret.is_some() {
            return Ok(ret);
        }

        for level in &curr_metadata.levels {
            let sstable_opt = level
                .range((Included(key), Unbounded))
                .next()
                .map(|entry| entry.1);
            if let Some(sstable) = sstable_opt {
                if let Some(value) = sstable.get(key)? {
                    return Ok(Some(value));
                }
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

        let sstables_len_hint: usize = curr_metadata
            .sstables
            .iter()
            .map(|sstable| sstable.summary.entry_count - sstable.summary.tombstone_count)
            .sum();

        let levels_len_hint: usize = curr_metadata
            .levels
            .iter()
            .map(|level| -> usize {
                level
                    .iter()
                    .map(|entry| entry.1.summary.entry_count - entry.1.summary.tombstone_count)
                    .sum()
            })
            .sum();

        Ok(sstables_len_hint + levels_len_hint)
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
        curr_metadata.levels.clear();
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
        let level_data_iters = curr_metadata
            .levels
            .iter()
            .map(|level| {
                level
                    .iter()
                    .map(|level_entry| level_entry.1.data_iter())
                    .collect()
            })
            .collect();
        let metadata_lock_count = Rc::clone(&self.metadata_lock_count);
        let compaction_iter = LeveledIter::new(
            Some(metadata_lock_count),
            sstable_data_iters,
            level_data_iters,
        )?
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

#[derive(Eq, Ord, PartialEq, PartialOrd)]
enum LeveledIterEntryIndex {
    SSTableIndex(usize),
    LevelIndex(usize),
}

type LeveledIterEntry<T, U> = cmp::Reverse<(T, SSTableValue<U>, LeveledIterEntryIndex)>;

struct LeveledIter<T, U> {
    metadata_lock_count: Option<Rc<Cell<u64>>>,
    sstable_data_iters: Vec<SSTableDataIter<T, U>>,
    level_data_iters: Vec<VecDeque<SSTableDataIter<T, U>>>,
    entries: BinaryHeap<LeveledIterEntry<T, U>>,
    last_key_opt: Option<T>,
}

impl<T, U> LeveledIter<T, U>
where
    T: Hash + DeserializeOwned + Ord + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn get_next_level_entry(
        level_data_iter: &mut VecDeque<SSTableDataIter<T, U>>,
    ) -> Option<<SSTableDataIter<T, U> as Iterator>::Item> {
        loop {
            let entry_opt = match level_data_iter.front_mut() {
                Some(data_iter) => data_iter.next(),
                None => return None,
            };

            match entry_opt {
                None => level_data_iter.pop_front(),
                _ => return entry_opt,
            };
        }
    }

    pub fn new(
        metadata_lock_count: Option<Rc<Cell<u64>>>,
        mut sstable_data_iters: Vec<SSTableDataIter<T, U>>,
        mut level_data_iters: Vec<VecDeque<SSTableDataIter<T, U>>>,
    ) -> Result<Self> {
        if let Some(ref metadata_lock_count) = metadata_lock_count {
            metadata_lock_count.set(metadata_lock_count.get() + 1);
        }

        let mut entries = BinaryHeap::new();

        for (index, sstable_data_iter) in sstable_data_iters.iter_mut().enumerate() {
            if let Some(entry) = sstable_data_iter.next() {
                let Entry { key, value } = entry?;
                entries.push(cmp::Reverse((
                    key,
                    value,
                    LeveledIterEntryIndex::SSTableIndex(index),
                )));
            }
        }

        for (index, level_data_iter) in level_data_iters.iter_mut().enumerate() {
            if let Some(entry) = Self::get_next_level_entry(level_data_iter) {
                let Entry { key, value } = entry?;
                entries.push(cmp::Reverse((
                    key,
                    value,
                    LeveledIterEntryIndex::LevelIndex(index),
                )));
            }
        }

        Ok(LeveledIter {
            metadata_lock_count,
            sstable_data_iters,
            level_data_iters,
            entries,
            last_key_opt: None,
        })
    }
}

impl<T, U> Iterator for LeveledIter<T, U>
where
    T: Clone + Hash + DeserializeOwned + Ord + Serialize,
    U: DeserializeOwned + Serialize,
{
    type Item = Result<(T, SSTableValue<U>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(cmp::Reverse((key, value, index))) = self.entries.pop() {
            let entry_opt = match index {
                LeveledIterEntryIndex::LevelIndex(index) => {
                    Self::get_next_level_entry(&mut self.level_data_iters[index])
                },
                LeveledIterEntryIndex::SSTableIndex(index) => self.sstable_data_iters[index].next(),
            };

            if let Some(entry) = entry_opt {
                match entry {
                    Ok(entry) => {
                        self.entries
                            .push(cmp::Reverse((entry.key, entry.value, index)))
                    },
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

impl<T, U> Drop for LeveledIter<T, U> {
    fn drop(&mut self) {
        if let Some(ref metadata_lock_count) = self.metadata_lock_count {
            metadata_lock_count.set(metadata_lock_count.get() - 1);
        }
    }
}
