use bincode::{deserialize, serialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use lsm_tree::compaction::{CompactionIter, CompactionStrategy};
use lsm_tree::{sstable, Result, SSTable, SSTableBuilder, SSTableDataIter, SSTableValue};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::{FromIterator, self};
use std::mem;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "T: DeserializeOwned, U: DeserializeOwned"))]
struct LeveledMetadata<T, U>
where
    T: Ord,
{
    max_sstable_size: u64,
    max_in_memory_size: u64,
    growth_factor: u64,
    sstables: Vec<Arc<SSTable<T, U>>>,
    levels: Vec<BTreeMap<T, Arc<SSTable<T, U>>>>,
}

impl<T, U> LeveledMetadata<T, U>
where
    T: Hash + DeserializeOwned + Ord + Serialize,
    U: DeserializeOwned + Serialize,
{
    pub fn new(
        max_sstable_size: u64,
        max_in_memory_size: u64,
        growth_factor: u64,
    ) -> Self {
        LeveledMetadata {
            max_sstable_size,
            max_in_memory_size,
            growth_factor,
            sstables: Vec::new(),
            levels: Vec::new(),
        }
    }
}

pub struct LeveledStrategy<T, U>
where
    T: DeserializeOwned + Ord,
    U: DeserializeOwned,
{
    db_path: PathBuf,
    compaction_thread_join_handle: Option<thread::JoinHandle<()>>,
    is_compacting: Arc<AtomicBool>,
    curr_logical_time: u64,
    logical_time_file: fs::File,
    metadata_lock_count: Rc<RefCell<u64>>,
    metadata_file: fs::File,
    curr_metadata: Arc<Mutex<LeveledMetadata<T, U>>>,
    next_metadata: Arc<Mutex<Option<LeveledMetadata<T, U>>>>,
}

impl<T, U> LeveledStrategy<T, U>
where
    T: 'static + Clone + Hash + DeserializeOwned + Ord + Send + Serialize + Sync,
    U: 'static + Clone + DeserializeOwned + Send + Serialize + Sync,
{
    pub fn new<P>(
        db_path: P,
        max_sstable_size: u64,
        max_in_memory_size: u64,
        growth_factor: u64,
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
        let mut ret = LeveledStrategy {
            db_path: PathBuf::from(db_path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            curr_logical_time: 0,
            logical_time_file,
            metadata_lock_count: Rc::new(RefCell::new(0)),
            metadata_file,
            curr_metadata: Arc::new(Mutex::new(LeveledMetadata::new(
                max_sstable_size,
                max_in_memory_size,
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
        Ok(LeveledStrategy {
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

    fn try_replace_metadata(
        &self,
        curr_metadata: &mut MutexGuard<LeveledMetadata<T, U>>,
    ) -> Result<bool> {
        let mut next_metadata = self.next_metadata.lock().unwrap();

        if let Some(next_metadata) = next_metadata.take() {
            let logical_time_opt = next_metadata.sstables
                .iter()
                .map(|sstable| sstable.summary.logical_time)
                .max();
            let old_sstables = mem::replace(&mut curr_metadata.sstables, next_metadata.sstables);
            let old_levels = mem::replace(&mut curr_metadata.levels, next_metadata.levels);
            curr_metadata.sstables.extend(
                old_sstables
                    .iter()
                    .filter(|sstable| Some(sstable.summary.logical_time) > logical_time_opt)
                    .map(|sstable| Arc::clone(sstable)),
            );

            // removing paths from L0
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

            // removing sstables from L1 onwards
            for (index, level) in curr_metadata.levels.iter().enumerate() {
                let new_sstable_paths: HashSet<&PathBuf> = HashSet::from_iter(
                    level
                        .iter()
                        .map(|pair| &pair.1.path),
                );

                if let Some(old_sstable_map) = old_levels.get(index) {
                    for old_sstable in old_sstable_map.iter().map(|entry| entry.1) {
                        if !new_sstable_paths.contains(&old_sstable.path) {
                            fs::remove_dir_all(old_sstable.path.as_path())?;
                        }
                    }
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<T, U> CompactionStrategy<T, U> for LeveledStrategy<T, U>
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

        // TODO(compaction_thread))
        unimplemented!();

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
        for sstable in &curr_metadata.sstables {
            let res = sstable.get(key)?;
            if res.is_some() && (ret.is_none() || res < ret) {
                ret = res;
            }
        }

        if ret.is_some() {
            return Ok(ret);
        }

        for level in &curr_metadata.levels {
            let sstable_opt = level
                .range(key..)
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

        let sstables_len_hint: usize = curr_metadata.sstables
            .iter()
            .map(|sstable| sstable.summary.entry_count - sstable.summary.tombstone_count)
            .sum();

        let levels_len_hint: usize = curr_metadata.levels
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
        unimplemented!();
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
        unimplemented!();
    }

    fn max(&mut self) -> Result<Option<T>> {
        unimplemented!();
    }

    fn iter(&mut self) -> Result<Box<CompactionIter<T, U>>> {
        unimplemented!();
    }
}
