use bincode::{deserialize, serialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use lsm_tree::compaction::{CompactionIter, CompactionStrategy};
use lsm_tree::{sstable, Result, SSTable, SSTableBuilder, SSTableDataIter, SSTableValue};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};
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
    sstables: Vec<BTreeMap<T, Arc<SSTable<T, U>>>>,
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
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn get(&mut self, key: &T) -> Result<Option<SSTableValue<U>>> {
        unimplemented!();
    }

    fn len_hint(&mut self) -> Result<usize> {
        unimplemented!();
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
