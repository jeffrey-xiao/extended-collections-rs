use bincode::{deserialize, serialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use entry::Entry;
use lsm_tree::compaction::{CompactionIter, CompactionStrategy};
use lsm_tree::{sstable, Result, SSTable, SSTableBuilder, SSTableDataIter, SSTableValue};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::cell::RefCell;
use std::cmp;
use std::collections::{BinaryHeap, BTreeMap, HashSet};
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
    T: Hash + DeserializeOwned + Ord + Serialize,
    U: DeserializeOwned + Serialize,
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
        max_in_memory_size: u64,
        max_sstable_count: usize,
        max_sstable_size: u64,
        max_initial_level_count: usize,
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
                .map(|sstable| sstable.summary.logical_time_range.1)
                .max();
            let old_sstables = mem::replace(&mut curr_metadata.sstables, next_metadata.sstables);
            let old_levels = mem::replace(&mut curr_metadata.levels, next_metadata.levels);
            curr_metadata.sstables.extend(
                old_sstables
                    .iter()
                    .filter(|sstable| Some(sstable.summary.logical_time_range.0) > logical_time_opt)
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

    fn spawn_compaction_thread(
        &mut self,
        mut metadata: LeveledMetadata<T, U>,
    ) {
        let db_path = self.db_path.clone();
        let next_metadata = self.next_metadata.clone();
        let is_compacting = self.is_compacting.clone();
        self.is_compacting.store(true, Ordering::Release);
        self.compaction_thread_join_handle = Some(thread::spawn(move || {
            let compaction_result = (|| -> Result<()> {
                println!("Started compacting.");

                if metadata.levels.is_empty() {
                    metadata.levels.push(BTreeMap::new());
                }

                // compacting L0
                let mut entry_hint = 0;
                let mut sstable_data_iters: Vec<_> = metadata.sstables
                    .drain(..)
                    .flat_map(|sstable| {
                        entry_hint += sstable.summary.entry_count;
                        sstable.data_iter()
                    })
                    .collect();
                entry_hint += metadata.levels[0]
                    .iter()
                    .map(|entry| entry.1.summary.entry_count)
                    .sum::<usize>();
                let mut level_sstable_iter = mem::replace(&mut metadata.levels[0], BTreeMap::new())
                    .into_iter()
                    .map(|entry| entry.1.data_iter());

                if let Some(level_sstable_data_iter) = level_sstable_iter.next() {
                    sstable_data_iters.push(level_sstable_data_iter?);
                }

                let mut sstable_builder: SSTableBuilder<T, U> = SSTableBuilder::new(
                    db_path.as_path(),
                    entry_hint,
                )?;

                let mut entries = BinaryHeap::new();
                let mut last_key_opt = None;

                for (index, sstable_data_iter) in sstable_data_iters.iter_mut().enumerate() {
                    if let Some(entry) = sstable_data_iter.next() {
                        let entry = entry?;
                        entries.push(cmp::Reverse((entry.key, entry.value, index)));
                    }
                }

                while let Some(cmp::Reverse((key, value, index))) = entries.pop() {
                    if let Some(entry) = sstable_data_iters[index].next() {
                        let Entry { key, value } = entry?;
                        entries.push(cmp::Reverse((key, value, index)));
                    } else if index == sstable_data_iters.len() - 1 {
                        if let Some(data_iter) = level_sstable_iter.next() {
                            sstable_data_iters[index] = data_iter?;
                            if let Some(entry) = sstable_data_iters[index].next() {
                                let Entry { key, value } = entry?;
                                entries.push(cmp::Reverse((key, value, index)));
                            }
                        }
                    }

                    let should_append = match last_key_opt {
                        Some(ref last_key) => *last_key != key,
                        None => true,
                    } && (metadata.levels.len() > 1 || value.data.is_some());

                    if should_append {
                        sstable_builder.append(key.clone(), value)?;
                    }

                    if sstable_builder.size > metadata.max_sstable_size {
                        let curr_sstable: SSTable<T, U> = SSTable::new(sstable_builder.flush()?)?;
                        metadata.levels[0].insert(
                            curr_sstable.summary.key_range.1.clone(),
                            Arc::new(curr_sstable),
                        );
                        sstable_builder = SSTableBuilder::new(
                            db_path.as_path(),
                            entry_hint,
                        )?;
                    }

                    last_key_opt = Some(key);
                }

                if sstable_builder.key_range.is_some() {
                    metadata.sstables.push(Arc::new(SSTable::new(sstable_builder.flush()?)?));
                }

                // compacting L1 and onwards
                for index in 0.. {
                    if index == metadata.levels.len() {
                        break;
                    }

                    let max_len = metadata.max_initial_level_count * metadata.growth_factor.pow(index as u32) as usize;
                    if metadata.levels[index].len() <= max_len {
                        continue;
                    }
                    let old_sstable = {
                        let old_sstable = metadata.levels[index]
                            .iter()
                            .max_by(|x, y| {
                                (x.1.summary.tombstone_count * y.1.summary.entry_count)
                                    .cmp(&(y.1.summary.tombstone_count * x.1.summary.entry_count))
                            })
                            .map(|level_entry| level_entry.1.summary.key_range.1.clone())
                            .expect("Unreachable code");
                        metadata.levels[index]
                            .remove(&old_sstable)
                            .expect("Unreachable code")
                    };
                    metadata.levels[index].remove(&old_sstable.summary.key_range.1);
                    if index + 1 == metadata.levels.len() {
                        metadata.levels.push(BTreeMap::new());
                        metadata.levels[index + 1].insert(
                            old_sstable.summary.key_range.1.clone(),
                            old_sstable,
                        );
                        continue;
                    }

                    let mut sstable_builder: SSTableBuilder<T, U> = SSTableBuilder::new(
                        db_path.as_path(),
                        entry_hint,
                    )?;
                    let mut sstable_data_iter = old_sstable.data_iter()?.flat_map(|x| x);
                    let mut level_sstable_data_iters = metadata.levels[index]
                        .iter()
                        .map(|level_entry| level_entry.1.data_iter());
                    let mut level_sstable_data_iter = level_sstable_data_iters
                        .next()
                        .expect("Unreachable code")?
                        .flat_map(|x| x);
                    let mut sstable_entry = sstable_data_iter.next();
                    let mut level_sstable_entry = level_sstable_data_iter.next();

                    loop {
                        let ordering = match (&sstable_entry, &level_sstable_entry) {
                            (&Some(ref sstable_entry), &Some(ref level_sstable_entry)) => sstable_entry.cmp(&level_sstable_entry),
                            (&Some(ref sstable_entry), &None) => cmp::Ordering::Less,
                            (&None, &Some(ref level_sstable_entry)) => cmp::Ordering::Greater,
                            (&None, &None) => break,
                        };

                        match ordering {
                            cmp::Ordering::Less | cmp::Ordering::Equal => {
                                let entry = mem::replace(
                                    &mut sstable_entry,
                                    sstable_data_iter.next(),
                                ).expect("Unreachable code");
                                sstable_builder.append(entry.key, entry.value)?;
                            },
                            cmp::Ordering::Greater => {
                                // let new_entry = {
                                //     match level_sstable_data_iter.next() {
                                //         Some(entry) => entry,
                                //         None => {
                                //             if let Some(data_iter) = level_sstable_data_iters.next() {
                                //                 level_sstable_data_iter = data_iter;
                                //                 level_sstable_data_iter.next().expect("")
                                //             } else {

                                //             }
                                //         }
                                //     }
                                // };
                            },
                        }
                    }
                }


                println!("Locking in compaction");
                *next_metadata.lock().unwrap() = Some(metadata);

                is_compacting.store(false, Ordering::Release);
                println!("Finished compacting");
                Ok(())
            })();

            if compaction_result.is_err() {
                println!("Compaction thread errored: {:?}", compaction_result);
                is_compacting.store(false, Ordering::Release);
            }
        }))
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
