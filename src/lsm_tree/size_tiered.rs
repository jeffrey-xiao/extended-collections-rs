use bincode::{deserialize, serialize};
use lsm_tree::{CompactionStrategy, SSTable, SSTableBuilder, SSTableDataIter, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{BinaryHeap, Bound, HashSet};
use std::cmp;
use std::fs;
use std::hash::Hash;
use std::io::{Read, Write};
use std::iter::FromIterator;
use std::marker::Send;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, self};
use std::thread;

#[derive(Clone, Debug, Serialize, Deserialize)]
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

pub struct SizeTieredStrategy<T, U> {
    db_path: PathBuf,
    compaction_thread_join_handle: Option<thread::JoinHandle<()>>,
    is_compacting: Arc<AtomicBool>,
    curr_metadata: Arc<Mutex<SizeTieredMetadata<T, U>>>,
    next_metadata: Arc<Mutex<Option<SizeTieredMetadata<T, U>>>>,
}

impl<T, U> SizeTieredStrategy<T, U>
where
    T: ::std::fmt::Debug + 'static + Clone + Hash + DeserializeOwned + Ord + Send + Serialize + Sync,
    U: ::std::fmt::Debug + 'static + Clone + DeserializeOwned + Send + Serialize + Sync,
{
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
        Ok(SizeTieredStrategy {
            db_path: PathBuf::from(db_path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
            curr_metadata: Arc::new(Mutex::new(SizeTieredMetadata::new(
                min_sstable_count,
                min_sstable_size,
                bucket_low,
                bucket_high,
                max_in_memory_size,
            ))),
            next_metadata: Arc::new(Mutex::new(None)),
        })
    }

    pub fn open<P>(db_path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut metadata_file = fs::File::open(db_path.as_ref().join("metadata.dat"))?;
        let mut buffer = Vec::new();
        metadata_file.read_to_end(&mut buffer)?;
        Ok(SizeTieredStrategy {
            db_path: PathBuf::from(db_path.as_ref()),
            compaction_thread_join_handle: None,
            is_compacting: Arc::new(AtomicBool::new(false)),
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
            // println!("next sstables {:?}", metadata.sstables);
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
                old_sstables.iter().map(|sstable| sstable.summary.item_count).sum(),
            ).unwrap();

            // println!("old sstables {:#?}", old_sstables);
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
                    last_entry_opt = Some(entry.key);
                }

                if let Some(entry) = old_sstable_data_iters[index].next() {
                    entries.push((cmp::Reverse(entry.unwrap()), index));
                }
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

            is_compacting.store(false, atomic::Ordering::Release);
            println!("Finished compacting.");
            // println!("New sstable: {:?}", new_sstable_builder.path);
            // println!("New metadata: {:#?}", *next_metadata.lock().unwrap());
        }));
        self.is_compacting.store(true, atomic::Ordering::Release);
    }

    fn try_replace_metadata(&self) -> Result<()> {
        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        let mut next_metadata = self.next_metadata.lock().unwrap();

        if let Some(next_metadata) = next_metadata.take() {
            let old_sstables = mem::replace(&mut curr_metadata.sstables, next_metadata.sstables);
            self.flush_metadata(&*curr_metadata)?;
            let new_sstable_paths: HashSet<&PathBuf> = HashSet::from_iter(
                curr_metadata.sstables
                    .iter()
                    .map(|sstable| &sstable.path),
            );

            for old_sstable in old_sstables {
                if !new_sstable_paths.contains(&old_sstable.path) {
                    // println!("Removing {:?}", old_sstable.path);
                    fs::remove_dir_all(old_sstable.path.as_path())?;
                }
            }
        }

        Ok(())
    }

    fn flush_metadata(&self, metadata: &SizeTieredMetadata<T, U>) -> Result<()> {
        let mut metadata_file = fs::File::create(self.db_path.join("metadata.dat"))?;

        let serialized_metadata = serialize(metadata)?;
        metadata_file.write_all(&serialized_metadata)?;
        Ok(())
    }
}

impl<T, U> CompactionStrategy<T, U> for SizeTieredStrategy<T, U>
where
    T: ::std::fmt::Debug + 'static + Clone + Hash + DeserializeOwned + Ord + Send + Serialize + Sync,
    U: ::std::fmt::Debug + 'static + Clone + DeserializeOwned + Send + Serialize + Sync,
{
    fn get_db_path(&self) -> &Path {
        self.db_path.as_path()
    }

    fn get_max_in_memory_size(&self) -> u64 {
        self.curr_metadata.lock().unwrap().max_in_memory_size
    }

    fn try_compact(&mut self, mut sstable: SSTable<T, U>) -> Result<()> {
        self.try_replace_metadata()?;
        let mut metadata_snapshot = {
            let mut curr_metadata = self.curr_metadata.lock().unwrap();
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
            self.flush_metadata(&*curr_metadata)?;
            curr_metadata.clone()
        };
        if self.is_compacting.load(atomic::Ordering::Acquire) {
            return Ok(());
        }

        metadata_snapshot.sstables.sort_by_key(|sstable| sstable.summary.size);
        if let Some(range) = self.get_compaction_range(&metadata_snapshot) {
            // taking snapshot of current metadata
            self.spawn_compaction_thread(metadata_snapshot, range);
        }

        Ok(())
    }

    fn get(&self, key: &T) -> Result<Option<U>> {
        self.try_replace_metadata()?;
        let curr_metadata = self.curr_metadata.lock().unwrap();

        for sstable in curr_metadata.sstables.iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(value);
            }
        }

        Ok(None)
    }
}

impl<T, U> Drop for SizeTieredStrategy<T, U> {
    fn drop(&mut self) {
        if let Some(compaction_thread_join_handle) = self.compaction_thread_join_handle.take() {
            match compaction_thread_join_handle.join() {
                Ok(_) => println!("Child thread terminated successfully."),
                Err(error) => println!("Child thread terminated with error: {:?}", error),
            }
        }
    }
}
