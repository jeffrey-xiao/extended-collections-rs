use bincode::{deserialize, self, serialize, serialized_size};
use lsm::{CompactionStrategy, SSTable, SSTableBuilder, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::Bound;
use std::hash::Hash;
use std::io::{BufWriter, BufReader, BufRead, Read, Write};
use std::marker::Send;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use std::thread;

#[derive(Clone)]
struct SizeTieredMetadata<T, U> {
    sstables: Vec<SSTable<T, U>>,
}

impl<T, U> SizeTieredMetadata<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    pub fn new() -> Self {
        SizeTieredMetadata {
            sstables: Vec::new(),
        }
    }

    pub fn should_compact(&self) -> bool { false }
}

#[derive(Clone)]
pub struct SizeTieredStrategy<T, U> {
    db_path: PathBuf,
    min_sstable_count: usize,
    min_sstable_size: u64,
    bucket_low: f64,
    bucket_high: f64,
    max_in_memory_size: u64,
    is_compacting: Arc<AtomicBool>,
    curr_metadata: Arc<Mutex<SizeTieredMetadata<T, U>>>,
    next_metadata: Arc<Mutex<Option<SizeTieredMetadata<T, U>>>>,
}

impl<T, U> SizeTieredStrategy<T, U>
where
    T: 'static + Clone + Hash + DeserializeOwned + Ord + Send + Serialize,
    U: 'static + Clone + DeserializeOwned + Send + Serialize,
{
    pub fn new<P>(
        db_path: P,
        min_sstable_count: usize,
        min_sstable_size: u64,
        bucket_low: f64,
        bucket_high: f64,
        max_in_memory_size: u64,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        SizeTieredStrategy {
            db_path: PathBuf::from(db_path.as_ref()),
            min_sstable_count,
            min_sstable_size,
            bucket_low,
            bucket_high,
            max_in_memory_size,
            is_compacting: Arc::new(AtomicBool::new(false)),
            curr_metadata: Arc::new(Mutex::new(SizeTieredMetadata::new())),
            next_metadata: Arc::new(Mutex::new(None)),
        }
    }

    fn get_compaction_range(&self, metadata: &SizeTieredMetadata<T, U>) -> Option<(usize, usize)> {
        let mut start = 0;
        let mut curr = 0;
        let mut range_size = 0;
        while curr < metadata.sstables.len() {
            let start_size = metadata.sstables[start].summary.size;
            let curr_size = metadata.sstables[curr].summary.size;
            let curr_avg = (range_size + curr_size) as f64 / (curr - start + 1) as f64;

            let in_min_bucket = curr_size <= self.min_sstable_size;
            let in_bucket = curr_avg * self.bucket_low <= start_size as f64
                && curr_size as f64 <= curr_avg * self.bucket_high;

            curr += 1;
            if in_min_bucket || in_bucket {
                range_size += curr_size;
            } else if curr - start > self.min_sstable_count {
                return Some((start, curr));
            } else {
                range_size = 0;
                start = curr;
            }
        }

        if curr - start > self.min_sstable_count {
            Some((start, curr))
        } else {
            None
        }
    }

    fn spawn_compaction_thread(
        &self,
        mut next_metadata: SizeTieredMetadata<T, U>,
        range: (usize, usize),
    ) {
        let strategy = (*self).clone();
        thread::spawn(move || {
            println!("Started compacting.");
            let old_sstables: Vec<SSTable<T, U>> = next_metadata.sstables
                .drain((Bound::Included(range.0), Bound::Excluded(range.1)))
                .collect();

            let new_sstable_builder: SSTableBuilder<T, U> = SSTableBuilder::new(
                strategy.db_path,
                old_sstables.iter().map(|sstable| sstable.summary.item_count).sum(),
            ).unwrap();

            let old_sstable_data_iters = old_sstables.iter().map(|sstable| sstable.data_iter());

            strategy.is_compacting.store(false, Ordering::Release);
            println!("Finished compacting.");
        });
        self.is_compacting.store(true, Ordering::Release);
    }
}

impl<T, U> CompactionStrategy<T, U> for SizeTieredStrategy<T, U>
where
    T: 'static + Clone + Hash + DeserializeOwned + Ord + Send + Serialize,
    U: 'static + Clone + DeserializeOwned + Send + Serialize,
{
    fn get_db_path(&self) -> &Path {
        self.db_path.as_path()
    }

    fn get_max_in_memory_size(&self) -> u64 {
        self.max_in_memory_size
    }

    fn try_compact(&self, mut sstable: SSTable<T, U>) -> Result<()> {
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
        curr_metadata.sstables.push(sstable);
        if self.is_compacting.load(Ordering::Acquire) {
            return Ok(());
        }

        curr_metadata.sstables.sort_by_key(|sstable| sstable.summary.size);
        if let Some(range) = self.get_compaction_range(&*curr_metadata) {
            // taking snapshot of current metadata
            self.spawn_compaction_thread((*curr_metadata).clone(), range);
        }

        Ok(())
    }

    fn get(&self, key: &T) -> Result<Option<U>> {
        let mut curr_metadata = self.curr_metadata.lock().unwrap();
        curr_metadata.sstables.sort_by_key(|sstable| sstable.summary.tag);
        curr_metadata.sstables.reverse();

        for sstable in &curr_metadata.sstables {
            if let Some(value) = sstable.get(key)? {
                return Ok(value);
            }
        }

        Ok(None)
    }
}
