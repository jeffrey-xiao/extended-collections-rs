use bincode::{deserialize, serialize};
use bloom::BloomFilter;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{Error, Result, Write};
use std::marker::PhantomData;

trait CompactionStrategy {
    fn should_compact(&self) -> bool;

    fn compact(&self);
}

struct SizeTieredStrategy {}

struct LevelTieredStrategy {}

pub struct SSTable<T: Hash, U>
where
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    file: File,
    bloom_filter: BloomFilter<T>,
    data: Option<(T, Option<U>)>,
    _marker: PhantomData<(T, U)>,
}

pub struct Metadata {}

pub struct WriteAheadLog<T, U>
where
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    log: File,
    _marker: PhantomData<(T, U)>,
}

impl<T, U> WriteAheadLog<T, U>
where
    T: Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    pub fn new(log_path: &str) -> Result<WriteAheadLog<T, U>> {
        let log = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(log_path)?;

        Ok(WriteAheadLog {
            log,
            _marker: PhantomData,
        })
    }

    pub fn clear(&self) -> Result<()> {
        self.log.set_len(0)
    }

    pub fn append(&mut self, key: T, value: Option<U>) {
        self.log
            .write_all(&serialize(&(key, value)).unwrap())
            .unwrap();
    }
}

pub struct Tree {}
