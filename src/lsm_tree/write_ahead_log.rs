use bincode::serialize;
use lsm_tree::{Error, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;

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
        self.log.set_len(0).map_err(Error::IOError)
    }

    pub fn append(&mut self, key: T, value: Option<U>) -> Result<()> {
        let serialized_entry = serialize(&(key, value))?;
        self.log.write_all(&serialized_entry).map_err(Error::IOError)
    }
}

