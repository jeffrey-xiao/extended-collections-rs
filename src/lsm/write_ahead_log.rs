use bincode::{deserialize, self, serialize, serialized_size};
use lsm::{Error, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{Write, self};
use std::marker::{PhantomData, Send, Sync};
use std::hash::Hash;

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
            .open(log_path)
            .map_err(Error::IOError)?;

        Ok(WriteAheadLog {
            log,
            _marker: PhantomData,
        })
    }

    pub fn clear(&self) -> Result<()> {
        self.log.set_len(0).map_err(Error::IOError)
    }

    pub fn append(&mut self, key: T, value: Option<U>) -> Result<()> {
        let serialized_entry = serialize(&(key, value)).map_err(Error::SerdeError)?;
        self.log.write_all(&serialized_entry).map_err(Error::IOError)
    }
}

