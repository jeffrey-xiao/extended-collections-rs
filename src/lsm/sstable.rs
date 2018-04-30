use bincode::{deserialize, self, serialize, serialized_size};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};
use bloom::BloomFilter;
use lsm::{Error, Result};
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{BufWriter, BufReader, BufRead, Read, Write};
use std::iter::Iterator;
use std::fs;
use std::hash::Hash;
use std::marker::{PhantomData};
use std::path::{PathBuf};

pub struct SSTable<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    path: PathBuf,
    bloom_filter: Option<BloomFilter>,
    index: Option<Vec<(T, u64)>>,
    _marker: PhantomData<(T, U)>,
}

impl<T, U> SSTable<T, U>
where
    T: Hash + DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    pub fn new(path: PathBuf) -> Self {
        SSTable {
            path,
            bloom_filter: None,
            index: None,
            _marker: PhantomData,
        }
    }

    fn generate_file_name() -> String {
        thread_rng().gen_ascii_chars().take(32).collect()
    }

    pub fn flush<V>(db_path: PathBuf, items: V) -> Result<PathBuf>
    where
        V: Iterator<Item=(T, U)>,
    {
        // create sstable dir
        let sstable_path = db_path.join(Self::generate_file_name());
        fs::create_dir(sstable_path.clone()).map_err(Error::IOError)?;

        // write data file
        let data_path = sstable_path.join("data.dat");
        let mut data_stream = BufWriter::new(fs::File::open(data_path).map_err(Error::IOError)?);

        for item in items {
            let size = serialized_size(&item).map_err(Error::SerdeError)?;
            data_stream.write_u64::<BigEndian>(size).map_err(Error::IOError)?;
            let serialized_item = serialize(&item).map_err(Error::SerdeError)?;
            data_stream.write(&serialized_item).map_err(Error::IOError)?;
        }

        Ok(sstable_path)
    }
}
