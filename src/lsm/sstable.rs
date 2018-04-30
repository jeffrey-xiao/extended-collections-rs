use bincode::{deserialize, self, serialize, serialized_size};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian, LittleEndian};
use bloom::BloomFilter;
use lsm::{Error, Result};
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{BufWriter, BufReader, BufRead, Read, Write};
use std::iter::ExactSizeIterator;
use std::fs;
use std::hash::Hash;
use std::marker::{PhantomData};
use std::path::{Path, PathBuf};

#[derive(Deserialize, Serialize)]
pub struct SSTableSummary<T> {
    item_count: usize,
    min: Option<T>,
    max: Option<T>,
    index: Vec<(T, u64)>,
}

pub struct SSTableBuilder<T, U> {
    path: PathBuf,
    summary: SSTableSummary<T>,
    block_index: usize,
    block_size: usize,
    index_block: Vec<(T, u64)>,
    filter: BloomFilter,
    index_offset: u64,
    index_stream: BufWriter<fs::File>,
    data_offset: u64,
    data_stream: BufWriter<fs::File>,
    _marker: PhantomData<U>,
}

impl<T, U> SSTableBuilder<T, U>
where
    T: Clone + DeserializeOwned + Hash + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn generate_file_name() -> String {
        thread_rng().gen_ascii_chars().take(32).collect()
    }

    pub fn new(db_path: &PathBuf, item_count_hint: usize) -> Result<Self> {
        let sstable_path = db_path.join(Self::generate_file_name());
        fs::create_dir(sstable_path.clone()).map_err(Error::IOError)?;

        let data_file = fs::File::create(sstable_path.join("data.dat")).map_err(Error::IOError)?;
        let mut data_stream = BufWriter::new(data_file);

        let index_file = fs::File::create(sstable_path.join("index.dat")).map_err(Error::IOError)?;
        let mut index_stream = BufWriter::new(index_file);

        Ok(SSTableBuilder {
            path: sstable_path,
            summary: SSTableSummary {
                item_count: 0,
                min: None,
                max: None,
                index: Vec::new(),
            },
            block_index: 0,
            block_size: (item_count_hint as f64).sqrt().ceil() as usize,
            index_block: Vec::new(),
            filter: BloomFilter::new(item_count_hint, 0.01),
            index_offset: 0,
            index_stream,
            data_offset: 0,
            data_stream,
            _marker: PhantomData,
        })
    }

    pub fn append(&mut self, key: T, value: Option<U>) -> Result<()> {
        self.summary.item_count += 1;
        if self.summary.min.is_none() {
            self.summary.min = Some(key.clone());
        }
        self.summary.max = Some(key.clone());

        self.filter.insert(&key);
        self.index_block.push((key.clone(), self.data_offset));

        let serialized_entry = serialize(&(key.clone(), value)).map_err(Error::SerdeError)?;
        self.data_stream.write_u64::<BigEndian>(serialized_entry.len() as u64).map_err(Error::IOError)?;
        self.data_stream.write(&serialized_entry).map_err(Error::IOError)?;
        self.data_offset += serialized_entry.len() as u64;
        self.block_index += 1;

        if self.block_index == self.block_size {
            self.summary.index.push((self.index_block[0].0.clone(), self.index_offset));

            let serialized_index_block = serialize(&self.index_block).map_err(Error::SerdeError)?;
            self.index_stream.write_u64::<BigEndian>(serialized_index_block.len() as u64).map_err(Error::IOError)?;
            self.index_stream.write(&serialized_index_block).map_err(Error::IOError)?;
            self.index_offset += serialized_entry.len() as u64;
            self.block_index = 0;
            self.index_block.clear();
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.index_block.is_empty() {
            self.summary.index.push((self.index_block[0].0.clone(), self.index_offset));

            let serialized_index_block = serialize(&self.index_block).map_err(Error::SerdeError)?;
            self.index_stream.write_u64::<BigEndian>(serialized_index_block.len() as u64).map_err(Error::IOError)?;
            self.index_stream.write(&serialized_index_block).map_err(Error::IOError)?;
        }

        let serialized_summary = serialize(&self.summary).map_err(Error::SerdeError)?;
        let mut summary_file = fs::File::create(self.path.join("summary.dat")).map_err(Error::IOError)?;
        summary_file.write_all(&serialized_summary).map_err(Error::IOError)?;

        let serialized_filter = serialize(&self.filter).map_err(Error::SerdeError)?;
        let mut filter_file = fs::File::create(self.path.join("filter.dat")).map_err(Error::IOError)?;
        filter_file.write_all(&serialized_filter).map_err(Error::IOError)?;

        self.index_stream.flush().map_err(Error::IOError)?;
        self.data_stream.flush().map_err(Error::IOError)
    }
}


pub struct SSTable<T, U> {
    path: PathBuf,
    bloom_filter: Option<BloomFilter>,
    index: Option<Vec<(T, u64)>>,
    _marker: PhantomData<U>,
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
}
