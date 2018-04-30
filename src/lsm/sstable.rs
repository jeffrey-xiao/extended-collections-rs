use bincode::{deserialize, serialize};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use bloom::BloomFilter;
use lsm::{Error, Result};
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{BufWriter, BufReader, BufRead, Read, Seek, SeekFrom, Write};
use std::iter::ExactSizeIterator;
use std::fs;
use std::hash::Hash;
use std::marker::{PhantomData};
use std::path::{Path, PathBuf};

#[derive(Deserialize, Serialize)]
pub struct SSTableSummary<T> {
    pub item_count: usize,
    pub size: u64,
    pub min: Option<T>,
    pub max: Option<T>,
    pub index: Vec<(T, u64)>,
    pub tag: u64,
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

    pub fn new<P>(db_path: P, item_count_hint: usize) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let db_path = PathBuf::from(db_path.as_ref());
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
                size: 0,
                min: None,
                max: None,
                index: Vec::new(),
                tag: 0,
            },
            block_index: 0,
            block_size: (item_count_hint as f64).sqrt().ceil() as usize,
            index_block: Vec::new(),
            filter: BloomFilter::new(item_count_hint, 0.05),
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
        self.data_offset += 8 + serialized_entry.len() as u64;
        self.summary.size += 8 + serialized_entry.len() as u64;
        self.block_index += 1;

        if self.block_index == self.block_size {
            self.summary.index.push((self.index_block[0].0.clone(), self.index_offset));

            let serialized_index_block = serialize(&self.index_block).map_err(Error::SerdeError)?;
            self.index_stream.write_u64::<BigEndian>(serialized_index_block.len() as u64).map_err(Error::IOError)?;
            self.index_stream.write(&serialized_index_block).map_err(Error::IOError)?;
            self.index_offset += 8 + serialized_index_block.len() as u64;
            self.summary.size += 8 + serialized_index_block.len() as u64;
            self.block_index = 0;
            self.index_block.clear();
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<PathBuf> {
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
        self.data_stream.flush().map_err(Error::IOError)?;
        Ok(self.path.clone())
    }
}


pub struct SSTable<T, U> {
    path: PathBuf,
    pub summary: SSTableSummary<T>,
    pub filter: BloomFilter,
    _marker: PhantomData<U>,
}

impl<T, U> SSTable<T, U>
where
    T: Hash + DeserializeOwned + Ord + Serialize,
    U: DeserializeOwned + Serialize,
{
    pub fn new<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut buffer = Vec::new();
        let mut file = fs::File::open(path.as_ref().join("summary.dat")).map_err(Error::IOError)?;
        file.read_to_end(&mut buffer).map_err(Error::IOError)?;
        let summary = deserialize(&buffer).map_err(Error::SerdeError)?;

        let mut buffer = Vec::new();
        let mut file = fs::File::open(path.as_ref().join("filter.dat")).map_err(Error::IOError)?;
        file.read_to_end(&mut buffer).map_err(Error::IOError)?;
        let filter = deserialize(&buffer).map_err(Error::SerdeError)?;

        Ok(SSTable {
            path: PathBuf::from(path.as_ref()),
            summary,
            filter,
            _marker: PhantomData,
        })
    }

    fn floor_offset(index: &Vec<(T, u64)>, key: &T) -> Option<usize> {
        let mut lo = 0isize;
        let mut hi = index.len() as isize - 1;
        while (lo <= hi) {
            let mid = (lo + hi) / 2;
            if index[mid as usize].0 <= *key {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        if hi == -1 {
            None
        } else {
            Some(hi as usize)
        }
    }

    pub fn get(&self, key: &T) -> Result<Option<Option<U>>> {
        println!("looking in filter file");
        if !self.filter.contains(key) {
            return Ok(None);
        }
        println!("looking in summary file");

        let index = {
            match Self::floor_offset(&self.summary.index, key) {
                Some(index) => index,
                None => return Ok(None),
            }
        };


        let mut index_file = fs::File::open(self.path.join("index.dat")).map_err(Error::IOError)?;
        index_file.seek(SeekFrom::Start(self.summary.index[index].1)).map_err(Error::IOError)?;
        let size = index_file.read_u64::<BigEndian>().map_err(Error::IOError)?;
        let mut buffer = vec![0; size as usize];
        index_file.read(buffer.as_mut_slice()).map_err(Error::IOError)?;
        let index_block: Vec<(T, u64)> = deserialize(&buffer).map_err(Error::SerdeError)?;

        println!("looking in index file");

        let index = {
            match index_block.binary_search_by_key(&key, |index_entry| &index_entry.0) {
                Ok(index) => index,
                Err(_) => return Ok(None),
            }
        };

        println!("looking in data file");

        let mut data_file = fs::File::open(self.path.join("data.dat")).map_err(Error::IOError)?;
        data_file.seek(SeekFrom::Start(index_block[index].1)).map_err(Error::IOError)?;
        let size = data_file.read_u64::<BigEndian>().map_err(Error::IOError)?;
        let mut buffer = vec![0; size as usize];
        data_file.read(buffer.as_mut_slice()).map_err(Error::IOError)?;
        deserialize(&buffer).map_err(Error::SerdeError).map(|entry: (T, Option<U>)| Some(entry.1))
    }
}
