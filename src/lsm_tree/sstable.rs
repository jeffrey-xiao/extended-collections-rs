use bincode::{deserialize, serialize};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use bloom::BloomFilter;
use entry::Entry;
use lsm_tree::{Error, Result};
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::fs;
use std::hash::Hash;
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::marker::{PhantomData};
use std::path::{Path, PathBuf};

#[derive(Clone, Deserialize, Serialize)]
pub struct SSTableSummary<T> {
    pub entry_count: usize,
    pub tombstone_count: usize,
    pub size: u64,
    pub min_entry: Option<T>,
    pub max_entry: Option<T>,
    pub index: Vec<(T, u64)>,
    pub tag: u64,
}

pub struct SSTableBuilder<T, U> {
    pub sstable_path: PathBuf,
    pub summary: SSTableSummary<T>,
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

    pub fn new<P>(db_path: P, entry_count_hint: usize) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let db_path = PathBuf::from(db_path.as_ref());
        let sstable_path = db_path.join(Self::generate_file_name());
        fs::create_dir(sstable_path.as_path())?;

        let data_file = fs::File::create(sstable_path.join("data.dat"))?;
        let data_stream = BufWriter::new(data_file);

        let index_file = fs::File::create(sstable_path.join("index.dat"))?;
        let index_stream = BufWriter::new(index_file);

        Ok(SSTableBuilder {
            sstable_path,
            summary: SSTableSummary {
                entry_count: 0,
                tombstone_count: 0,
                size: 0,
                min_entry: None,
                max_entry: None,
                index: Vec::new(),
                tag: 0,
            },
            block_index: 0,
            block_size: (entry_count_hint as f64).sqrt().ceil() as usize,
            index_block: Vec::new(),
            filter: BloomFilter::new(entry_count_hint, 0.05),
            index_offset: 0,
            index_stream,
            data_offset: 0,
            data_stream,
            _marker: PhantomData,
        })
    }

    pub fn append(&mut self, key: T, value: Option<U>) -> Result<()> {
        self.summary.entry_count += 1;
        if value.is_none() {
            self.summary.tombstone_count += 1;
        }
        if self.summary.min_entry.is_none() {
            self.summary.min_entry = Some(key.clone());
        }
        self.summary.max_entry = Some(key.clone());

        self.filter.insert(&key);
        self.index_block.push((key.clone(), self.data_offset));

        let serialized_entry = serialize(&(key.clone(), value))?;
        self.data_stream.write_u64::<BigEndian>(serialized_entry.len() as u64)?;
        self.data_stream.write(&serialized_entry)?;
        self.data_offset += 8 + serialized_entry.len() as u64;
        self.summary.size += 8 + serialized_entry.len() as u64;
        self.block_index += 1;

        if self.block_index == self.block_size {
            self.summary.index.push((self.index_block[0].0.clone(), self.index_offset));

            let serialized_index_block = serialize(&self.index_block)?;
            self.index_stream.write_u64::<BigEndian>(serialized_index_block.len() as u64)?;
            self.index_stream.write(&serialized_index_block)?;
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

            let serialized_index_block = serialize(&self.index_block)?;
            self.index_stream.write_u64::<BigEndian>(serialized_index_block.len() as u64)?;
            self.index_stream.write(&serialized_index_block)?;
        }

        let serialized_summary = serialize(&self.summary)?;
        let mut summary_file = fs::File::create(self.sstable_path.join("summary.dat"))?;
        summary_file.write_all(&serialized_summary)?;

        let serialized_filter = serialize(&self.filter)?;
        let mut filter_file = fs::File::create(self.sstable_path.join("filter.dat"))?;
        filter_file.write_all(&serialized_filter)?;

        self.index_stream.flush()?;
        self.data_stream.flush()?;
        Ok(self.sstable_path.clone())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SSTable<T, U> {
    pub path: PathBuf,
    pub summary: SSTableSummary<T>,
    pub filter: BloomFilter,
    _marker: PhantomData<U>,
}

impl<T, U> SSTable<T, U>
where
    T: DeserializeOwned,
    U: DeserializeOwned,
{
    pub fn new<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut buffer = Vec::new();
        let mut file = fs::File::open(path.as_ref().join("summary.dat"))?;
        file.read_to_end(&mut buffer)?;
        let summary = deserialize(&buffer)?;

        let mut buffer = Vec::new();
        let mut file = fs::File::open(path.as_ref().join("filter.dat"))?;
        file.read_to_end(&mut buffer)?;
        let filter = deserialize(&buffer)?;

        Ok(SSTable {
            path: PathBuf::from(path.as_ref()),
            summary,
            filter,
            _marker: PhantomData,
        })
    }
}

impl<T, U> SSTable<T, U>
where
    T: Hash + DeserializeOwned + Ord + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn floor_offset(index: &Vec<(T, u64)>, key: &T) -> Option<usize> {
        let mut lo = 0isize;
        let mut hi = index.len() as isize - 1;
        while lo <= hi {
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
        if !self.filter.contains(key) {
            return Ok(None);
        }

        let index = {
            match Self::floor_offset(&self.summary.index, key) {
                Some(index) => index,
                None => return Ok(None),
            }
        };

        let mut index_file = fs::File::open(self.path.join("index.dat"))?;
        index_file.seek(SeekFrom::Start(self.summary.index[index].1))?;
        let size = index_file.read_u64::<BigEndian>()?;
        let mut buffer = vec![0; size as usize];
        index_file.read(buffer.as_mut_slice())?;
        let index_block: Vec<(T, u64)> = deserialize(&buffer)?;


        let index = {
            match index_block.binary_search_by_key(&key, |index_entry| &index_entry.0) {
                Ok(index) => index,
                Err(_) => return Ok(None),
            }
        };

        let mut data_file = fs::File::open(self.path.join("data.dat"))?;
        data_file.seek(SeekFrom::Start(index_block[index].1))?;
        let size = data_file.read_u64::<BigEndian>()?;
        let mut buffer = vec![0; size as usize];
        data_file.read(buffer.as_mut_slice())?;
        deserialize(&buffer)
            .map_err(Error::SerdeError)
            .map(|entry: Entry<T, Option<U>>| Some(entry.value))
    }

    pub fn data_iter(&self) -> Result<SSTableDataIter<T, U>> {
        Ok(SSTableDataIter {
            data_file: fs::File::open(self.path.join("data.dat"))?,
            _marker: PhantomData,
        })
    }
}

pub struct SSTableDataIter<T, U> {
    data_file: fs::File,
    _marker: PhantomData<(T, U)>,
}

impl<T, U> Iterator for SSTableDataIter<T, U>
where
    T: DeserializeOwned,
    U: DeserializeOwned,
{
    type Item = Result<Entry<T, Option<U>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let size = {
            match self.data_file.read_u64::<BigEndian>() {
                Ok(size) => size,
                Err(error) => {
                    match error.kind() {
                        ErrorKind::UnexpectedEof => return None,
                        _ => return Some(Err(Error::from(error))),
                    }
                }
            }
        };

        let mut buffer = vec![0; size as usize];
        let result = self.data_file.read(buffer.as_mut_slice());
        if let Err(error) = result {
            return Some(Err(Error::from(error)));
        }

        Some(deserialize(&buffer).map_err(Error::SerdeError))
    }
}
