use crate::entry::Entry;
use crate::lsm_tree::{Error, Result};
use bincode::{deserialize, serialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use probabilistic_collections::bloom::BloomFilter;
use rand::{thread_rng, Rng};
use serde::de::{self, Deserialize, DeserializeOwned, Deserializer};
use serde::ser::{Serialize, Serializer};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::cmp;
use std::fmt::{self, Debug};
use std::fs;
use std::hash::Hash;
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::result;

pub fn merge_ranges<T>(range_1: (T, T), range_2: (T, T)) -> (T, T)
where
    T: Ord,
{
    (
        cmp::min(range_1.0, range_2.0),
        cmp::max(range_1.1, range_2.1),
    )
}

pub fn is_intersecting<T>(range_1: &(T, T), range_2: &(T, T)) -> bool
where
    T: Ord,
{
    let l = cmp::max(&range_1.0, &range_2.0);
    let r = cmp::min(&range_1.1, &range_2.1);
    l <= r
}

#[derive(Deserialize, Serialize)]
pub struct SSTableValue<U> {
    pub data: Option<U>,
    pub logical_time: u64,
}

impl<U> PartialEq for SSTableValue<U> {
    fn eq(&self, other: &SSTableValue<U>) -> bool {
        self.logical_time == other.logical_time
    }
}

impl<U> Ord for SSTableValue<U> {
    fn cmp(&self, other: &SSTableValue<U>) -> cmp::Ordering {
        other.logical_time.cmp(&self.logical_time)
    }
}

impl<U> PartialOrd for SSTableValue<U> {
    fn partial_cmp(&self, other: &SSTableValue<U>) -> Option<cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl<U> Eq for SSTableValue<U> {}

#[derive(Debug, Deserialize, Serialize)]
pub struct SSTableSummary<T> {
    pub entry_count: usize,
    pub tombstone_count: usize,
    pub size: u64,
    pub key_range: (T, T),
    pub logical_time_range: (u64, u64),
    pub index: Vec<(T, u64)>,
}

pub struct SSTableBuilder<T, U> {
    pub sstable_path: PathBuf,

    pub entry_count: usize,
    pub tombstone_count: usize,
    pub size: u64,
    pub key_range: Option<(T, T)>,
    pub logical_time_range: Option<(u64, u64)>,
    pub index: Vec<(T, u64)>,

    block_index: usize,
    block_size: usize,
    index_block: Vec<(T, u64)>,
    filter: BloomFilter<T>,
    index_offset: u64,
    index_stream: BufWriter<fs::File>,
    data_offset: u64,
    data_stream: BufWriter<fs::File>,
    _marker: PhantomData<U>,
}

impl<T, U> SSTableBuilder<T, U> {
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

            entry_count: 0,
            tombstone_count: 0,
            size: 0,
            key_range: None,
            logical_time_range: None,
            index: Vec::new(),

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

    pub fn append(&mut self, key: T, value: SSTableValue<U>) -> Result<()>
    where
        T: Clone + Hash + Serialize,
        U: Serialize,
    {
        let logical_time = value.logical_time;
        self.entry_count += 1;
        if value.data.is_none() {
            self.tombstone_count += 1;
        }
        match self.key_range.take() {
            Some((start, _)) => self.key_range = Some((start, key.clone())),
            None => self.key_range = Some((key.clone(), key.clone())),
        }
        match self.logical_time_range.take() {
            Some((start, end)) => {
                let start = cmp::min(start, logical_time);
                let end = cmp::max(end, logical_time);
                self.logical_time_range = Some((start, end))
            }
            None => self.logical_time_range = Some((logical_time, logical_time)),
        }

        self.filter.insert(&key);
        self.index_block.push((key.clone(), self.data_offset));

        let serialized_entry = serialize(&(key, value))?;
        self.data_stream
            .write_u64::<BigEndian>(serialized_entry.len() as u64)?;
        self.data_stream.write_all(&serialized_entry)?;
        self.data_offset += 8 + serialized_entry.len() as u64;
        self.size += 8 + serialized_entry.len() as u64;
        self.block_index += 1;

        if self.block_index == self.block_size {
            self.index
                .push((self.index_block[0].0.clone(), self.index_offset));

            let serialized_index_block = serialize(&self.index_block)?;
            self.index_stream
                .write_u64::<BigEndian>(serialized_index_block.len() as u64)?;
            self.index_stream.write_all(&serialized_index_block)?;
            self.index_offset += 8 + serialized_index_block.len() as u64;
            self.size += 8 + serialized_index_block.len() as u64;
            self.block_index = 0;
            self.index_block.clear();
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<PathBuf>
    where
        T: Clone + Serialize,
    {
        if !self.index_block.is_empty() {
            self.index
                .push((self.index_block[0].0.clone(), self.index_offset));

            let serialized_index_block = serialize(&self.index_block)?;
            self.index_stream
                .write_u64::<BigEndian>(serialized_index_block.len() as u64)?;
            self.index_stream.write_all(&serialized_index_block)?;
        }

        let key_range = {
            match self.key_range.clone() {
                Some(key_range) => key_range,
                _ => panic!("Expected non-empty SSTable."),
            }
        };

        let logical_time_range = {
            match self.logical_time_range {
                Some(logical_time_range) => logical_time_range,
                _ => panic!("Expected non-empty SSTable."),
            }
        };

        let serialized_summary = serialize(&SSTableSummary {
            entry_count: self.entry_count,
            tombstone_count: self.tombstone_count,
            size: self.size,
            key_range,
            logical_time_range,
            index: self.index.clone(),
        })?;
        fs::write(self.sstable_path.join("summary.dat"), &serialized_summary)?;

        let serialized_filter = serialize(&self.filter)?;
        fs::write(self.sstable_path.join("filter.dat"), &serialized_filter)?;

        self.index_stream.flush()?;
        self.data_stream.flush()?;
        Ok(self.sstable_path.clone())
    }
}

pub struct SSTable<T, U> {
    pub path: PathBuf,
    pub summary: SSTableSummary<T>,
    pub filter: BloomFilter<T>,
    _marker: PhantomData<U>,
}

impl<T, U> SSTable<T, U> {
    pub fn new<P>(path: P) -> Result<Self>
    where
        T: DeserializeOwned,
        P: AsRef<Path>,
    {
        let buffer = fs::read(path.as_ref().join("summary.dat"))?;
        let summary = deserialize(&buffer)?;

        let buffer = fs::read(path.as_ref().join("filter.dat"))?;
        let filter = deserialize(&buffer)?;

        Ok(SSTable {
            path: PathBuf::from(path.as_ref()),
            summary,
            filter,
            _marker: PhantomData,
        })
    }

    fn floor_offset<V>(index: &[(T, u64)], key: &V) -> Option<usize>
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        let mut lo = 0isize;
        let mut hi = index.len() as isize - 1;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            if index[mid as usize].0.borrow() <= key {
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

    pub fn get<V>(&self, key: &V) -> Result<Option<SSTableValue<U>>>
    where
        T: Borrow<V> + DeserializeOwned,
        U: DeserializeOwned,
        V: Ord + Hash + ?Sized,
    {
        if key < self.summary.key_range.0.borrow() || key > self.summary.key_range.1.borrow() {
            return Ok(None);
        }

        if !self.filter.contains(key) {
            return Ok(None);
        }

        let index = match Self::floor_offset(&self.summary.index, key) {
            Some(index) => index,
            None => return Ok(None),
        };

        let mut index_file = fs::File::open(self.path.join("index.dat"))?;
        index_file.seek(SeekFrom::Start(self.summary.index[index].1))?;
        let size = index_file.read_u64::<BigEndian>()?;
        let mut buffer = vec![0; size as usize];
        index_file.read_exact(buffer.as_mut_slice())?;
        let index_block: Vec<(T, u64)> = deserialize(&buffer)?;

        let index = {
            match index_block.binary_search_by_key(&key, |index_entry| index_entry.0.borrow()) {
                Ok(index) => index,
                Err(_) => return Ok(None),
            }
        };

        let mut data_file = fs::File::open(self.path.join("data.dat"))?;
        data_file.seek(SeekFrom::Start(index_block[index].1))?;
        let size = data_file.read_u64::<BigEndian>()?;
        let mut buffer = vec![0; size as usize];
        data_file.read_exact(buffer.as_mut_slice())?;
        deserialize(&buffer)
            .map_err(Error::SerdeError)
            .map(|entry: Entry<T, SSTableValue<U>>| Some(entry.value))
    }

    pub fn data_iter(&self) -> SSTableDataIter<T, U> {
        SSTableDataIter {
            data_path: self.path.join("data.dat"),
            data_file: None,
            _marker: PhantomData,
        }
    }
}

pub struct SSTableDataIter<T, U> {
    data_path: PathBuf,
    data_file: Option<fs::File>,
    _marker: PhantomData<(T, U)>,
}

impl<T, U> Iterator for SSTableDataIter<T, U>
where
    T: DeserializeOwned,
    U: DeserializeOwned,
{
    type Item = Result<Entry<T, SSTableValue<U>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_file.is_none() {
            match fs::File::open(self.data_path.as_path()) {
                Ok(data_file) => self.data_file = Some(data_file),
                Err(error) => return Some(Err(Error::from(error))),
            }
        }

        let data_file = self.data_file.as_mut().expect("Expected opened file.");

        let size = match data_file.read_u64::<BigEndian>() {
            Ok(size) => size,
            Err(error) => match error.kind() {
                ErrorKind::UnexpectedEof => return None,
                _ => return Some(Err(Error::from(error))),
            },
        };

        let mut buffer = vec![0; size as usize];
        let result = data_file.read_exact(buffer.as_mut_slice());
        if let Err(error) = result {
            return Some(Err(Error::from(error)));
        }

        Some(deserialize(&buffer).map_err(Error::SerdeError))
    }
}

impl<T, U> Serialize for SSTable<T, U> {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.path.serialize(serializer)
    }
}

impl<'de, T, U> Deserialize<'de> for SSTable<T, U>
where
    T: DeserializeOwned,
    U: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> result::Result<SSTable<T, U>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ret = SSTable::new(PathBuf::deserialize(deserializer)?).map_err(de::Error::custom);
        Ok(ret?)
    }
}

impl<T, U> Debug for SSTable<T, U>
where
    T: Debug,
    U: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "entry count: {:?}", self.summary.entry_count)?;
        writeln!(f, "tombstone count: {:?}", self.summary.tombstone_count)?;
        writeln!(f, "key range: {:?}", self.summary.key_range)
    }
}
