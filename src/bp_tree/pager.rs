use bincode::{self, deserialize, serialize, serialized_size};
use bp_tree::node::{LeafNode, Node};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::borrow::Borrow;
use std::error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::mem;
use std::path::Path;
use std::result;

/// Convenience `Error` enum for `bp_tree`.
#[derive(Debug)]
pub enum Error {
    /// An input or output error.
    IOError(io::Error),
    /// A serialization or deserialization error.
    SerdeError(bincode::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Error {
        Error::SerdeError(err)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::IOError(ref error) => error.description(),
            Error::SerdeError(ref error) => error.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            Error::IOError(ref error) => error.cause(),
            Error::SerdeError(ref error) => error.cause(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IOError(ref error) => write!(f, "{}", error),
            Error::SerdeError(ref error) => write!(f, "{}", error),
        }
    }
}

/// Convenience `Result` type for `bp_tree`.
pub type Result<T> = result::Result<T, Error>;

#[derive(Serialize, Deserialize)]
struct Metadata {
    pages: usize,
    len: usize,
    root_page: usize,
    key_size: u64,
    value_size: u64,
    leaf_degree: usize,
    internal_degree: usize,
    free_page: Option<usize>,
}

pub struct Pager<T, U> {
    db_file: File,
    metadata: Metadata,
    _marker: PhantomData<(T, U)>,
}

impl<T, U> Pager<T, U> {
    pub fn new<P>(
        file_path: P,
        key_size: u64,
        value_size: u64,
        leaf_degree: usize,
        internal_degree: usize,
    ) -> Result<Pager<T, U>>
    where
        T: Serialize,
        U: Serialize,
        P: AsRef<Path>,
    {
        let header_size = Self::get_metadata_size();
        let body_size =
            Node::<T, U>::get_max_size(key_size, value_size, leaf_degree, internal_degree) as u64;
        let metadata = Metadata {
            pages: 1,
            len: 0,
            root_page: 0,
            key_size,
            value_size,
            leaf_degree,
            internal_degree,
            free_page: None,
        };
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        db_file.set_len(header_size + body_size)?;

        db_file.seek(SeekFrom::Start(0))?;
        let serialized_metadata = &serialize(&metadata)?;
        db_file.write_all(serialized_metadata)?;

        db_file.seek(SeekFrom::Start(header_size))?;
        let serialized_node = &serialize(&Node::Leaf(LeafNode::<T, U>::new(leaf_degree)))?;
        db_file.write_all(serialized_node)?;

        let pager = Pager {
            db_file,
            metadata,
            _marker: PhantomData,
        };

        Ok(pager)
    }

    pub fn open<P>(file_path: P) -> Result<Pager<T, U>>
    where
        P: AsRef<Path>,
    {
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        db_file.seek(SeekFrom::Start(0))?;

        let mut buffer: Vec<u8> = vec![0; Self::get_metadata_size() as usize];
        db_file.read_exact(buffer.as_mut_slice())?;
        let metadata = deserialize(buffer.as_slice())?;

        Ok(Pager {
            db_file,
            metadata,
            _marker: PhantomData,
        })
    }

    #[inline]
    fn get_node_size(&self) -> u64 {
        Node::<T, U>::get_max_size(
            self.metadata.key_size,
            self.metadata.value_size,
            self.metadata.leaf_degree,
            self.metadata.internal_degree,
        ) as u64
    }

    #[inline]
    fn get_metadata_size() -> u64 {
        mem::size_of::<Metadata>() as u64
    }

    fn calculate_page_offset(&self, index: usize) -> u64 {
        let header_size = Self::get_metadata_size();
        let body_offset = self.get_node_size() * index as u64;
        header_size + body_offset
    }

    pub fn get_leaf_degree(&self) -> usize {
        self.metadata.leaf_degree
    }

    pub fn get_internal_degree(&self) -> usize {
        self.metadata.internal_degree
    }

    pub fn get_len(&self) -> usize {
        self.metadata.len
    }

    pub fn set_len(&mut self, len: usize) -> Result<()> {
        self.metadata.len = len;
        self.db_file.seek(SeekFrom::Start(0))?;
        let serialized_metadata = &serialize(&self.metadata)?;
        self.db_file
            .write_all(serialized_metadata)
            .map_err(Error::IOError)
    }

    pub fn get_root_page(&self) -> usize {
        self.metadata.root_page
    }

    pub fn set_root_page(&mut self, new_root_page: usize) -> Result<()> {
        self.metadata.root_page = new_root_page;
        self.db_file.seek(SeekFrom::Start(0))?;
        let serialized_metadata = &serialize(&self.metadata)?;
        self.db_file
            .write_all(serialized_metadata)
            .map_err(Error::IOError)
    }

    pub fn get_page(&mut self, index: usize) -> Result<Node<T, U>>
    where
        T: DeserializeOwned,
        U: DeserializeOwned,
    {
        let offset = self.calculate_page_offset(index);
        self.db_file.seek(SeekFrom::Start(offset))?;
        let mut buffer: Vec<u8> = vec![0; self.get_node_size() as usize];
        self.db_file.read_exact(buffer.as_mut_slice())?;
        deserialize(buffer.as_slice()).map_err(Error::SerdeError)
    }

    pub fn allocate_node(&mut self, new_node: &Node<T, U>) -> Result<usize>
    where
        T: DeserializeOwned + Serialize,
        U: DeserializeOwned + Serialize,
    {
        match self.metadata.free_page {
            None => {
                self.metadata.pages += 1;
                let len = self.calculate_page_offset(self.metadata.pages);
                let node_size = self.get_node_size();
                self.db_file.set_len(len)?;
                self.db_file.seek(SeekFrom::Start(len - node_size))?;
                let serialized_node = &serialize(&new_node)?;
                self.db_file.write_all(serialized_node)?;

                self.db_file.seek(SeekFrom::Start(0))?;
                let serialized_metadata = &serialize(&self.metadata)?;
                self.db_file.write_all(serialized_metadata)?;

                Ok(self.metadata.pages - 1)
            },
            Some(free_page) => {
                let offset = self.calculate_page_offset(free_page);
                let mut buffer: Vec<u8> = vec![0; self.get_node_size() as usize];

                self.db_file.seek(SeekFrom::Start(offset))?;
                self.db_file.read_exact(buffer.as_mut_slice())?;

                self.db_file.seek(SeekFrom::Start(offset))?;
                let serialized_node = &serialize(&new_node)?;
                self.db_file.write_all(serialized_node)?;

                match deserialize(buffer.as_slice())? {
                    Node::Free::<T, U>(new_free_page) => self.metadata.free_page = new_free_page,
                    _ => panic!("Expected a free node."),
                }
                self.db_file.seek(SeekFrom::Start(0))?;
                let serialized_metadata = &serialize(&self.metadata)?;
                self.db_file.write_all(serialized_metadata)?;

                Ok(free_page)
            },
        }
    }

    pub fn deallocate_node(&mut self, index: usize) -> Result<()>
    where
        T: Serialize,
        U: Serialize,
    {
        let offset = self.calculate_page_offset(index);

        self.db_file.seek(SeekFrom::Start(offset))?;
        let serialized_node = &serialize(&Node::Free::<T, U>(self.metadata.free_page))?;
        self.db_file.write_all(serialized_node)?;

        self.metadata.free_page = Some(index);
        self.db_file.seek(SeekFrom::Start(0))?;
        let serialized_metadata = &serialize(&self.metadata)?;
        self.db_file
            .write_all(serialized_metadata)
            .map_err(Error::IOError)
    }

    pub fn write_node(&mut self, index: usize, node: &Node<T, U>) -> Result<()>
    where
        T: Serialize,
        U: Serialize,
    {
        let offset = self.calculate_page_offset(index);
        self.db_file.seek(SeekFrom::Start(offset))?;
        let serialized_node = &serialize(&node)?;
        self.db_file
            .write_all(serialized_node)
            .map_err(Error::IOError)
    }

    pub fn clear(&mut self) -> Result<()>
    where
        T: Serialize,
        U: Serialize,
    {
        let header_size = Self::get_metadata_size();
        let body_size = self.get_node_size();
        self.metadata.pages = 1;
        self.metadata.len = 0;
        self.metadata.root_page = 0;
        self.metadata.free_page = None;
        self.db_file.set_len(header_size + body_size)?;

        self.db_file.seek(SeekFrom::Start(0))?;
        let serialized_metadata = &serialize(&self.metadata)?;
        self.db_file.write_all(serialized_metadata)?;

        self.db_file.seek(SeekFrom::Start(header_size))?;
        let serialized_node = &serialize(&Node::Leaf(LeafNode::<T, U>::new(
            self.metadata.leaf_degree,
        )))?;
        self.db_file
            .write_all(serialized_node)
            .map_err(Error::IOError)
    }

    pub fn validate_key<V>(&self, key: &V) -> Result<()>
    where
        T: Borrow<V>,
        V: Serialize + ?Sized,
    {
        assert!(serialized_size(key)? <= self.metadata.key_size);
        Ok(())
    }

    pub fn validate_value<V>(&self, value: &V) -> Result<()>
    where
        U: Borrow<V>,
        V: Serialize + ?Sized,
    {
        assert!(serialized_size(value)? <= self.metadata.value_size);
        Ok(())
    }
}
