use bincode::{serialize, deserialize};
use bptree::node::{LeafNode, Node};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem;

#[derive(Serialize, Deserialize)]
struct Metadata {
    pages: u64,
    len: usize,
    root_page: u64,
    leaf_degree: usize,
    internal_degree: usize,
    free_page: Option<u64>,
}

pub struct Pager<T: Ord + Clone + Serialize + DeserializeOwned, U: Serialize + DeserializeOwned> {
    db_file: File,
    metadata: Metadata,
    _marker: PhantomData<(T, U)>,
}

impl<T: Ord + Clone + Serialize + DeserializeOwned, U: Serialize + DeserializeOwned> Pager<T, U> {
    pub fn new(file_path: &str, leaf_degree: usize, internal_degree: usize) -> Result<Pager<T, U>, Error> {
        let header_size = Self::get_metadata_size();
        let body_size = Node::<T, U>::get_max_size(leaf_degree, internal_degree) as u64;
        let metadata = Metadata {
            pages: 1,
            len: 0,
            root_page: 0,
            leaf_degree,
            internal_degree,
            free_page: None,
        };
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        db_file.set_len(header_size + body_size).unwrap();
        db_file.seek(SeekFrom::Start(0)).unwrap();
        db_file.write(&serialize(&metadata).unwrap()).unwrap();
        db_file.seek(SeekFrom::Start(header_size)).unwrap();
        db_file.write(&serialize(&Node::Leaf(LeafNode::<T, U>::new(leaf_degree))).unwrap()).unwrap();

        let pager = Pager {
            db_file,
            metadata,
            _marker: PhantomData,
        };

        Ok(pager)
    }

    pub fn open(file_path: &str) -> Result<Pager<T, U>, Error> {
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        db_file.seek(SeekFrom::Start(0)).unwrap();

        let mut buffer: Vec<u8> = vec![0; Self::get_metadata_size() as usize];
        db_file.read(buffer.as_mut_slice()).unwrap();
        let metadata = deserialize(buffer.as_slice()).unwrap();

        Ok(Pager {
            db_file,
            metadata,
            _marker: PhantomData,
        })
    }

    #[inline]
    fn get_node_size(&self) -> u64 {
        Node::<T, U>::get_max_size(self.metadata.leaf_degree, self.metadata.internal_degree) as u64
    }

    #[inline]
    fn get_metadata_size() -> u64 {
        mem::size_of::<Metadata>() as u64
    }

    fn calculate_page_offset(&self, index: u64) -> u64 {
        let header_size = Self::get_metadata_size();
        let body_offset = self.get_node_size() * index;
        return header_size + body_offset
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

    pub fn set_len(&mut self, len: usize) {
        self.metadata.len = len;
        self.db_file.seek(SeekFrom::Start(0)).unwrap();
        self.db_file.write(&serialize(&self.metadata).unwrap()).unwrap();
    }

    pub fn get_root_page(&self) -> u64 {
        self.metadata.root_page
    }

    pub fn set_root_page(&mut self, new_root_page: u64) {
        self.metadata.root_page = new_root_page;
        self.db_file.seek(SeekFrom::Start(0)).unwrap();
        self.db_file.write(&serialize(&self.metadata).unwrap()).unwrap();
    }

    pub fn get_page(&mut self, index: u64) -> Node<T, U> {
        let offset = self.calculate_page_offset(index);
        self.db_file.seek(SeekFrom::Start(offset)).unwrap();
        let mut buffer: Vec<u8> = vec![0; self.get_node_size() as usize];
        self.db_file.read(buffer.as_mut_slice()).unwrap();
        deserialize(buffer.as_slice()).unwrap()
    }

    pub fn allocate_node(&mut self, new_node: Node<T, U>) -> u64 {
        match self.metadata.free_page {
            None => {
                self.metadata.pages += 1;
                let len = self.calculate_page_offset(self.metadata.pages);
                let node_size = self.get_node_size();
                self.db_file.set_len(len).unwrap();
                self.db_file.seek(SeekFrom::Start(len - node_size)).unwrap();
                self.db_file.write(&serialize(&new_node).unwrap()).unwrap();

                self.db_file.seek(SeekFrom::Start(0)).unwrap();
                self.db_file.write(&serialize(&self.metadata).unwrap()).unwrap();
                self.metadata.pages - 1
            },
            Some(free_page) => {
                let offset = self.calculate_page_offset(free_page);
                let mut buffer: Vec<u8> = vec![0; self.get_node_size() as usize];
                self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
                self.db_file.read(buffer.as_mut_slice()).unwrap();
                self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
                self.db_file.write(&serialize(&new_node).unwrap()).unwrap();

                match deserialize(buffer.as_slice()).unwrap() {
                    Node::Free::<T, U>(new_free_page) => self.metadata.free_page = new_free_page,
                    _ => unreachable!(),
                }
                self.db_file.seek(SeekFrom::Start(0)).unwrap();
                self.db_file.write(&serialize(&self.metadata).unwrap()).unwrap();

                free_page
            }
        }
    }

    pub fn deallocate_node(&mut self, index: u64) {
        let offset = self.calculate_page_offset(index);
        self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.db_file.write(&serialize(&Node::Free::<T, U>(self.metadata.free_page)).unwrap()).unwrap();
        self.metadata.free_page = Some(offset);
        self.db_file.seek(SeekFrom::Start(0)).unwrap();
        self.db_file.write(&serialize(&self.metadata).unwrap()).unwrap();
    }

    pub fn write_node(&mut self, index: u64, node: Node<T, U>) {
        let offset = self.calculate_page_offset(index);
        self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.db_file.write(&serialize(&node).unwrap()).unwrap();
    }

    pub fn clear(&mut self) {
        let header_size = Self::get_metadata_size();
        let body_size = Node::<T, U>::get_max_size(self.metadata.leaf_degree, self.metadata.internal_degree) as u64;
        self.metadata.pages = 1;
        self.metadata.len = 0;
        self.metadata.root_page = 0;
        self.metadata.free_page = None;
        self.db_file.set_len(header_size + body_size).unwrap();
        self.db_file.seek(SeekFrom::Start(0)).unwrap();
        self.db_file.write(&serialize(&self.metadata).unwrap()).unwrap();
        self.db_file.seek(SeekFrom::Start(header_size)).unwrap();
        self.db_file.write(&serialize(&Node::Leaf(LeafNode::<T, U>::new(self.metadata.leaf_degree))).unwrap()).unwrap();
    }
}
