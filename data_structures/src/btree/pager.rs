use bincode::{serialize, deserialize};
use btree::node::Node;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem;

const U64_SIZE: u64 = mem::size_of::<u64>() as u64;

pub struct Pager<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> {
    db_file: File,
    pages: u64,
    root_page: u64,
    free_page: Option<u64>,
    _marker: PhantomData<(T, U)>,
}

impl<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> Pager<T, U> {
    pub fn new(db_file_path: &str) -> Result<Pager<T, U>, Error> {
        let db_size = U64_SIZE * 2 + Self::get_node_size() + Self::get_free_page_size();
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file_path)?;
        db_file.set_len(db_size as u64).unwrap();
        db_file.seek(SeekFrom::Start(0)).unwrap();
        db_file.write(&serialize(&1u64).unwrap()).unwrap();
        db_file.write(&serialize(&0u64).unwrap()).unwrap();
        db_file.write(&serialize(&None::<u64>).unwrap()).unwrap();
        db_file.seek(SeekFrom::Start(Self::calculate_page_offset(0))).unwrap();
        db_file.write(&serialize(&Node::<T, U>::new_leaf_node()).unwrap()).unwrap();

        Ok(Pager {
            db_file,
            pages: 1,
            root_page: 0,
            free_page: None,
            _marker: PhantomData,
        })
    }

    pub fn open(db_file_path: &str) -> Result<Pager<T, U>, Error> {
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file_path)?;
        db_file.seek(SeekFrom::Start(0)).unwrap();

        let mut buffer: Vec<u8> = vec![0; U64_SIZE as usize];
        db_file.read(buffer.as_mut_slice()).unwrap();
        let pages = deserialize(buffer.as_slice()).unwrap();
        db_file.read(buffer.as_mut_slice()).unwrap();
        let root_page = deserialize(buffer.as_slice()).unwrap();

        let mut buffer: Vec<u8> = vec![0; Self::get_free_page_size() as usize];
        db_file.read(buffer.as_mut_slice()).unwrap();
        let free_page = deserialize(buffer.as_slice()).unwrap();

        Ok(Pager {
            db_file,
            pages,
            root_page,
            free_page,
            _marker: PhantomData,
        })
    }

    #[inline]
    fn get_node_size() -> u64 {
        mem::size_of::<Node<T, U>>() as u64
    }

    #[inline]
    fn get_free_page_size() -> u64 {
        mem::size_of::<Option<u64>>() as u64
    }

    fn calculate_page_offset(index: u64) -> u64 {
        U64_SIZE * 2 + Self::get_free_page_size() + Self::get_node_size() * (index as u64)
    }

    pub fn get_root_page(&mut self) -> u64 {
        self.root_page
    }

    pub fn set_root_page(&mut self, new_root_page: u64) {
        self.root_page = new_root_page;
        self.db_file.seek(SeekFrom::Start(U64_SIZE)).unwrap();
        self.db_file.write(&serialize(&self.root_page).unwrap()).unwrap();
    }

    pub fn get_page(&mut self, index: u64) -> Node<T, U> {
        let offset = Self::calculate_page_offset(index);
        self.db_file.seek(SeekFrom::Start(offset)).unwrap();
        let mut buffer: Vec<u8> = vec![0; mem::size_of::<Node<T, U>>()];
        self.db_file.read(buffer.as_mut_slice()).unwrap();
        deserialize(buffer.as_slice()).unwrap()
    }

    pub fn allocate_node(&mut self, new_node: Node<T, U>) -> u64 {
        match self.free_page {
            None => {
                let len = Self::calculate_page_offset(self.pages + 1);
                self.db_file.set_len(len).unwrap();
                self.db_file.seek(SeekFrom::Start(len - Self::get_node_size())).unwrap();
                self.db_file.write(&serialize(&new_node).unwrap()).unwrap();

                self.pages += 1;
                self.db_file.seek(SeekFrom::Start(0)).unwrap();
                self.db_file.write(&serialize(&self.pages).unwrap()).unwrap();
                self.pages - 1
            },
            Some(free_page) => {
                let offset = Self::calculate_page_offset(free_page);
                let mut buffer: Vec<u8> = vec![0; mem::size_of::<Node<T, U>>()];
                self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
                self.db_file.read(buffer.as_mut_slice()).unwrap();
                self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
                self.db_file.write(&serialize(&new_node).unwrap()).unwrap();

                match deserialize(buffer.as_slice()).unwrap() {
                    Node::Free::<T, U>(new_free_page) => self.free_page = new_free_page,
                    _ => unreachable!(),
                }
                free_page
            }
        }
    }

    pub fn deallocate_node(&mut self, index: u64) {
        let offset = Self::calculate_page_offset(index);
        self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.db_file.write(&serialize(&Node::Free::<T, U>(self.free_page)).unwrap()).unwrap();
        self.free_page = Some(offset);
        self.db_file.seek(SeekFrom::Start((U64_SIZE * 2) as u64)).unwrap();
        self.db_file.write(&serialize(&self.free_page).unwrap()).unwrap();
    }

    pub fn write_node(&mut self, index: u64, node: Node<T, U>) {
        let offset = Self::calculate_page_offset(index);
        self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.db_file.write(&serialize(&node).unwrap()).unwrap();
    }
}
