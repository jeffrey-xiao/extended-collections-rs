use bincode::{serialize, deserialize};
use entry::Entry;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem;
use std::fmt::Debug;

const INTERNAL_DEGREE: usize = 3;
const LEAF_DEGREE: usize = 3;

macro_rules! init_array(
    ($ty:ty, $len:expr, $val:expr) => (
        {
            let mut array: [$ty; $len] = unsafe { mem::uninitialized() };
            for i in array.iter_mut() {
                unsafe { ::std::ptr::write(i, $val); }
            }
            array
        }
    )
);

#[derive(Serialize, Deserialize, Debug)]
pub enum InternalEntry<T: Ord + Clone> {
    Vacant,
    Occupied(T),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum LeafEntry<T: Ord + Clone, U> {
    Vacant,
    Occupied(Entry<T, U>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Node<T: Ord + Clone, U> {
    Internal {
        is_root: bool,
        keys: [InternalEntry<T>; INTERNAL_DEGREE],
        pointers: [usize; INTERNAL_DEGREE + 1],
    },
    Leaf {
        entries: [LeafEntry<T, U>; LEAF_DEGREE],
        next_leaf: usize,
    }
}

impl<T: Ord + Clone, U> Node<T, U> {
    fn new_leaf_node() -> Self {
        Node::Leaf {
            entries: init_array!(LeafEntry<T, U>, LEAF_DEGREE, LeafEntry::Vacant),
            next_leaf: 0,
        }
    }

    fn new_internal_node(is_root: bool) -> Self {
        Node::Internal {
            is_root,
            keys: init_array!(InternalEntry<T>, INTERNAL_DEGREE, InternalEntry::Vacant),
            pointers: init_array!(usize, INTERNAL_DEGREE + 1, 0),
        }
    }
}

pub struct Tree<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U : Serialize + DeserializeOwned + Debug> {
    db_file: File,
    pages: usize,
    pub root_page: usize,
    _marker: PhantomData<(T, U)>,
}

impl<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> Tree<T, U> {
    pub fn new(db_file_path: &str) -> Result<Tree<T, U>, Error> {
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file_path)?;
        let db_size = mem::size_of::<Node<T, U>>() + mem::size_of::<usize>();
        db_file.set_len(db_size as u64).unwrap();
        db_file.seek(SeekFrom::Start(0)).unwrap();
        db_file.write(&serialize(&0usize).unwrap()).unwrap();
        db_file.write(&serialize(&1usize).unwrap()).unwrap();
        db_file.write(&serialize(&Node::<T, U>::new_leaf_node()).unwrap()).unwrap();

        Ok(Tree {
            db_file,
            pages: 1,
            root_page: 0,
            _marker: PhantomData,
        })
    }

    pub fn open(db_file_path: &str) -> Result<Tree<T, U>, Error> {
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file_path)?;
        db_file.seek(SeekFrom::Start(0)).unwrap();
        let usize_size = mem::size_of::<usize>();
        let mut buffer: Vec<u8> = vec![0; usize_size];
        db_file.read(buffer.as_mut_slice()).unwrap();
        let root_page = deserialize(buffer.as_slice()).unwrap();
        db_file.read(buffer.as_mut_slice()).unwrap();
        let pages = deserialize(buffer.as_slice()).unwrap();
        Ok(Tree {
            db_file,
            pages,
            root_page,
            _marker: PhantomData,
        })
    }

    fn set_root_page(&mut self, new_root_page: usize) {
        println!("writing {:?}", new_root_page);
        self.root_page = new_root_page;
        self.db_file.seek(SeekFrom::Start(0)).unwrap();
        self.db_file.write(&serialize(&self.root_page).unwrap()).unwrap();
    }

    pub fn allocate_node(&mut self, new_node: Node<T, U>) -> usize {
        let usize_size = mem::size_of::<usize>();
        let node_size = mem::size_of::<Node<T, U>>();
        let len = usize_size * 2 + node_size * (self.pages + 1);
        self.db_file.set_len(len as u64).unwrap();
        self.db_file.seek(SeekFrom::Start((len - node_size) as u64)).unwrap();
        self.db_file.write(&serialize(&new_node).unwrap()).unwrap();

        self.pages += 1;
        self.db_file.seek(SeekFrom::Start(usize_size as u64)).unwrap();
        self.db_file.write(&serialize(&self.pages).unwrap()).unwrap();
        self.pages - 1
    }

    pub fn write_node(&mut self, index: usize, node: Node<T, U>) {
        let usize_size = mem::size_of::<usize>();
        let node_size = mem::size_of::<Node<T, U>>();
        let offset = usize_size * 2 + node_size * index;
        self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.db_file.write(&serialize(&node).unwrap()).unwrap();
    }

    pub fn get_page(&mut self, index: usize) -> Node<T, U> {
        let usize_size = mem::size_of::<usize>();
        let node_size = mem::size_of::<Node<T, U>>();
        let offset = usize_size * 2 + node_size * index;
        self.db_file.seek(SeekFrom::Start(offset as u64)).unwrap();
        let mut buffer: Vec<u8> = vec![0; node_size];
        self.db_file.read(buffer.as_mut_slice()).unwrap();
        deserialize(buffer.as_slice()).unwrap()
    }

    pub fn insert(&mut self, key: T, value: U) {
        let mut new_entry = Entry { key, value };
        let mut curr_page = self.root_page;
        let mut curr_node = self.get_page(curr_page);
        let mut pages = Vec::new();

        while let Node::Internal { keys, pointers, .. } = curr_node {
            pages.push(curr_page);
            let mut lo = 0;
            let mut hi = (INTERNAL_DEGREE - 1) as isize;
            while lo <= hi {
                let mid = lo + ((hi - lo) >> 1);
                match keys[mid as usize] {
                    InternalEntry::Vacant => hi = mid - 1,
                    InternalEntry::Occupied(ref key) => {
                        if *key < new_entry.key {
                            lo = mid + 1;
                        } else {
                            hi = mid - 1;
                        }
                    }
                }
            }
            curr_page = pointers[lo as usize];
            curr_node = self.get_page(curr_page);
        }

        let mut split_node_entry = None;
        match &mut curr_node {
            &mut Node::Leaf { ref mut entries, ref mut next_leaf } => {
                // node has room; can insert
                if let LeafEntry::Vacant = entries[LEAF_DEGREE - 1] {
                    let mut index = 0;
                    while let LeafEntry::Occupied(ref mut entry) = entries[index] {
                        if new_entry < *entry {
                            mem::swap(entry, &mut new_entry);
                        }
                        index += 1;
                    }
                    entries[index] = LeafEntry::Occupied(new_entry);
                }
                // node is full; have to split
                else {
                    let mut new_node_entries = init_array!(LeafEntry<T, U>, LEAF_DEGREE, LeafEntry::Vacant);
                    let mut index = 0;
                    while index < LEAF_DEGREE {
                        if let LeafEntry::Occupied(ref mut entry) = entries[index] {
                            if new_entry < *entry {
                                mem::swap(entry, &mut new_entry);
                            }
                        }
                        if index > LEAF_DEGREE / 2 {
                            mem::swap(&mut entries[index], &mut new_node_entries[index - LEAF_DEGREE / 2 - 1]);
                        }
                        index += 1;
                    }
                    new_node_entries[LEAF_DEGREE / 2] = LeafEntry::Occupied(new_entry);
                    let split_key = match new_node_entries[0] {
                        LeafEntry::Occupied(ref mut entry) => entry.key.clone(),
                        _ => unreachable!(),
                    };
                    let new_node = Node::Leaf {
                        entries: new_node_entries,
                        next_leaf: *next_leaf,
                    };
                    let new_node_index = self.allocate_node(new_node);
                    *next_leaf = new_node_index;
                    split_node_entry = Some((split_key, new_node_index));
                }
            },
            _ => unreachable!(),
        }

        self.write_node(curr_page, curr_node);

        while let Some((mut split_key, mut split_pointer)) = split_node_entry {
            match pages.pop() {
                Some(curr_page) => {
                    curr_node = self.get_page(curr_page);
                    match &mut curr_node {
                        &mut Node::Internal { ref mut keys, ref mut pointers, ref mut is_root } => {
                            // node has room; can insert
                            if let InternalEntry::Vacant = keys[INTERNAL_DEGREE - 1] {
                                let mut index = 0;
                                while let InternalEntry::Occupied(ref mut key) = keys[index] {
                                    if split_key < *key {
                                        mem::swap(&mut split_key, key);
                                        mem::swap(&mut split_pointer, &mut pointers[index + 1]);
                                    }
                                    index += 1;
                                }
                                keys[index] = InternalEntry::Occupied(split_key);
                                pointers[index + 1] = split_pointer;
                                split_node_entry = None;
                            }
                            // node is full; have to split
                            else {
                                let mut new_node_keys = init_array!(InternalEntry<T>, INTERNAL_DEGREE, InternalEntry::Vacant);
                                let mut new_node_pointers = init_array!(usize, INTERNAL_DEGREE + 1, 0);
                                let mut index = 0;
                                while index < INTERNAL_DEGREE {
                                    if let InternalEntry::Occupied(ref mut key) = keys[index] {
                                        if split_key < *key {
                                            mem::swap(&mut split_key, key);
                                            mem::swap(&mut split_pointer, &mut pointers[index + 1]);
                                        }
                                    }
                                    if index > (INTERNAL_DEGREE + 1) / 2 {
                                        mem::swap(&mut keys[index], &mut new_node_keys[index - (INTERNAL_DEGREE + 1) / 2- 1]);
                                        mem::swap(&mut pointers[index + 1], &mut new_node_pointers[index - (INTERNAL_DEGREE + 1) / 2]);
                                    }
                                    index += 1;
                                }
                                new_node_keys[(INTERNAL_DEGREE - 2) / 2] = InternalEntry::Occupied(split_key);
                                new_node_pointers[(INTERNAL_DEGREE - 2) / 2 + 1] = split_pointer;
                                let split_key = match mem::replace(&mut keys[(INTERNAL_DEGREE + 1) / 2], InternalEntry::Vacant) {
                                    InternalEntry::Occupied(key) => key,
                                    _ => unreachable!(),
                                };
                                mem::swap(&mut pointers[(INTERNAL_DEGREE + 1) / 2 + 1], &mut new_node_pointers[0]);
                                let new_node = Node::Internal {
                                    is_root: false,
                                    keys: new_node_keys,
                                    pointers: new_node_pointers,
                                };

                                split_pointer = self.allocate_node(new_node);

                                // create new root if root is too large
                                if *is_root {
                                    *is_root = false;
                                    let mut new_root_keys = init_array!(InternalEntry<T>, INTERNAL_DEGREE, InternalEntry::Vacant);
                                    let mut new_root_pointers = init_array!(usize, INTERNAL_DEGREE + 1, 0);
                                    new_root_keys[0] = InternalEntry::Occupied(split_key);
                                    new_root_pointers[0] = curr_page;
                                    new_root_pointers[1] = split_pointer;
                                    let new_root = Node::Internal {
                                        is_root: true,
                                        keys: new_root_keys,
                                        pointers: new_root_pointers,
                                    };
                                    let new_root_page = self.allocate_node(new_root);
                                    self.set_root_page(new_root_page);
                                    split_node_entry = None;
                                } else {
                                    split_node_entry = Some((split_key, split_pointer));
                                }
                            }
                        },
                        _ => unreachable!(),
                    }
                    self.write_node(curr_page, curr_node);
                },
                None => {
                    let mut new_root_keys = init_array!(InternalEntry<T>, INTERNAL_DEGREE, InternalEntry::Vacant);
                    let mut new_root_pointers = init_array!(usize, INTERNAL_DEGREE + 1, 0);
                    new_root_keys[0] = InternalEntry::Occupied(split_key);
                    new_root_pointers[0] = curr_page;
                    new_root_pointers[1] = split_pointer;
                    let new_root = Node::Internal {
                        is_root: true,
                        keys: new_root_keys,
                        pointers: new_root_pointers,
                    };
                    self.root_page = self.allocate_node(new_root);
                    split_node_entry = None;
                },
            }
        }
    }

    pub fn print(&mut self, curr_page: usize) {
        let curr_node = self.get_page(curr_page);
        println!("{:?}, {:?}", curr_node, curr_page);
        if let Node::Internal { keys, pointers, .. } = curr_node {
            let mut index = 0;
            while let InternalEntry::Occupied(_) = keys[index] {
                self.print(pointers[index]);
                index += 1;
                if index == INTERNAL_DEGREE {
                    break;
                }
            }
            self.print(pointers[index]);
        }
    }
}
