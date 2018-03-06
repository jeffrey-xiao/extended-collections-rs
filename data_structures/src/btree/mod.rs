pub mod pager;

use entry::Entry;
use self::pager::Pager;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::VecDeque;
use std::io::{Error};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;

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
pub struct InternalNode<T: Ord + Clone + Debug, U: Debug> {
    len: usize,
    keys: [Option<T>; INTERNAL_DEGREE],
    pointers: [u64; INTERNAL_DEGREE + 1],
    _marker: PhantomData<U>,
}

impl<T: Ord + Clone + Debug, U: Debug> InternalNode<T, U> {
    // the inserted key should never be the first key of the internal node
    fn insert(&mut self, mut new_key: T, mut new_pointer: u64) -> Option<(T, Node<T, U>)> {
        // node has room; can insert
        if self.len < INTERNAL_DEGREE {
            let mut index = 0;
            while let Some(ref mut key) = self.keys[index] {
                if new_key < *key {
                    mem::swap(&mut new_key, key);
                    mem::swap(&mut new_pointer, &mut self.pointers[index + 1]);
                }
                index += 1;
            }
            self.keys[index] = Some(new_key);
            self.pointers[index + 1] = new_pointer;
            self.len += 1;
            None
        }
        // node is full; have to split
        else {
            let mut split_node_keys = init_array!(Option<T>, INTERNAL_DEGREE, None);
            let mut split_node_pointers = init_array!(u64, INTERNAL_DEGREE + 1, 0);
            let mut index = 0;
            while index < INTERNAL_DEGREE {
                if let Some(ref mut key) = self.keys[index] {
                    if new_key < *key {
                        mem::swap(&mut new_key, key);
                        mem::swap(&mut new_pointer, &mut self.pointers[index + 1]);
                    }
                }
                if index > (INTERNAL_DEGREE + 1) / 2 {
                    mem::swap(&mut self.keys[index], &mut split_node_keys[index - (INTERNAL_DEGREE + 1) / 2 - 1]);
                    mem::swap(&mut self.pointers[index + 1], &mut split_node_pointers[index - (INTERNAL_DEGREE + 1) / 2]);
                }
                index += 1;
            }
            split_node_keys[(INTERNAL_DEGREE - 2) / 2] = Some(new_key);
            split_node_pointers[(INTERNAL_DEGREE - 2) / 2 + 1] = new_pointer;
            let split_key = match mem::replace(&mut self.keys[(INTERNAL_DEGREE + 1) / 2], None) {
                Some(key) => key,
                _ => unreachable!(),
            };
            mem::swap(&mut self.pointers[(INTERNAL_DEGREE + 1) / 2 + 1], &mut split_node_pointers[0]);
            let split_node = Node::Internal(InternalNode {
                len: INTERNAL_DEGREE / 2,
                keys: split_node_keys,
                pointers: split_node_pointers,
                _marker: PhantomData,
            });
            self.len = (INTERNAL_DEGREE + 1) / 2;

            Some((split_key, split_node))
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LeafNode<T: Ord + Clone + Debug, U: Debug> {
    len: usize,
    entries: [Option<Entry<T, U>>; LEAF_DEGREE],
    next_leaf: u64,
}

impl<T: Ord + Clone + Debug, U: Debug> LeafNode<T, U> {
    fn insert(&mut self, mut new_entry: Entry<T, U>) -> Option<(T, Node<T, U>)> {
        // node has room; can insert
        if self.len < LEAF_DEGREE {
            let mut index = 0;
            while let Some(ref mut entry) = self.entries[index] {
                if new_entry < *entry {
                    mem::swap(entry, &mut new_entry);
                }
                index += 1;
            }
            self.len += 1;
            self.entries[index] = Some(new_entry);
            None
        }
        // node is full; have to split
        else {
            let mut split_node_entries = init_array!(Option<Entry<T, U>>, LEAF_DEGREE, None);
            let mut index = 0;
            while index < LEAF_DEGREE {
                if let Some(ref mut entry) = self.entries[index] {
                    if new_entry < *entry {
                        mem::swap(entry, &mut new_entry);
                    }
                }
                if index > LEAF_DEGREE / 2 {
                    mem::swap(&mut self.entries[index], &mut split_node_entries[index - LEAF_DEGREE / 2 - 1]);
                }
                index += 1;
            }
            split_node_entries[(LEAF_DEGREE - 1) / 2] = Some(new_entry);
            let split_key = match split_node_entries[0] {
                Some(ref mut entry) => entry.key.clone(),
                _ => unreachable!(),
            };
            let split_node = Node::Leaf(LeafNode {
                len: (self.len + 1) / 2,
                entries: split_node_entries,
                next_leaf: self.next_leaf,
            });
            self.len = (self.len + 2) / 2;
            Some((split_key, split_node))
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Node<T: Ord + Clone + Debug, U: Debug> {
    Internal(InternalNode<T, U>),
    Leaf(LeafNode<T, U>),
    Free(Option<u64>),
}

impl<T: Ord + Clone + Debug, U: Debug> Node<T, U> {
    fn new_leaf_node() -> Self {
        Node::Leaf(LeafNode {
            len: 0,
            entries: init_array!(Option<Entry<T, U>>, LEAF_DEGREE, None),
            next_leaf: 0,
        })
    }

    fn new_internal_node() -> Self {
        Node::Internal(InternalNode {
            len: 0,
            keys: init_array!(Option<T>, INTERNAL_DEGREE, None),
            pointers: init_array!(u64, INTERNAL_DEGREE + 1, 0),
            _marker: PhantomData,
        })
    }
}

pub struct Tree<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> {
    pager: Pager<T, U>,
}

impl<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> Tree<T, U> {
    pub fn new(db_file_path: &str) -> Result<Tree<T, U>, Error> {
        Ok(Tree { pager: Pager::new(db_file_path)? })
    }

    pub fn open(db_file_path: &str) -> Result<Tree<T, U>, Error> {
        Ok(Tree { pager: Pager::open(db_file_path)? })
    }

    fn search_node(&mut self, search_key: &T) -> (u64, Node<T, U>, Vec<(u64, Node<T, U>, usize)>) {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page);

        let mut stack = Vec::new();

        while let Node::Internal(node) = curr_node {
            let mut lo = 0;
            let mut hi = (INTERNAL_DEGREE - 1) as isize;
            while lo <= hi {
                let mid = lo + ((hi - lo) >> 1);
                match node.keys[mid as usize] {
                    None => hi = mid - 1,
                    Some(ref key) => {
                        if key < search_key {
                            lo = mid + 1;
                        } else {
                            hi = mid - 1;
                        }
                    }
                }
            }
            let next_page = node.pointers[lo as usize];
            stack.push((curr_page, Node::Internal(node), lo as usize));
            curr_page = next_page;
            curr_node = self.pager.get_page(curr_page);
        }
        (curr_page, curr_node, stack)
    }

    pub fn insert(&mut self, key: T, value: U) {
        let (mut curr_page, mut curr_node, mut stack) = self.search_node(&key);
        let new_entry = Entry { key, value };

        let mut split_node_entry = None;
        match &mut curr_node {
            &mut Node::Leaf(ref mut node) => {
                if let Some((split_key, split_node)) = node.insert(new_entry) {
                    let split_node_index = self.pager.allocate_node(split_node);
                    node.next_leaf = split_node_index;
                    split_node_entry = Some((split_key, split_node_index));
                }
            },
            _ => unreachable!(),
        }

        self.pager.write_node(curr_page, curr_node);

        while let Some((split_key, split_pointer)) = split_node_entry {
            match stack.pop() {
                Some((parent_page, mut parent_node, _)) => {
                    match &mut parent_node {
                        &mut Node::Internal(ref mut node) => {
                            if let Some((split_key, split_node)) = node.insert(split_key, split_pointer) {
                                let split_node_index = self.pager.allocate_node(split_node);
                                split_node_entry = Some((split_key, split_node_index));
                            } else {
                                split_node_entry = None
                            }
                        },
                        _ => unreachable!(),
                    }
                    curr_node = parent_node;
                    curr_page = parent_page;
                    self.pager.write_node(curr_page, curr_node);
                },
                None => {
                    let mut new_root_keys = init_array!(Option<T>, INTERNAL_DEGREE, None);
                    let mut new_root_pointers = init_array!(u64, INTERNAL_DEGREE + 1, 0);
                    new_root_keys[0] = Some(split_key);
                    new_root_pointers[0] = curr_page;
                    new_root_pointers[1] = split_pointer;
                    let new_root = Node::Internal(InternalNode {
                        len: 1,
                        keys: new_root_keys,
                        pointers: new_root_pointers,
                        _marker: PhantomData,
                    });
                    let new_root_page = self.pager.allocate_node(new_root);
                    self.pager.set_root_page(new_root_page);
                    split_node_entry = None;
                },
            }
        }
    }

    // pub fn remove(&mut self, key: &T) {
    //     let (mut curr_page, mut curr_node, mut stack) = self.search_node(&key);
    //     let mut deleted_entry = None;

    //     match &mut curr_node {
    //         &mut Node::Leaf { ref mut len, ref mut entries, ref mut next_leaf } => {
    //             let mut index = 0;
    //             while let Some(entry) = entries[index].take() {
    //                 if entry.key == *key {
    //                     deleted_entry = Some(entry);
    //                 } else if deleted_entry.is_some() {
    //                     entries.swap(index - 1, index);
    //                 }
    //                 index += 1;
    //                 if index == LEAF_DEGREE {
    //                     break;
    //                 }
    //             }
    //             if index < LEAF_DEGREE / 2 {
    //                 // have to borrow or merge
    //                 if let Some((parent_page, parent_node, parent_index)) = stack.pop() {
    //                     let sibling_index = {
    //                         if parent_index > 0 {
    //                             parent_index - 1
    //                         } else {
    //                             parent_index + 1
    //                         }
    //                     };
    //                     match parent_node {
    //                         Node::Internal { len: mut parent_len, mut keys, mut pointers } => {
    //                             let sibling_page = pointers[sibling_index];
    //                             let sibling_node = self.pager.get_page(sibling_page);
    //                             match sibling_node {
    //                                 Node::Leaf { len: mut sibling_len, entries: mut sibling_entries, next_leaf: mut sibling_next_leaf } => {
    //                                     // merge nodes
    //                                     if sibling_len == LEAF_DEGREE / 2 {
    //                                     } else {
    //                                         sibling_len -= 1;
    //                                         *len += 1;
    //                                         let mut new_entry = &mut sibling_entries[sibling_len];
    //                                         let index = 0;
    //                                         while index < *len {
    //                                             mem::swap(&mut entries[index], new_entry);
    //                                         }

    //                                         if sibling_index == parent_index - 1 {
    //                                             let new_parent_key = match entries[0] {
    //                                                 Some(ref entry) => entry.key.clone(),
    //                                                 None => unreachable!(),
    //                                             };
    //                                         } else {
    //                                         }
    //                                     }
    //                                 },
    //                                 _ => unreachable!(),
    //                             }
    //                         },
    //                         _ => unreachable!(),
    //                     }
    //                 }
    //             }
    //         },
    //         _ => unreachable!(),
    //     }
    // }

    pub fn print(&mut self) {
        let curr_page = self.pager.get_root_page();
        let mut queue = VecDeque::new();
        queue.push_back(curr_page);
        while let Some(curr_page) = queue.pop_front() {
            let curr_node = self.pager.get_page(curr_page);
            println!("{:?} {:?}", curr_node, curr_page);
            if let Node::Internal(InternalNode { keys, pointers, .. }) = curr_node {
                let mut index = 0;
                while let Some(_) = keys[index] {
                    queue.push_back(pointers[index]);
                    index += 1;
                    if index == INTERNAL_DEGREE {
                        break;
                    }
                }
                queue.push_back(pointers[index]);
            }
        }
    }
}
