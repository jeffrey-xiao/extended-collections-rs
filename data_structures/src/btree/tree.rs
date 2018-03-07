use btree::{INTERNAL_DEGREE, LEAF_DEGREE};
use btree::node::{InternalNode, Node};
use btree::pager::Pager;
use entry::Entry;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::VecDeque;
use std::io::{Error};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;

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
                        if key <= search_key {
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

        let mut split_node_entry = None;
        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                if let Some((split_key, split_node)) = curr_leaf_node.insert(Entry { key, value }) {
                    let split_node_index = self.pager.allocate_node(split_node);
                    curr_leaf_node.next_leaf = Some(split_node_index);
                    split_node_entry = Some((split_key, split_node_index));
                }
                self.pager.write_node(curr_page, Node::Leaf(curr_leaf_node));
            },
            _ => unreachable!(),
        }

        while let Some((split_key, split_pointer)) = split_node_entry {
            match stack.pop() {
                Some((parent_page, mut parent_node, _)) => {
                    match &mut parent_node {
                        &mut Node::Internal(ref mut node) => {
                            if let Some((split_key, split_node)) = node.insert(split_key, split_pointer, true) {
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

    pub fn remove(&mut self, key: &T) -> Option<Entry<T, U>> {
        let (curr_page, curr_node, mut stack) = self.search_node(key);
        let mut delete_entry = None;
        let ret;

        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                ret = curr_leaf_node.remove(key);
                if curr_leaf_node.len < (LEAF_DEGREE + 1) / 2 {
                    if let Some((parent_page, parent_node, curr_index)) = stack.pop() {
                        let mut parent_internal_node = {
                            match parent_node {
                                Node::Internal(node) => node,
                                _ => unreachable!(),
                            }
                        };
                        let sibling_index = {
                            if curr_index == 0 {
                                curr_index + 1
                            } else {
                                curr_index - 1
                            }
                        };
                        let sibling_page = parent_internal_node.pointers[sibling_index];
                        let mut sibling_leaf_node = {
                            match self.pager.get_page(sibling_page) {
                                Node::Leaf(node) => node,
                                _ => unreachable!(),
                            }
                        };

                        // merge
                        if sibling_leaf_node.len == (LEAF_DEGREE + 1) / 2 {
                            if sibling_index == curr_index + 1 {
                                curr_leaf_node.merge(&mut sibling_leaf_node);
                                delete_entry = Some((curr_index, parent_page, parent_internal_node));
                                self.pager.deallocate_node(sibling_page);
                                self.pager.write_node(curr_page, Node::Leaf(curr_leaf_node));
                            } else {
                                sibling_leaf_node.merge(&mut curr_leaf_node);
                                delete_entry = Some((sibling_index, parent_page, parent_internal_node));
                                self.pager.deallocate_node(curr_page);
                                self.pager.write_node(sibling_page, Node::Leaf(sibling_leaf_node));
                            }
                        }
                        // take one entry
                        else {
                            if sibling_index == curr_index + 1 {
                                let removed_entry = sibling_leaf_node.remove_at(0);
                                let new_key = {
                                    match sibling_leaf_node.entries[0] {
                                        Some(ref entry) => entry.key.clone(),
                                        _ => unreachable!(),
                                    }
                                };
                                parent_internal_node.keys[curr_index] = Some(new_key);
                                curr_leaf_node.insert(removed_entry);
                            } else {
                                let remove_index = sibling_leaf_node.len - 1;
                                let removed_entry = sibling_leaf_node.remove_at(remove_index);
                                parent_internal_node.keys[sibling_index] = Some(removed_entry.key.clone());
                                curr_leaf_node.insert(removed_entry);
                            }
                            self.pager.write_node(parent_page, Node::Internal(parent_internal_node));
                            self.pager.write_node(sibling_page, Node::Leaf(sibling_leaf_node));
                            self.pager.write_node(curr_page, Node::Leaf(curr_leaf_node));
                        }
                    }
                } else if ret.is_some() {
                    self.pager.write_node(curr_page, Node::Leaf(curr_leaf_node));
                }
            },
            _ => unreachable!(),
        }

        while let Some((delete_index, curr_page, mut curr_internal_node)) = delete_entry {
            delete_entry = None;
            curr_internal_node.remove_at(delete_index, true);

            if curr_internal_node.len + 1 < (INTERNAL_DEGREE + 1) / 2 {
                if let Some((parent_page, parent_node, curr_index)) = stack.pop() {
                    let mut parent_internal_node = {
                        match parent_node {
                            Node::Internal(node) => node,
                            _ => unreachable!(),
                        }
                    };
                    let sibling_index = {
                        if curr_index == 0 {
                            curr_index + 1
                        } else {
                            curr_index - 1
                        }
                    };
                    let sibling_page = parent_internal_node.pointers[sibling_index];
                    let mut sibling_internal_node = {
                        match self.pager.get_page(sibling_page) {
                            Node::Internal(node) => node,
                            _ => unreachable!(),
                        }
                    };

                    if sibling_internal_node.len + 1 == (INTERNAL_DEGREE + 1) / 2 {
                        if sibling_index == curr_index + 1 {
                            let parent_key = match parent_internal_node.keys[curr_index] {
                                Some(ref key) => key.clone(),
                                None => unreachable!(),
                            };
                            curr_internal_node.merge(parent_key, &mut sibling_internal_node);
                            delete_entry = Some((curr_index, parent_page, parent_internal_node));
                            self.pager.deallocate_node(sibling_page);
                            self.pager.write_node(curr_page, Node::Internal(curr_internal_node));
                        } else {
                            let parent_key = match parent_internal_node.keys[sibling_index] {
                                Some(ref key) => key.clone(),
                                None => unreachable!(),
                            };
                            sibling_internal_node.merge(parent_key, &mut curr_internal_node);
                            delete_entry = Some((sibling_index, parent_page, parent_internal_node));
                            self.pager.deallocate_node(curr_page);
                            self.pager.write_node(sibling_page, Node::Internal(sibling_internal_node));
                        }
                    } else {
                        if sibling_index == curr_index + 1 {
                            let (mut removed_key, removed_pointer) = sibling_internal_node.remove_at(0, false);
                            let removed_key = match mem::replace(&mut parent_internal_node.keys[curr_index], Some(removed_key)) {
                                Some(key) => key,
                                _ => unreachable!(),
                            };
                            curr_internal_node.insert(removed_key, removed_pointer, true);
                            self.pager.write_node(parent_page, Node::Internal(parent_internal_node));
                            self.pager.write_node(sibling_page, Node::Internal(sibling_internal_node));
                            self.pager.write_node(curr_page, Node::Internal(curr_internal_node));
                        } else {
                            let remove_index = sibling_internal_node.len - 1;
                            let (mut removed_key, removed_pointer) = sibling_internal_node.remove_at(remove_index, true);
                            let removed_key = match mem::replace(&mut parent_internal_node.keys[sibling_index], Some(removed_key)) {
                                Some(key) => key,
                                _ => unreachable!(),
                            };
                            curr_internal_node.insert(removed_key, removed_pointer, false);
                            self.pager.write_node(parent_page, Node::Internal(parent_internal_node));
                            self.pager.write_node(sibling_page, Node::Internal(sibling_internal_node));
                            self.pager.write_node(curr_page, Node::Internal(curr_internal_node));
                        }
                    }
                } else if curr_internal_node.len == 0 {
                    self.pager.set_root_page(curr_internal_node.pointers[0]);
                    self.pager.deallocate_node(curr_page);
                }
            } else {
                self.pager.write_node(curr_page, Node::Internal(curr_internal_node));
            }
        }

        ret
    }

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
