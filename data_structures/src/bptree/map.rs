use bptree::node::{BLOCK_SIZE, LeafNode, InternalNode, InsertCases, Node};
use bptree::pager::Pager;
use entry::Entry;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::VecDeque;
use std::io::{Error};
use std::fmt::Debug;
use std::mem;

pub struct BPMap<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> {
    pager: Pager<T, U>,
}

impl<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> BPMap<T, U> {
    pub fn new(db_file_path: &str) -> Result<BPMap<T, U>, Error> {
        let leaf_degree = LeafNode::<T, U>::get_degree();
        let internal_degree = InternalNode::<T, U>::get_degree();
        Ok(BPMap { pager: Pager::new(db_file_path, leaf_degree, internal_degree)? })
    }

    pub fn with_degrees(db_file_path: &str, leaf_degree: usize, internal_degree: usize) -> Result<BPMap<T, U>, Error> {
        assert!(LeafNode::<T, U>::get_max_size(leaf_degree) <= BLOCK_SIZE);
        assert!(InternalNode::<T, U>::get_max_size(internal_degree) <= BLOCK_SIZE);
        Ok(BPMap { pager: Pager::new(db_file_path, leaf_degree, internal_degree)? })
    }

    pub fn open(db_file_path: &str) -> Result<BPMap<T, U>, Error> {
        Ok(BPMap { pager: Pager::open(db_file_path)? })
    }

    fn search_node(&mut self, key: &T) -> (u64, Node<T, U>, Vec<(u64, Node<T, U>, usize)>) {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page);

        let mut stack = Vec::new();

        while let Node::Internal(node) = curr_node {
            let next_index = node.search(key);
            let next_page = node.pointers[next_index];
            stack.push((curr_page, Node::Internal(node), next_index));
            curr_page = next_page;
            curr_node = self.pager.get_page(curr_page);
        }
        (curr_page, curr_node, stack)
    }

    pub fn insert(&mut self, key: T, value: U) -> Option<U> {
        let (mut curr_page, mut curr_node, mut stack) = self.search_node(&key);

        let mut split_node_entry = None;
        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                match curr_leaf_node.insert(Entry { key, value }) {
                    Some(InsertCases::Split { split_key, split_node }) => {
                        let split_node_index = self.pager.allocate_node(split_node);
                        curr_leaf_node.next_leaf = Some(split_node_index);
                        split_node_entry = Some((split_key, split_node_index));
                        self.pager.write_node(curr_page, Node::Leaf(curr_leaf_node));
                    },
                    Some(InsertCases::Entry(entry)) => {
                        self.pager.write_node(curr_page, Node::Leaf(curr_leaf_node));
                        return Some(entry.value);
                    },
                    None => self.pager.write_node(curr_page, Node::Leaf(curr_leaf_node)),
                }
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
                    let mut new_root = InternalNode::new(self.pager.get_internal_degree());
                    new_root.keys[0] = Some(split_key);
                    new_root.pointers[0] = curr_page;
                    new_root.pointers[1] = split_pointer;
                    new_root.len = 1;
                    let new_root_page = self.pager.allocate_node(Node::Internal(new_root));
                    self.pager.set_root_page(new_root_page);
                    split_node_entry = None;
                },
            }
        }
        let new_len = self.pager.get_len() + 1;
        self.pager.set_len(new_len);
        None
    }

    pub fn remove(&mut self, key: &T) -> Option<(T, U)> {
        let (curr_page, curr_node, mut stack) = self.search_node(key);
        let mut delete_entry = None;
        let ret;

        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                ret = curr_leaf_node.remove(key);
                if curr_leaf_node.len < (self.pager.get_leaf_degree() + 1) / 2 {
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
                        if sibling_leaf_node.len == (self.pager.get_leaf_degree() + 1) / 2 {
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
                    let new_len = self.pager.get_len() - 1;
                    self.pager.set_len(new_len);
                } else if ret.is_some() {
                    let new_len = self.pager.get_len() - 1;
                    self.pager.set_len(new_len);
                    self.pager.write_node(curr_page, Node::Leaf(curr_leaf_node));
                }
            },
            _ => unreachable!(),
        }

        while let Some((delete_index, curr_page, mut curr_internal_node)) = delete_entry {
            delete_entry = None;
            curr_internal_node.remove_at(delete_index, true);

            if curr_internal_node.len + 1 < (self.pager.get_internal_degree() + 1) / 2 {
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

                    if sibling_internal_node.len + 1 == (self.pager.get_internal_degree() + 1) / 2 {
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
                } else {
                    self.pager.write_node(curr_page, Node::Internal(curr_internal_node));
                }
            } else {
                self.pager.write_node(curr_page, Node::Internal(curr_internal_node));
            }
        }
        ret.map(|entry| (entry.key, entry.value))
    }

    pub fn contains_key(&mut self, key: &T) -> bool {
        self.get(key).is_some()
    }

    pub fn get(&mut self, key: &T) -> Option<U> {
        let (_, curr_node, _) = self.search_node(key);
        match curr_node {
            Node::Leaf(mut curr_leaf_node) =>{
                curr_leaf_node.search(key).and_then(|index| {
                    match mem::replace(&mut curr_leaf_node.entries[index], None) {
                        Some(entry) => Some(entry.value),
                        _ => unreachable!(),
                    }
                })
            }
            _ => unreachable!(),
        }
    }

    pub fn len(&self) -> usize {
        self.pager.get_len()
    }

    pub fn is_empty(&self) -> bool {
        self.pager.get_len() == 0
    }

    pub fn clear(&mut self) {
        self.pager.clear();
    }

    pub fn min(&mut self) -> Option<T> {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page);

        while let Node::Internal(curr_internal_node) = curr_node {
            curr_page = curr_internal_node.pointers[0];
            curr_node = self.pager.get_page(curr_page);
        }

        match curr_node {
            Node::Leaf(mut curr_leaf_node) => mem::replace(&mut curr_leaf_node.entries[0], None).map(|entry| entry.key),
            _ => unreachable!(),
        }
    }

    pub fn max(&mut self) -> Option<T> {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page);

        while let Node::Internal(curr_internal_node) = curr_node {
            curr_page = curr_internal_node.pointers[curr_internal_node.len];
            curr_node = self.pager.get_page(curr_page);
        }

        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                if curr_leaf_node.len == 0 {
                    None
                } else {
                    let index = curr_leaf_node.len - 1;
                    mem::replace(&mut curr_leaf_node.entries[index], None).map(|entry| entry.key)
                }
            },
            _ => unreachable!(),
        }
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
                    if index == self.pager.get_internal_degree() {
                        break;
                    }
                }
                queue.push_back(pointers[index]);
            }
        }
    }
}

#[cfg(test)]
mod tests {

}
