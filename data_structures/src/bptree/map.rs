use bptree::node::{BLOCK_SIZE, LeafNode, InternalNode, InsertCases, Node};
use bptree::pager::Pager;
use entry::Entry;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::VecDeque;
use std::io::Error;
use std::fmt::Debug;
use std::mem;

/// An ordered map implemented by an on-disk B+ tree.
///
/// A B+ is an N-ary tree with a variable number of children per node. A B+ tree is a B-tree in
/// which each internal node contains keys and pointers to other nodes, and each leaf node
/// contains keys and values.
///
/// # Examples
/// ```
/// # fn foo() -> std::io::Result<()> {
/// # use std::fs;
/// use data_structures::bptree::BPMap;
///
/// let mut map = BPMap::new("example.dat")?;
/// map.insert(0, 1);
/// map.insert(3, 4);
///
/// assert_eq!(map.get(&0), Some(1));
/// assert_eq!(map.get(&1), None);
/// assert_eq!(map.len(), 2);
///
/// assert_eq!(map.min(), Some(0));
///
/// assert_eq!(map.remove(&0), Some((0, 1)));
/// assert_eq!(map.remove(&1), None);
/// # fs::remove_file("example.dat")?;
/// # Ok(())
/// # }
/// # foo();
/// ```
pub struct BPMap<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> {
    pager: Pager<T, U>,
}

impl<T: Ord + Clone + Serialize + DeserializeOwned + Debug, U: Serialize + DeserializeOwned + Debug> BPMap<T, U> {
    /// Constructs a new, empty `BPMap<T, U>` and creates a file for data persistence.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let map: BPMap<u32, u32> = BPMap::new("example_new.dat")?;
    /// # fs::remove_file("example_new.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn new(file_path: &str) -> Result<BPMap<T, U>, Error> {
        let leaf_degree = LeafNode::<T, U>::get_degree();
        let internal_degree = InternalNode::<T, U>::get_degree();
        Ok(BPMap { pager: Pager::new(file_path, leaf_degree, internal_degree)? })
    }

    /// Constructs a new, empty `BPMap<T, U>` with specific sizes for leaf and internal nodes
    /// and creates a file for data persistence.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let map: BPMap<u32, u32> = BPMap::with_degrees("example_with_degrees.dat", 3, 3)?;
    /// # fs::remove_file("example_with_degrees.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn with_degrees(file_path: &str, leaf_degree: usize, internal_degree: usize) -> Result<BPMap<T, U>, Error> {
        assert!(LeafNode::<T, U>::get_max_size(leaf_degree) <= BLOCK_SIZE);
        assert!(InternalNode::<T, U>::get_max_size(internal_degree) <= BLOCK_SIZE);
        Ok(BPMap { pager: Pager::new(file_path, leaf_degree, internal_degree)? })
    }

    /// Opens an existing `BPMap<T, U>` from a file.
    ///
    /// # Examples
    /// ```no run
    /// # fn foo() -> std::io::Result<()> {
    /// use data_structures::bptree::BPMap;
    ///
    /// let map: BPMap<u32, u32> = BPMap::open("example_open.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn open(file_path: &str) -> Result<BPMap<T, U>, Error> {
        Ok(BPMap { pager: Pager::open(file_path)? })
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

    /// Inserts a key-value pair into the map. If the key already exists in the map, it will return
    /// and replace the old key-value pair.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_insert.dat")?;
    /// assert_eq!(map.insert(1, 1), None);
    /// assert_eq!(map.get(&1), Some(1));
    /// assert_eq!(map.insert(1, 2), Some((1, 1)));
    /// assert_eq!(map.get(&1), Some(2));
    /// # fs::remove_file("example_insert.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn insert(&mut self, key: T, value: U) -> Option<(T, U)> {
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
                        return Some((entry.key, entry.value));
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

    /// Removes a key-value pair from the map. If the key exists in the map, it will return the
    /// associated key-value pair. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_remove.dat")?;
    /// map.insert(1, 1);
    /// assert_eq!(map.remove(&1), Some((1, 1)));
    /// assert_eq!(map.remove(&1), None);
    /// # fs::remove_file("example_remove.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn remove(&mut self, key: &T) -> Option<(T, U)> {
        let (curr_page, curr_node, mut stack) = self.search_node(key);
        let mut delete_entry = None;
        let ret;

        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                ret = curr_leaf_node.remove(key);
                if curr_leaf_node.len < (self.pager.get_leaf_degree() + 1) / 2 && !stack.is_empty() {
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

    /// Checks if a key exists in the map.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_contains_key.dat")?;
    /// map.insert(1, 1);
    /// assert_eq!(map.contains_key(&0), false);
    /// assert_eq!(map.contains_key(&1), true);
    /// # fs::remove_file("example_contains_key.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn contains_key(&mut self, key: &T) -> bool {
        self.get(key).is_some()
    }

    /// Returns the value associated with a particular key. It will return `None` if the key does
    /// not exist in the map.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_get.dat")?;
    /// map.insert(1, 1);
    /// assert_eq!(map.get(&0), None);
    /// assert_eq!(map.get(&1), Some(1));
    /// # fs::remove_file("example_get.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
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

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_len.dat")?;
    /// map.insert(1, 1);
    /// assert_eq!(map.len(), 1);
    /// # fs::remove_file("example_len.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn len(&self) -> usize {
        self.pager.get_len()
    }

    /// Returns `true` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let map: BPMap<u32, u32> = BPMap::new("example_is_empty.dat")?;
    /// assert!(map.is_empty());
    /// # fs::remove_file("example_is_empty.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn is_empty(&self) -> bool {
        self.pager.get_len() == 0
    }

    /// Clears the map, removing all values.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_clear.dat")?;
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    /// map.clear();
    /// assert_eq!(map.is_empty(), true);
    /// # fs::remove_file("example_clear.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn clear(&mut self) {
        self.pager.clear();
    }

    /// Returns the minimum key of the map. Returns `None` if the bptree is empty.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_min.dat")?;
    /// map.insert(1, 1);
    /// map.insert(3, 3);
    /// assert_eq!(map.min(), Some(1));
    /// # fs::remove_file("example_min.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
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

    /// Returns the maximum key of the map. Returns `None` if the bptree is empty.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_max.dat")?;
    /// map.insert(1, 1);
    /// map.insert(3, 3);
    /// assert_eq!(map.max(), Some(3));
    /// # fs::remove_file("example_max.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
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

    /// Returns a mutable iterator over the map. The iterator will yield key-value pairs using
    /// in-order traversal.
    ///
    /// # Examples
    /// ```
    /// # fn foo() -> std::io::Result<()> {
    /// # use std::fs;
    /// use data_structures::bptree::BPMap;
    ///
    /// let mut map = BPMap::new("example_iter_mut.dat")?;
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    ///
    /// let mut iterator = map.iter_mut();
    /// assert_eq!(iterator.next(), Some((1, 1)));
    /// assert_eq!(iterator.next(), Some((2, 2)));
    /// assert_eq!(iterator.next(), None);
    /// # fs::remove_file("example_iter_mut.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn iter_mut(&mut self) -> BPMapIterMut<T, U> {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page);

        while let Node::Internal(curr_internal_node) = curr_node {
            curr_page = curr_internal_node.pointers[0];
            curr_node = self.pager.get_page(curr_page);
        }

        match curr_node {
            Node::Leaf(curr_leaf_node) => BPMapIterMut {
                pager: &mut self.pager,
                curr_node: curr_leaf_node,
                curr_index: 0,
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

impl<'a, T: 'a + Ord + Clone + Serialize + DeserializeOwned + Debug, U: 'a + Serialize + DeserializeOwned + Debug> IntoIterator for &'a mut BPMap<T, U> {
    type Item = (T, U);
    type IntoIter = BPMapIterMut<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// A mutable iterator for `BPMap<T, U>`
///
/// This iterator traverses the elements of the map in ascending order and yields owned entries.
pub struct BPMapIterMut<'a, T: 'a + Ord + Clone + Serialize + DeserializeOwned + Debug, U: 'a + Serialize + DeserializeOwned + Debug> {
    pager: &'a mut Pager<T, U>,
    curr_node: LeafNode<T, U>,
    curr_index: usize,
}

impl<'a, T: 'a + Ord + Clone + Serialize + DeserializeOwned + Debug, U: 'a + Serialize + DeserializeOwned + Debug> Iterator for BPMapIterMut<'a, T, U> {
    type Item = (T, U);

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_index >= self.curr_node.len {
            match self.curr_node.next_leaf {
                Some(next_page) => {
                    self.curr_node = {
                        match self.pager.get_page(next_page) {
                            Node::Leaf(leaf_node) => leaf_node,
                            _ => unreachable!(),
                        }
                    };
                    self.curr_index = 0;
                },
                None => return None,
            }
        }

        match self.curr_node.entries[self.curr_index].take() {
            Some(entry) => {
                self.curr_index += 1;
                Some((entry.key, entry.value))
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::panic;
    use super::BPMap;

    fn teardown(test_name: &str) {
        fs::remove_file(format!("{}.dat", test_name)).ok();
    }

    fn run_test<T: FnOnce() -> () + panic::UnwindSafe>(test: T, test_name: &str) {
        let result = panic::catch_unwind(|| {
            test()
        });

        teardown(test_name);

        assert!(result.is_ok());
    }

    #[test]
    fn test_len_empty() {
        let test_name = "test_len_empty";
        run_test(|| {
            let map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            assert_eq!(map.len(), 0);
        }, test_name);
    }

    #[test]
    fn test_is_empty() {
        let test_name = "test_is_empty";
        run_test(|| {
            let map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            assert!(map.is_empty());
        }, test_name);
    }

    #[test]
    fn test_min_max_empty() {
        let test_name = "test_min_max_empty";
        run_test(|| {
            let mut map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            assert_eq!(map.min(), None);
            assert_eq!(map.max(), None);
        }, test_name);
    }

    #[test]
    fn test_get() {
        let test_name = "test_get";
        run_test(|| {
            let mut map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            map.insert(1, 1);
            assert_eq!(map.get(&1), Some(1));
        }, test_name);
    }

    #[test]
    fn test_insert() {
        let test_name = "test_insert";
        run_test(|| {
            let mut map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            map.insert(1, 1);
            assert!(map.contains_key(&1));
            assert_eq!(map.get(&1), Some(1));
        }, test_name);
    }

    #[test]
    fn test_insert_replace() {
        let test_name = "test_insert_replace";
        run_test(|| {
            let mut map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            let ret_1 = map.insert(1, 1);
            let ret_2 = map.insert(1, 3);
            assert_eq!(map.get(&1), Some(3));
            assert_eq!(ret_1, None);
            assert_eq!(ret_2, Some((1, 1)));
        }, test_name);
    }

    #[test]
    fn test_remove() {
        let test_name = "test_remove";
        run_test(|| {
            let mut map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            map.insert(1, 1);
            let ret = map.remove(&1);
            assert!(!map.contains_key(&1));
            assert_eq!(ret, Some((1, 1)));
        }, test_name);
    }

    #[test]
    fn test_min_max() {
        let test_name = "test_min_max";
        run_test(|| {
            let mut map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            map.insert(1, 1);
            map.insert(3, 3);
            map.insert(5, 5);

            assert_eq!(map.min(), Some(1));
            assert_eq!(map.max(), Some(5));
        }, test_name);
    }

    #[test]
    fn test_iter_mut() {
        let test_name = "test_iter_mut";
        run_test(|| {
            let mut map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 3, 3).expect("Could not create B+ tree.");
            map.insert(1, 2);
            map.insert(5, 6);
            map.insert(3, 4);

            assert_eq!(
                map.iter_mut().collect::<Vec<(u32, u32)>>(),
                vec![(1, 2), (3, 4), (5, 6)],
            );
        }, test_name);
    }
}
