use bptree::node::{InsertCases, InternalNode, LeafNode, Node, BLOCK_SIZE};
use bptree::pager::{Pager, Result};
use entry::Entry;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::mem;

// (page, node, index)
type SearchHistory<T, U> = Vec<(usize, Node<T, U>, usize)>;
// (page, node, history)
type SearchOutcome<T, U> = (usize, Node<T, U>, SearchHistory<T, U>);

/// An ordered map implemented by an on-disk B+ tree.
///
/// A B+ is an N-ary tree with a variable number of children per node. A B+ tree is a B-tree in
/// which each internal node contains keys and pointers to other nodes, and each leaf node
/// contains keys and values.
///
/// # Examples
/// ```
/// # use extended_collections::bptree;
/// # fn foo() -> bptree::Result<()> {
/// # use std::fs;
/// use extended_collections::bptree::BPMap;
///
/// let mut map: BPMap<u32, u64> = BPMap::new("example.dat", 4, 8)?;
/// map.insert(0, 1)?;
/// map.insert(3, 4)?;
///
/// assert_eq!(map.get(&0)?, Some(1));
/// assert_eq!(map.get(&1)?, None);
/// assert_eq!(map.len(), 2);
///
/// assert_eq!(map.min()?, Some(0));
///
/// assert_eq!(map.remove(&0)?, Some((0, 1)));
/// assert_eq!(map.remove(&1)?, None);
/// # fs::remove_file("example.dat").map_err(bptree::Error::IOError)?;
/// # Ok(())
/// # }
/// # foo();
/// ```
pub struct BPMap<T, U>
where
    T: Ord + Clone + Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    pager: Pager<T, U>,
}

impl<T, U> BPMap<T, U>
where
    T: Ord + Clone + Serialize + DeserializeOwned,
    U: Serialize + DeserializeOwned,
{
    /// Constructs a new, empty `BPMap<T, U>` with maximum sizes for keys and values, and creates a
    /// file for data persistence.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// // keys have a maximum of 4 bytes and values have a maximum of 8 bytes
    /// let map: BPMap<u32, u64> = BPMap::new("example_new.dat", 4, 8)?;
    /// # fs::remove_file("example_new.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn new(
        file_path: &str,
        key_size: u64,
        value_size: u64,
    ) -> Result<BPMap<T, U>> {
        let leaf_degree = LeafNode::<T, U>::get_degree(key_size, value_size);
        let internal_degree = InternalNode::<T, U>::get_degree(key_size);
        Pager::new(file_path, key_size, value_size, leaf_degree, internal_degree)
            .map(|pager| BPMap { pager })
    }

    /// Constructs a new, empty `BPMap<T, U>` with maximum sizes for keys and values and specific
    /// sizes for leaf and internal nodes, and creates a file for data persistence.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let map: BPMap<u32, u64> = BPMap::with_degrees("example_with_degrees.dat", 4, 8, 3, 3)?;
    /// # fs::remove_file("example_with_degrees.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn with_degrees(
        file_path: &str,
        key_size: u64,
        value_size: u64,
        leaf_degree: usize,
        internal_degree: usize,
    ) -> Result<BPMap<T, U>> {
        assert!(LeafNode::<T, U>::get_max_size(leaf_degree, key_size, value_size) <= BLOCK_SIZE);
        assert!(InternalNode::<T, U>::get_max_size(internal_degree, key_size) <= BLOCK_SIZE);
        Pager::new(file_path, key_size, value_size, leaf_degree, internal_degree)
            .map(|pager| BPMap { pager })
    }

    /// Opens an existing `BPMap<T, U>` from a file.
    ///
    /// # Examples
    /// ```no run
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// use extended_collections::bptree::BPMap;
    ///
    /// let map: BPMap<u32, u64> = BPMap::open("example_open.dat")?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn open(file_path: &str) -> Result<BPMap<T, U>> {
        Pager::open(file_path).map(|pager| BPMap { pager })
    }

    fn search_node(&mut self, key: &T) -> Result<SearchOutcome<T, U>> {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page)?;

        let mut stack = Vec::new();

        while let Node::Internal(node) = curr_node {
            let next_index = node.search(key);
            let next_page = node.pointers[next_index];
            stack.push((curr_page, Node::Internal(node), next_index));
            curr_page = next_page;
            curr_node = self.pager.get_page(curr_page)?;
        }
        Ok((curr_page, curr_node, stack))
    }

    /// Inserts a key-value pair into the map. If the key already exists in the map, it will return
    /// and replace the old key-value pair.
    ///
    /// # Panics
    /// Panics if attempting to insert a key or value that exceeds the maximum key or value size
    /// specified on creation.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_insert.dat", 4, 8)?;
    /// assert_eq!(map.insert(1, 1)?, None);
    /// assert_eq!(map.get(&1)?, Some(1));
    /// assert_eq!(map.insert(1, 2)?, Some((1, 1)));
    /// assert_eq!(map.get(&1)?, Some(2));
    /// # fs::remove_file("example_insert.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn insert(&mut self, key: T, value: U) -> Result<Option<(T, U)>> {
        self.pager.validate_key(&key)?;
        self.pager.validate_value(&value)?;
        let (mut curr_page, mut curr_node, mut stack) = self.search_node(&key)?;

        let mut split_node_entry = None;
        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                match curr_leaf_node.insert(Entry { key, value }) {
                    Some(InsertCases::Split { split_key, split_node }) => {
                        let split_node_index = self.pager.allocate_node(&split_node)?;
                        curr_leaf_node.next_leaf = Some(split_node_index);
                        split_node_entry = Some((split_key, split_node_index));
                        self.pager.write_node(curr_page, &Node::Leaf(curr_leaf_node))?;
                    },
                    Some(InsertCases::Entry(entry)) => {
                        self.pager.write_node(curr_page, &Node::Leaf(curr_leaf_node))?;
                        return Ok(Some((entry.key, entry.value)));
                    },
                    None => self.pager.write_node(curr_page, &Node::Leaf(curr_leaf_node))?,
                }
            },
            _ => unreachable!(),
        }

        while let Some((split_key, split_pointer)) = split_node_entry {
            match stack.pop() {
                Some((parent_page, mut parent_node, _)) => {
                    match parent_node {
                        Node::Internal(ref mut node) => {
                            if let Some((split_key, split_node)) = node.insert(split_key, split_pointer, true) {
                                let split_node_index = self.pager.allocate_node(&split_node)?;
                                split_node_entry = Some((split_key, split_node_index));
                            } else {
                                split_node_entry = None
                            }
                        },
                        _ => unreachable!(),
                    }
                    curr_node = parent_node;
                    curr_page = parent_page;
                    self.pager.write_node(curr_page, &curr_node)?;
                },
                None => {
                    let mut new_root = InternalNode::new(self.pager.get_internal_degree());
                    new_root.keys[0] = Some(split_key);
                    new_root.pointers[0] = curr_page;
                    new_root.pointers[1] = split_pointer;
                    new_root.len = 1;
                    let new_root_page = self.pager.allocate_node(&Node::Internal(new_root))?;
                    self.pager.set_root_page(new_root_page)?;
                    split_node_entry = None;
                },
            }
        }
        let new_len = self.pager.get_len() + 1;
        self.pager.set_len(new_len)?;
        Ok(None)
    }

    /// Removes a key-value pair from the map. If the key exists in the map, it will return the
    /// associated key-value pair. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_remove.dat", 4, 8)?;
    /// map.insert(1, 1)?;
    /// assert_eq!(map.remove(&1)?, Some((1, 1)));
    /// assert_eq!(map.remove(&1)?, None);
    /// # fs::remove_file("example_remove.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn remove(&mut self, key: &T) -> Result<Option<(T, U)>> {
        let (curr_page, curr_node, mut stack) = self.search_node(key)?;
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
                            match self.pager.get_page(sibling_page)? {
                                Node::Leaf(node) => node,
                                _ => unreachable!(),
                            }
                        };

                        // merge
                        if sibling_leaf_node.len == (self.pager.get_leaf_degree() + 1) / 2 {
                            if sibling_index == curr_index + 1 {
                                curr_leaf_node.merge(&mut sibling_leaf_node);
                                delete_entry = Some((curr_index, parent_page, parent_internal_node));
                                self.pager.deallocate_node(sibling_page)?;
                                self.pager.write_node(curr_page, &Node::Leaf(curr_leaf_node))?;
                            } else {
                                sibling_leaf_node.merge(&mut curr_leaf_node);
                                delete_entry = Some((sibling_index, parent_page, parent_internal_node));
                                self.pager.deallocate_node(curr_page)?;
                                self.pager.write_node(sibling_page, &Node::Leaf(sibling_leaf_node))?;
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
                            self.pager.write_node(parent_page, &Node::Internal(parent_internal_node))?;
                            self.pager.write_node(sibling_page, &Node::Leaf(sibling_leaf_node))?;
                            self.pager.write_node(curr_page, &Node::Leaf(curr_leaf_node))?;
                        }
                    }
                    let new_len = self.pager.get_len() - 1;
                    self.pager.set_len(new_len)?;
                } else if ret.is_some() {
                    let new_len = self.pager.get_len() - 1;
                    self.pager.set_len(new_len)?;
                    self.pager.write_node(curr_page, &Node::Leaf(curr_leaf_node))?;
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
                        match self.pager.get_page(sibling_page)? {
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
                            self.pager.deallocate_node(sibling_page)?;
                            self.pager.write_node(curr_page, &Node::Internal(curr_internal_node))?;
                        } else {
                            let parent_key = match parent_internal_node.keys[sibling_index] {
                                Some(ref key) => key.clone(),
                                None => unreachable!(),
                            };
                            sibling_internal_node.merge(parent_key, &mut curr_internal_node);
                            delete_entry = Some((sibling_index, parent_page, parent_internal_node));
                            self.pager.deallocate_node(curr_page)?;
                            self.pager.write_node(sibling_page, &Node::Internal(sibling_internal_node))?;
                        }
                    } else if sibling_index == curr_index + 1 {
                        let (mut removed_key, removed_pointer) = sibling_internal_node.remove_at(0, false);
                        let removed_key = match mem::replace(&mut parent_internal_node.keys[curr_index], Some(removed_key)) {
                            Some(key) => key,
                            _ => unreachable!(),
                        };
                        curr_internal_node.insert(removed_key, removed_pointer, true);
                        self.pager.write_node(parent_page, &Node::Internal(parent_internal_node))?;
                        self.pager.write_node(sibling_page, &Node::Internal(sibling_internal_node))?;
                        self.pager.write_node(curr_page, &Node::Internal(curr_internal_node))?;
                    } else {
                        let remove_index = sibling_internal_node.len - 1;
                        let (mut removed_key, removed_pointer) = sibling_internal_node.remove_at(remove_index, true);
                        let removed_key = match mem::replace(&mut parent_internal_node.keys[sibling_index], Some(removed_key)) {
                            Some(key) => key,
                            _ => unreachable!(),
                        };
                        curr_internal_node.insert(removed_key, removed_pointer, false);
                        self.pager.write_node(parent_page, &Node::Internal(parent_internal_node))?;
                        self.pager.write_node(sibling_page, &Node::Internal(sibling_internal_node))?;
                        self.pager.write_node(curr_page, &Node::Internal(curr_internal_node))?;
                    }
                } else if curr_internal_node.len == 0 {
                    self.pager.set_root_page(curr_internal_node.pointers[0])?;
                    self.pager.deallocate_node(curr_page)?;
                } else {
                    self.pager.write_node(curr_page, &Node::Internal(curr_internal_node))?;
                }
            } else {
                self.pager.write_node(curr_page, &Node::Internal(curr_internal_node))?;
            }
        }
        Ok(ret.map(|entry| (entry.key, entry.value)))
    }

    /// Checks if a key exists in the map.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_contains_key.dat", 4, 8)?;
    /// map.insert(1, 1)?;
    /// assert!(!map.contains_key(&0)?);
    /// assert!(map.contains_key(&1)?);
    /// # fs::remove_file("example_contains_key.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn contains_key(&mut self, key: &T) -> Result<bool> {
        self.get(key).map(|value| value.is_some())
    }

    /// Returns the value associated with a particular key. It will return `None` if the key does
    /// not exist in the map.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_get.dat", 4, 8)?;
    /// map.insert(1, 1)?;
    /// assert_eq!(map.get(&0)?, None);
    /// assert_eq!(map.get(&1)?, Some(1));
    /// # fs::remove_file("example_get.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn get(&mut self, key: &T) -> Result<Option<U>> {
        let (_, curr_node, _) = self.search_node(key)?;
        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                Ok(curr_leaf_node.search(key).and_then(|index| {
                    match mem::replace(&mut curr_leaf_node.entries[index], None) {
                        Some(entry) => Some(entry.value),
                        _ => unreachable!(),
                    }
                }))
            },
            _ => unreachable!(),
        }
    }

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_len.dat", 4, 8)?;
    /// map.insert(1, 1)?;
    /// assert_eq!(map.len(), 1);
    /// # fs::remove_file("example_len.dat").map_err(bptree::Error::IOError)?;
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
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let map: BPMap<u32, u64> = BPMap::new("example_is_empty.dat", 4, 8)?;
    /// assert!(map.is_empty());
    /// # fs::remove_file("example_is_empty.dat").map_err(bptree::Error::IOError)?;
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
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_clear.dat", 4, 8)?;
    /// map.insert(1, 1)?;
    /// map.insert(2, 2)?;
    /// map.clear();
    /// assert_eq!(map.is_empty(), true);
    /// # fs::remove_file("example_clear.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn clear(&mut self) -> Result<()> {
        self.pager.clear()
    }

    /// Returns the minimum key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_min.dat", 4, 8)?;
    /// map.insert(1, 1)?;
    /// map.insert(3, 3)?;
    /// assert_eq!(map.min()?, Some(1));
    /// # fs::remove_file("example_min.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn min(&mut self) -> Result<Option<T>> {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page)?;

        while let Node::Internal(curr_internal_node) = curr_node {
            curr_page = curr_internal_node.pointers[0];
            curr_node = self.pager.get_page(curr_page)?;
        }

        match curr_node {
            Node::Leaf(mut curr_leaf_node) => Ok(mem::replace(&mut curr_leaf_node.entries[0], None).map(|entry| entry.key)),
            _ => unreachable!(),
        }
    }

    /// Returns the maximum key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_max.dat", 4, 8)?;
    /// map.insert(1, 1)?;
    /// map.insert(3, 3)?;
    /// assert_eq!(map.max()?, Some(3));
    /// # fs::remove_file("example_max.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn max(&mut self) -> Result<Option<T>> {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page)?;

        while let Node::Internal(curr_internal_node) = curr_node {
            curr_page = curr_internal_node.pointers[curr_internal_node.len];
            curr_node = self.pager.get_page(curr_page)?;
        }

        match curr_node {
            Node::Leaf(mut curr_leaf_node) => {
                if curr_leaf_node.len == 0 {
                    Ok(None)
                } else {
                    let index = curr_leaf_node.len - 1;
                    Ok(mem::replace(&mut curr_leaf_node.entries[index], None).map(|entry| entry.key))
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
    /// # use extended_collections::bptree;
    /// # fn foo() -> bptree::Result<()> {
    /// # use std::fs;
    /// use extended_collections::bptree::BPMap;
    ///
    /// let mut map: BPMap<u32, u64> = BPMap::new("example_iter_mut.dat", 4, 8)?;
    /// map.insert(1, 1)?;
    /// map.insert(2, 2)?;
    ///
    /// let mut iterator = map.iter_mut()?.map(|value| value.unwrap());
    /// assert_eq!(iterator.next(), Some((1, 1)));
    /// assert_eq!(iterator.next(), Some((2, 2)));
    /// assert_eq!(iterator.next(), None);
    /// # fs::remove_file("example_iter_mut.dat").map_err(bptree::Error::IOError)?;
    /// # Ok(())
    /// # }
    /// # foo();
    /// ```
    pub fn iter_mut(&mut self) -> Result<BPMapIterMut<T, U>> {
        let mut curr_page = self.pager.get_root_page();
        let mut curr_node = self.pager.get_page(curr_page)?;

        while let Node::Internal(curr_internal_node) = curr_node {
            curr_page = curr_internal_node.pointers[0];
            curr_node = self.pager.get_page(curr_page)?;
        }

        match curr_node {
            Node::Leaf(curr_leaf_node) => {
                Ok(BPMapIterMut {
                    pager: &mut self.pager,
                    curr_node: curr_leaf_node,
                    curr_index: 0,
                })
            },
            _ => unreachable!(),
        }
    }
}

impl<'a, T, U> IntoIterator for &'a mut BPMap<T, U>
where
    T: 'a + Ord + Clone + Serialize + DeserializeOwned,
    U: 'a + Serialize + DeserializeOwned,
{
    type Item = Result<(T, U)>;
    type IntoIter = BPMapIterMut<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut().unwrap()
    }
}

/// A mutable iterator for `BPMap<T, U>`.
///
/// This iterator traverses the elements of the map in ascending order and yields owned entries.
pub struct BPMapIterMut<'a, T, U>
where
    T: 'a + Ord + Clone + Serialize + DeserializeOwned,
    U: 'a + Serialize + DeserializeOwned,
{
    pager: &'a mut Pager<T, U>,
    curr_node: LeafNode<T, U>,
    curr_index: usize,
}

impl<'a, T, U> Iterator for BPMapIterMut<'a, T, U>
where
    T: 'a + Ord + Clone + Serialize + DeserializeOwned,
    U: 'a + Serialize + DeserializeOwned,
{
    type Item = Result<(T, U)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_index >= self.curr_node.len {
            match self.curr_node.next_leaf {
                Some(next_page) => {
                    self.curr_node = {
                        match self.pager.get_page(next_page) {
                            Ok(node) => {
                                match node {
                                    Node::Leaf(leaf_node) => leaf_node,
                                    _ => unreachable!(),
                                }
                            },
                            Err(error) => return Some(Err(error)),
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
                Some(Ok((entry.key, entry.value)))
            },
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BPMap, Result};
    use std::fs;
    use std::panic;

    fn teardown(test_name: &str) {
        fs::remove_file(&format!("{}.dat", test_name)).ok();
    }

    fn run_test<T>(test: T, test_name: &str)
    where
        T: FnOnce() -> Result<()> + panic::UnwindSafe,
    {
        let result = panic::catch_unwind(|| test().unwrap());

        teardown(test_name);

        assert!(result.is_ok());
    }

    #[test]
    fn test_len_empty() {
        let test_name = "test_len_empty";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let map: BPMap<u32, u64> = BPMap::new(file_name, 4, 8)?;
                assert_eq!(map.len(), 0);
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_is_empty() {
        let test_name = "test_is_empty";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let map: BPMap<u32, u64> = BPMap::new(file_name, 4, 8)?;
                assert!(map.is_empty());
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_min_max_empty() {
        let test_name = "test_min_max_empty";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, u64> = BPMap::new(file_name, 4, 8)?;
                assert_eq!(map.min()?, None);
                assert_eq!(map.max()?, None);
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_free_node() {
        let test_name = "test_free_node";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, u64> = BPMap::with_degrees(file_name, 4, 8, 3, 3)?;
                map.insert(1, 1)?;
                map.insert(2, 2)?;
                map.insert(3, 3)?;
                map.insert(4, 4)?;
                assert_eq!(map.pager.get_root_page(), 2);
                map.remove(&1)?;
                map.remove(&2)?;
                map.remove(&3)?;
                map.remove(&4)?;
                assert_eq!(map.pager.get_root_page(), 0);
                map.insert(1, 1)?;
                map.insert(2, 2)?;
                map.insert(3, 3)?;
                map.insert(4, 4)?;
                assert_eq!(map.pager.get_root_page(), 1);
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_get() {
        let test_name = "test_get";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, u64> = BPMap::with_degrees(file_name, 4, 8, 3, 3)?;
                map.insert(1, 1)?;
                assert_eq!(map.get(&1)?, Some(1));
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_insert() {
        let test_name = "test_insert";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, u64> = BPMap::with_degrees(file_name, 4, 8, 3, 3)?;
                assert_eq!(map.insert(1, 1)?, None);
                assert!(map.contains_key(&1)?);
                assert_eq!(map.get(&1)?, Some(1));
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    #[should_panic]
    fn test_insert_panic() {
        let test_name = "test_insert_panic";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, Box<[u32]>> = BPMap::new(file_name, 4, 12)?;
                map.insert(0, Box::new([0, 1]))?;
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_insert_variable_sizes() {
        let test_name = "test_insert_variable_sizes";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, Box<[u32]>> = BPMap::new(file_name, 4, 16)?;
                map.insert(0, Box::new([0, 1]))?;
                map.insert(1, Box::new([0]))?;
                assert_eq!(*(map.get(&0)?.unwrap()), [0, 1]);
                assert_eq!(*(map.get(&1)?.unwrap()), [0]);
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_insert_replace() {
        let test_name = "test_insert_replace";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, u64> = BPMap::with_degrees(file_name, 4, 8, 3, 3)?;
                assert_eq!(map.insert(1, 1)?, None);
                assert_eq!(map.insert(1, 3)?, Some((1, 1)));
                assert_eq!(map.get(&1)?, Some(3));
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_remove() {
        let test_name = "test_remove";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, u64> = BPMap::with_degrees(file_name, 4, 8, 3, 3)?;
                map.insert(1, 1)?;
                assert_eq!(map.remove(&1)?, Some((1, 1)));
                assert!(!map.contains_key(&1)?);
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_min_max() {
        let test_name = "test_min_max";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, u64> = BPMap::with_degrees(file_name, 4, 8, 3, 3)?;
                map.insert(1, 1)?;
                map.insert(3, 3)?;
                map.insert(5, 5)?;

                assert_eq!(map.min()?, Some(1));
                assert_eq!(map.max()?, Some(5));
                Ok(())
            },
            test_name,
        );
    }

    #[test]
    fn test_iter_mut() {
        let test_name = "test_iter_mut";
        let file_name = &format!("{}.dat", test_name);
        run_test(
            || {
                let mut map: BPMap<u32, u64> = BPMap::with_degrees(file_name, 4, 8, 3, 3)?;
                map.insert(1, 2)?;
                map.insert(5, 6)?;
                map.insert(3, 4)?;

                map.insert(7, 8)?;
                map.insert(11, 12)?;
                map.insert(9, 10)?;

                assert_eq!(
                    map.iter_mut()?.map(|value| value.unwrap()).collect::<Vec<(u32, u64)>>(),
                    vec![(1, 2), (3, 4), (5, 6), (7, 8), (9, 10), (11, 12)],
                );
                Ok(())
            },
            test_name,
        );
    }
}
