use entry::Entry;
use std::borrow::Borrow;
use std::cmp::{self, Ordering};
use std::marker::PhantomData;
use std::mem;

const U64_SIZE: u64 = mem::size_of::<u64>() as u64;
const OPT_U64_SIZE: u64 = mem::size_of::<Option<u64>>() as u64;
pub const BLOCK_SIZE: u64 = 4096;

#[derive(Serialize, Deserialize)]
pub struct InternalNode<T, U> {
    pub len: usize,
    pub keys: Box<[Option<T>]>,
    pub pointers: Box<[usize]>,
    pub _marker: PhantomData<U>,
}

impl<T, U> InternalNode<T, U> {
    // 1) a usize is encoded as u64 (8 bytes)
    // 2) a boxed slice is encoded as a tuple of u64 (8 bytes) and the items
    #[inline]
    fn get_constant_size() -> u64 {
        U64_SIZE * 4 + mem::size_of::<PhantomData<U>>() as u64
    }

    #[inline]
    fn get_payload_size(key_size: u64) -> u64 {
        let option_size = mem::size_of::<Option<T>>() as u64;
        let entry_size = mem::size_of::<T>() as u64;
        U64_SIZE + key_size + option_size - entry_size
    }

    #[inline]
    pub fn get_degree(key_size: u64) -> usize {
        let payload_capacity = BLOCK_SIZE - Self::get_constant_size();
        (payload_capacity / Self::get_payload_size(key_size)) as usize
    }

    #[inline]
    pub fn get_max_size(degree: usize, key_size: u64) -> u64 {
        Self::get_constant_size() + degree as u64 * Self::get_payload_size(key_size)
    }

    pub fn new(degree: usize) -> Self {
        InternalNode {
            len: 0,
            keys: init_array!(Option<T>, degree, None),
            pointers: init_array!(usize, degree + 1, 0),
            _marker: PhantomData,
        }
    }

    pub fn insert(
        &mut self,
        mut new_key: T,
        mut new_pointer: usize,
        is_right: bool,
    ) -> Option<(T, Node<T, U>)>
    where
        T: Ord,
    {
        let internal_degree = self.keys.len();
        let offset = is_right as usize;
        // node has room; can insert
        if self.len < internal_degree {
            let mut index = 0;
            while let Some(ref mut key) = self.keys[index] {
                if new_key < *key {
                    mem::swap(&mut new_key, key);
                    mem::swap(&mut new_pointer, &mut self.pointers[index + offset]);
                }
                index += 1;
            }
            self.keys[index] = Some(new_key);
            mem::swap(&mut new_pointer, &mut self.pointers[index + offset]);
            if !is_right {
                self.pointers[index + 1] = new_pointer;
            }
            self.len += 1;
            None
        }
        // node is full; have to split
        else {
            let mut split_node = InternalNode::<T, U>::new(internal_degree);
            let mut index = 0;
            while index < internal_degree {
                if let Some(ref mut key) = self.keys[index] {
                    if new_key < *key {
                        mem::swap(&mut new_key, key);
                        mem::swap(&mut new_pointer, &mut self.pointers[index + offset]);
                    }
                }
                if index > (internal_degree + 1) / 2 {
                    mem::swap(
                        &mut self.keys[index],
                        &mut split_node.keys[index - (internal_degree + 1) / 2 - 1],
                    );
                    mem::swap(
                        &mut self.pointers[index + offset],
                        &mut split_node.pointers[index - (internal_degree + 1) / 2 - (1 - offset)],
                    );
                }
                index += 1;
            }
            split_node.keys[(internal_degree - 2) / 2] = Some(new_key);
            split_node.pointers[(internal_degree - 2) / 2 + offset] = new_pointer;
            let split_key = mem::replace(&mut self.keys[(internal_degree + 1) / 2], None)
                .expect("Expected some key.");
            mem::swap(
                &mut self.pointers[(internal_degree + 1) / 2 + 1],
                &mut split_node.pointers[(1 - offset)],
            );
            split_node.len = internal_degree / 2;
            self.len = (internal_degree + 1) / 2;

            Some((split_key, Node::Internal(split_node)))
        }
    }

    pub fn remove_at(&mut self, remove_index: usize, is_right: bool) -> (T, usize) {
        assert!(remove_index < self.len);
        let offset = is_right as usize;
        self.len -= 1;
        for index in remove_index..self.len {
            self.keys.swap(index, index + 1);
            self.pointers.swap(index + offset, index + offset + 1);
        }

        let ret_pointer = {
            if !is_right {
                self.pointers.swap(self.len, self.len + 1);
            }
            mem::replace(&mut self.pointers[self.len + 1], 0)
        };
        let ret_key = mem::replace(&mut self.keys[self.len], None).expect("Expected some key.");

        (ret_key, ret_pointer)
    }

    pub fn search<V>(&self, search_key: &V) -> usize
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        let mut lo = 0;
        let mut hi = (self.keys.len() - 1) as isize;
        while lo <= hi {
            let mid = lo + ((hi - lo) >> 1);
            match self.keys[mid as usize] {
                None => hi = mid - 1,
                Some(ref key) => {
                    if key.borrow() <= search_key {
                        lo = mid + 1;
                    } else {
                        hi = mid - 1;
                    }
                },
            }
        }
        lo as usize
    }

    pub fn merge(&mut self, split_key: T, node: &mut InternalNode<T, U>) {
        assert!(self.len + node.len < self.keys.len());
        self.keys[self.len] = Some(split_key);
        for index in 0..node.len {
            self.keys[self.len + index + 1] = node.keys[index].take();
            self.pointers[self.len + index + 1] = mem::replace(&mut node.pointers[index], 0);
        }
        self.len += node.len + 1;
        self.pointers[self.len] = mem::replace(&mut node.pointers[node.len], 0);
        node.len = 0;
    }
}

#[derive(Serialize, Deserialize)]
pub struct LeafNode<T, U> {
    pub len: usize,
    pub entries: Box<[Option<Entry<T, U>>]>,
    pub next_leaf: Option<usize>,
}

pub enum InsertCases<T, U> {
    Split {
        split_key: T,
        split_node: Node<T, U>,
    },
    Entry(Entry<T, U>),
}

impl<T, U> LeafNode<T, U> {
    // 1) a usize is encoded as u64 (8 bytes)
    // 2) a boxed slice is encoded as a tuple of u64 (8 bytes) and the items
    #[inline]
    fn get_constant_size() -> u64 {
        U64_SIZE * 2 + OPT_U64_SIZE
    }

    #[inline]
    fn get_payload_size(key_size: u64, value_size: u64) -> u64 {
        let option_size = mem::size_of::<Option<Entry<T, U>>>() as u64;
        let entry_size = mem::size_of::<Entry<T, U>>() as u64;
        key_size + value_size + option_size - entry_size
    }

    #[inline]
    pub fn get_degree(key_size: u64, value_size: u64) -> usize {
        let payload_capacity = BLOCK_SIZE - Self::get_constant_size();
        (payload_capacity / Self::get_payload_size(key_size, value_size)) as usize
    }

    #[inline]
    pub fn get_max_size(degree: usize, key_size: u64, value_size: u64) -> u64 {
        Self::get_constant_size() + degree as u64 * Self::get_payload_size(key_size, value_size)
    }

    pub fn new(degree: usize) -> Self {
        LeafNode {
            len: 0,
            entries: init_array!(Option<Entry<T, U>>, degree, None),
            next_leaf: None,
        }
    }

    pub fn insert(&mut self, mut new_entry: Entry<T, U>) -> Option<InsertCases<T, U>>
    where
        T: Clone + Ord,
    {
        let leaf_degree = self.entries.len();
        // node has room; can insert
        if self.len < leaf_degree {
            let mut index = 0;
            while let Some(ref mut entry) = self.entries[index] {
                if new_entry <= *entry {
                    mem::swap(entry, &mut new_entry);
                    if new_entry == *entry {
                        return Some(InsertCases::Entry(new_entry));
                    }
                }
                index += 1;
            }
            self.len += 1;
            self.entries[index] = Some(new_entry);
            None
        }
        // node is full; have to split
        else {
            let mut split_node = LeafNode::new(leaf_degree);
            for index in 0..leaf_degree {
                if let Some(ref mut entry) = self.entries[index] {
                    if new_entry <= *entry {
                        mem::swap(entry, &mut new_entry);
                        if new_entry == *entry {
                            return Some(InsertCases::Entry(new_entry));
                        }
                    }
                }
                if index > leaf_degree / 2 {
                    mem::swap(
                        &mut self.entries[index],
                        &mut split_node.entries[index - leaf_degree / 2 - 1],
                    );
                }
            }
            split_node.entries[(leaf_degree - 1) / 2] = Some(new_entry);
            let split_key = split_node.entries[0]
                .as_ref()
                .map(|entry| entry.key.clone())
                .expect("Expected some key.");
            let split_node = Node::Leaf(LeafNode {
                len: (self.len + 1) / 2,
                entries: split_node.entries,
                next_leaf: self.next_leaf,
            });
            self.len = (self.len + 2) / 2;
            Some(InsertCases::Split {
                split_key,
                split_node,
            })
        }
    }

    pub fn remove_at(&mut self, remove_index: usize) -> Entry<T, U> {
        assert!(remove_index < self.len);
        self.len -= 1;
        for index in remove_index..self.len {
            self.entries.swap(index, index + 1);
        }
        self.entries[self.len].take().expect("Expected some entry.")
    }

    pub fn remove<V>(&mut self, key: &V) -> Option<Entry<T, U>>
    where
        T: Borrow<V>,
        V: Eq + ?Sized,
    {
        let mut removed = false;
        for index in 0..self.len {
            let swap = {
                if let Some(ref entry) = self.entries[index] {
                    if key == entry.key.borrow() {
                        removed = true;
                        index + 1 < self.len
                    } else {
                        false
                    }
                } else {
                    false
                }
            };

            if swap {
                self.entries.swap(index, index + 1);
            }
        }

        if removed {
            self.len -= 1;
            self.entries[self.len].take()
        } else {
            None
        }
    }

    pub fn search<V>(&self, search_key: &V) -> Option<usize>
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        let mut lo = 0;
        let mut hi = (self.entries.len() - 1) as isize;
        while lo <= hi {
            let mid = lo + ((hi - lo) >> 1);
            match self.entries[mid as usize] {
                None => hi = mid - 1,
                Some(ref entry) => {
                    match entry.key.borrow().cmp(search_key) {
                        Ordering::Less => lo = mid + 1,
                        Ordering::Greater => hi = mid - 1,
                        Ordering::Equal => return Some(mid as usize),
                    }
                },
            }
        }
        None
    }

    pub fn merge(&mut self, node: &mut LeafNode<T, U>) {
        assert!(self.len + node.len <= self.entries.len());
        self.next_leaf = node.next_leaf.take();
        for index in 0..node.len {
            self.entries[self.len + index] = node.entries[index].take();
        }
        self.len += node.len;
        node.len = 0;
    }
}

#[derive(Serialize, Deserialize)]
pub enum Node<T, U> {
    Internal(InternalNode<T, U>),
    Leaf(LeafNode<T, U>),
    Free(Option<usize>),
}

impl<T, U> Node<T, U> {
    #[inline]
    pub fn get_max_size(
        key_size: u64,
        value_size: u64,
        leaf_degree: usize,
        internal_degree: usize,
    ) -> u64 {
        cmp::max(
            LeafNode::<T, U>::get_max_size(leaf_degree, key_size, value_size),
            InternalNode::<T, U>::get_max_size(internal_degree, key_size),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{InsertCases, InternalNode, LeafNode, Node};
    use entry::Entry;
    use std::marker::PhantomData;

    #[test]
    fn test_node_get_max_size() {
        assert_eq!(Node::<u32, u64>::get_max_size(4, 8, 1, 1), 52);
    }

    #[test]
    fn test_internal_node_degree() {
        assert_eq!(InternalNode::<u32, u64>::get_degree(4), 254);
    }

    #[test]
    fn test_internal_node_get_max_size() {
        assert_eq!(InternalNode::<u32, u64>::get_max_size(1, 4), 48);
    }

    #[test]
    fn test_internal_node_new() {
        let n = InternalNode::<u32, u64>::new(3);

        assert_eq!(n.len, 0);
        assert_eq!(*n.keys, [None, None, None]);
        assert_eq!(*n.pointers, [0, 0, 0, 0]);
    }

    #[test]
    fn test_internal_node_insert_left_not_full() {
        let mut n = InternalNode::<u32, u64> {
            len: 2,
            keys: Box::new([Some(0), Some(2), None]),
            pointers: Box::new([0, 2, 3, 0]),
            _marker: PhantomData,
        };

        assert!(n.insert(1, 1, false).is_none());
        assert_eq!(n.len, 3);
        assert_eq!(*n.keys, [Some(0), Some(1), Some(2)]);
        assert_eq!(*n.pointers, [0, 1, 2, 3]);
    }

    #[test]
    fn test_internal_node_insert_right_not_full() {
        let mut n = InternalNode::<u32, u64> {
            len: 2,
            keys: Box::new([Some(0), Some(2), None]),
            pointers: Box::new([0, 1, 3, 0]),
            _marker: PhantomData,
        };

        assert!(n.insert(1, 2, true).is_none());
        assert_eq!(n.len, 3);
        assert_eq!(*n.keys, [Some(0), Some(1), Some(2)]);
        assert_eq!(*n.pointers, [0, 1, 2, 3]);
    }

    #[test]
    fn test_internal_node_insert_left_full() {
        let mut n = InternalNode::<u32, u64> {
            len: 3,
            keys: Box::new([Some(0), Some(1), Some(3)]),
            pointers: Box::new([0, 1, 3, 4]),
            _marker: PhantomData,
        };
        let res = n.insert(2, 2, false).unwrap();

        let (split_key, split_node) = res;
        let internal_node = {
            match split_node {
                Node::Internal(node) => node,
                _ => panic!("Expected internal node."),
            }
        };

        assert_eq!(split_key, 2);
        assert_eq!(internal_node.len, 1);
        assert_eq!(*internal_node.keys, [Some(3), None, None]);
        assert_eq!(*internal_node.pointers, [3, 4, 0, 0]);

        assert_eq!(n.len, 2);
        assert_eq!(*n.keys, [Some(0), Some(1), None]);
        assert_eq!(*n.pointers, [0, 1, 2, 0]);
    }

    #[test]
    fn test_internal_node_insert_right_full() {
        let mut n = InternalNode::<u32, u64> {
            len: 3,
            keys: Box::new([Some(0), Some(1), Some(3)]),
            pointers: Box::new([0, 1, 2, 4]),
            _marker: PhantomData,
        };
        let res = n.insert(2, 3, true).unwrap();

        let (split_key, split_node) = res;
        let internal_node = {
            match split_node {
                Node::Internal(node) => node,
                _ => panic!("Expected internal node."),
            }
        };

        assert_eq!(split_key, 2);
        assert_eq!(internal_node.len, 1);
        assert_eq!(*internal_node.keys, [Some(3), None, None]);
        assert_eq!(*internal_node.pointers, [3, 4, 0, 0]);

        assert_eq!(n.len, 2);
        assert_eq!(*n.keys, [Some(0), Some(1), None]);
        assert_eq!(*n.pointers, [0, 1, 2, 0]);
    }

    #[test]
    fn test_internal_node_remove_at_left() {
        let mut n = InternalNode::<u32, u64> {
            len: 3,
            keys: Box::new([Some(0), Some(1), Some(2)]),
            pointers: Box::new([0, 1, 2, 3]),
            _marker: PhantomData,
        };

        assert_eq!(n.remove_at(1, false), (1, 1));
        assert_eq!(n.len, 2);
        assert_eq!(*n.keys, [Some(0), Some(2), None]);
        assert_eq!(*n.pointers, [0, 2, 3, 0]);
    }

    #[test]
    fn test_internal_node_remove_at_right() {
        let mut n = InternalNode::<u32, u64> {
            len: 3,
            keys: Box::new([Some(0), Some(1), Some(2)]),
            pointers: Box::new([0, 1, 2, 3]),
            _marker: PhantomData,
        };

        assert_eq!(n.remove_at(1, true), (1, 2));
        assert_eq!(n.len, 2);
        assert_eq!(*n.keys, [Some(0), Some(2), None]);
        assert_eq!(*n.pointers, [0, 1, 3, 0]);
    }

    #[test]
    fn test_internal_node_search() {
        let n = InternalNode::<u32, u64> {
            len: 3,
            keys: Box::new([Some(1), Some(3), Some(5)]),
            pointers: Box::new([0, 1, 2, 3]),
            _marker: PhantomData,
        };

        assert_eq!(n.search(&0), 0);
        assert_eq!(n.search(&1), 1);
        assert_eq!(n.search(&2), 1);
        assert_eq!(n.search(&3), 2);
        assert_eq!(n.search(&4), 2);
        assert_eq!(n.search(&5), 3);
        assert_eq!(n.search(&6), 3);
    }

    #[test]
    fn test_internal_node_merge() {
        let mut n = InternalNode::<u32, u64> {
            len: 1,
            keys: Box::new([Some(0), None, None]),
            pointers: Box::new([0, 1, 0, 0]),
            _marker: PhantomData,
        };
        let mut m = InternalNode::<u32, u64> {
            len: 1,
            keys: Box::new([Some(2), None, None]),
            pointers: Box::new([2, 3, 0, 0]),
            _marker: PhantomData,
        };
        n.merge(1, &mut m);

        assert_eq!(n.len, 3);
        assert_eq!(*n.keys, [Some(0), Some(1), Some(2)]);
        assert_eq!(*n.pointers, [0, 1, 2, 3]);

        assert_eq!(m.len, 0);
        assert_eq!(*m.keys, [None, None, None]);
        assert_eq!(*m.pointers, [0, 0, 0, 0]);
    }

    #[test]
    fn test_leaf_node_degree() {
        assert_eq!(LeafNode::<u32, u64>::get_degree(4, 8), 203);
    }

    #[test]
    fn test_leaf_node_get_max_size() {
        assert_eq!(LeafNode::<u32, u64>::get_max_size(1, 4, 8), 52);
    }

    #[test]
    fn test_leaf_node_new() {
        let n = LeafNode::<u32, u64>::new(3);

        assert_eq!(n.len, 0);
        assert_eq!(*n.entries, [None, None, None]);
        assert_eq!(n.next_leaf, None);
    }

    #[test]
    fn test_leaf_node_insert_not_full() {
        let mut n = LeafNode::<u32, u64> {
            len: 2,
            entries: Box::new([
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 2, value: 2 }),
                None,
            ]),
            next_leaf: None,
        };

        assert!(n.insert(Entry { key: 1, value: 1 }).is_none());
        assert_eq!(n.len, 3);
        assert_eq!(
            *n.entries,
            [
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 1, value: 1 }),
                Some(Entry { key: 2, value: 2 }),
            ]
        );
        assert_eq!(n.next_leaf, None);
    }

    #[test]
    fn test_leaf_node_insert_full() {
        let mut n = LeafNode::<u32, u64> {
            len: 3,
            entries: Box::new([
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 2, value: 2 }),
                Some(Entry { key: 3, value: 3 }),
            ]),
            next_leaf: None,
        };
        let res = n.insert(Entry { key: 1, value: 1 }).unwrap();

        let (split_key, split_node) = {
            match res {
                InsertCases::Split {
                    split_key,
                    split_node,
                } => (split_key, split_node),
                _ => panic!("Expected split insert case."),
            }
        };

        let leaf_node = {
            match split_node {
                Node::Leaf(node) => node,
                _ => panic!("Expected leaf node."),
            }
        };

        assert_eq!(split_key, 2);
        assert_eq!(leaf_node.len, 2);
        assert_eq!(
            *leaf_node.entries,
            [
                Some(Entry { key: 2, value: 2 }),
                Some(Entry { key: 3, value: 3 }),
                None,
            ]
        );
        assert_eq!(leaf_node.next_leaf, None);

        assert_eq!(n.len, 2);
        assert_eq!(
            *n.entries,
            [
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 1, value: 1 }),
                None,
            ]
        );
        assert_eq!(n.next_leaf, None);
    }

    #[test]
    fn test_leaf_node_insert_existing() {
        let mut n = LeafNode::<u32, u64> {
            len: 3,
            entries: Box::new([
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 1, value: 0 }),
                Some(Entry { key: 2, value: 2 }),
            ]),
            next_leaf: None,
        };
        let res = n.insert(Entry { key: 1, value: 1 }).unwrap();

        let entry = match res {
            InsertCases::Entry(entry) => entry,
            _ => panic!("Expected entry insert case."),
        };

        assert_eq!(entry.key, 1);
        assert_eq!(entry.value, 0);

        assert_eq!(n.len, 3);
        assert_eq!(
            *n.entries,
            [
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 1, value: 1 }),
                Some(Entry { key: 2, value: 2 }),
            ]
        );
        assert_eq!(n.next_leaf, None);
    }

    #[test]
    fn test_leaf_node_remove_at() {
        let mut n = LeafNode::<u32, u64> {
            len: 3,
            entries: Box::new([
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 1, value: 1 }),
                Some(Entry { key: 2, value: 2 }),
            ]),
            next_leaf: None,
        };

        assert_eq!(n.remove_at(1), Entry { key: 1, value: 1 });
        assert_eq!(n.len, 2);
        assert_eq!(
            *n.entries,
            [
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 2, value: 2 }),
                None,
            ]
        );
        assert_eq!(n.next_leaf, None);
    }

    #[test]
    fn test_leaf_node_search() {
        let n = LeafNode::<u32, u64> {
            len: 3,
            entries: Box::new([
                Some(Entry { key: 1, value: 1 }),
                Some(Entry { key: 3, value: 3 }),
                Some(Entry { key: 5, value: 5 }),
            ]),
            next_leaf: None,
        };

        assert_eq!(n.search(&0), None);
        assert_eq!(n.search(&1), Some(0));
        assert_eq!(n.search(&2), None);
        assert_eq!(n.search(&3), Some(1));
        assert_eq!(n.search(&4), None);
        assert_eq!(n.search(&5), Some(2));
        assert_eq!(n.search(&6), None);
    }

    #[test]
    fn test_leaf_node_remove() {
        let mut n = LeafNode::<u32, u64> {
            len: 3,
            entries: Box::new([
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 1, value: 1 }),
                Some(Entry { key: 2, value: 2 }),
            ]),
            next_leaf: None,
        };

        assert_eq!(n.remove(&1), Some(Entry { key: 1, value: 1 }));
        assert_eq!(n.len, 2);
        assert_eq!(
            *n.entries,
            [
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 2, value: 2 }),
                None,
            ]
        );
        assert_eq!(n.next_leaf, None);
    }

    #[test]
    fn test_leaf_node_merge() {
        let mut n = LeafNode::<u32, u64> {
            len: 2,
            entries: Box::new([
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 1, value: 1 }),
                None,
            ]),
            next_leaf: None,
        };
        let mut m = LeafNode::<u32, u64> {
            len: 1,
            entries: Box::new([Some(Entry { key: 2, value: 2 }), None, None]),
            next_leaf: Some(1),
        };
        n.merge(&mut m);

        assert_eq!(n.len, 3);
        assert_eq!(
            *n.entries,
            [
                Some(Entry { key: 0, value: 0 }),
                Some(Entry { key: 1, value: 1 }),
                Some(Entry { key: 2, value: 2 }),
            ]
        );
        assert_eq!(n.next_leaf, Some(1));

        assert_eq!(m.len, 0);
        assert_eq!(*m.entries, [None, None, None]);
        assert_eq!(m.next_leaf, None);
    }
}
