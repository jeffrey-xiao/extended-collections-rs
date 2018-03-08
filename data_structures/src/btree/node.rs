use entry::Entry;
use std::cmp;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;

const U64_SIZE: usize = mem::size_of::<u64>() as usize;
const OPT_U64_SIZE: usize = mem::size_of::<Option<u64>>() as usize;
pub const BLOCK_SIZE: usize = 4096;

#[derive(Serialize, Deserialize, Debug)]
pub struct InternalNode<T: Ord + Clone + Debug, U: Debug> {
    pub len: usize,
    pub keys: Box<[Option<T>]>,
    pub pointers: Box<[u64]>,
    pub _marker: PhantomData<U>,
}

impl<T: Ord + Clone + Debug, U: Debug> InternalNode<T, U> {

    // 1) a usize is encoded as u64 (8 bytes)
    // 2) a boxed slice is encoded as a tuple of u64 (8 bytes) and the items
    #[inline]
    pub fn get_degree() -> usize {
        (BLOCK_SIZE - U64_SIZE * 3 - mem::size_of::<PhantomData<U>>()) / (mem::size_of::<T>() + U64_SIZE)
    }

    #[inline]
    pub fn get_max_size(degree: usize) -> usize {
        U64_SIZE * 4 + mem::size_of::<PhantomData<U>>() + degree * (mem::size_of::<Entry<T, U>>() + U64_SIZE)
    }

    pub fn new(degree: usize) -> Self {
        InternalNode {
            len: 0,
            keys: init_array!(Option<T>, degree, None),
            pointers: init_array!(u64, degree + 1, 0),
            _marker: PhantomData,
        }
    }

    pub fn insert(&mut self, mut new_key: T, mut new_pointer: u64, is_right: bool) -> Option<(T, Node<T, U>)> {
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
                    mem::swap(&mut self.keys[index], &mut split_node.keys[index - (internal_degree + 1) / 2 - 1]);
                    mem::swap(&mut self.pointers[index + offset], &mut split_node.pointers[index - (internal_degree + 1) / 2 - (1 - offset)]);
                }
                index += 1;
            }
            split_node.keys[(internal_degree - 2) / 2] = Some(new_key);
            split_node.pointers[(internal_degree - 2) / 2 + offset] = new_pointer;
            let split_key = match mem::replace(&mut self.keys[(internal_degree + 1) / 2], None) {
                Some(key) => key,
                _ => unreachable!(),
            };
            mem::swap(&mut self.pointers[(internal_degree + 1) / 2 + 1], &mut split_node.pointers[(1 - offset)]);
            split_node.len = internal_degree / 2;
            self.len = (internal_degree + 1) / 2;

            Some((split_key, Node::Internal(split_node)))
        }
    }

    pub fn remove_at(&mut self, remove_index: usize, is_right: bool) -> (T, u64) {
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

        let ret_key = {
            match mem::replace(&mut self.keys[self.len], None) {
                Some(key) => key,
                _ => unreachable!(),
            }
        };

        (ret_key, ret_pointer)
    }

    pub fn merge(&mut self, split_key: T, node: &mut InternalNode<T, U>) {
        assert!(self.len + node.len + 1 <= self.keys.len());
        self.keys[self.len] = Some(split_key);
        for index in 0..node.len {
            self.keys[self.len + index + 1] = node.keys[index].take();
            self.pointers[self.len + index + 1] = node.pointers[index];
        }
        self.len += node.len + 1;
        self.pointers[self.len] = node.pointers[node.len];
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LeafNode<T: Ord + Clone + Debug, U: Debug> {
    pub len: usize,
    pub entries: Box<[Option<Entry<T, U>>]>,
    pub next_leaf: Option<u64>,
}

impl<T: Ord + Clone + Debug, U: Debug> LeafNode<T, U> {

    // 1) a usize is encoded as u64 (8 bytes)
    // 2) a boxed slice is encoded as a tuple of u64 (8 bytes) and the items
    #[inline]
    pub fn get_degree() -> usize {
        (BLOCK_SIZE - U64_SIZE * 2 - OPT_U64_SIZE) / mem::size_of::<Entry<T, U>>()
    }

    #[inline]
    pub fn get_max_size(degree: usize) -> usize {
        U64_SIZE * 2 + OPT_U64_SIZE + degree * mem::size_of::<Entry<T, U>>()
    }

    pub fn new(degree: usize) -> Self {
        LeafNode {
            len: 0,
            entries: init_array!(Option<Entry<T, U>>, degree, None),
            next_leaf: None,
        }
    }

    pub fn insert(&mut self, mut new_entry: Entry<T, U>) -> Option<(T, Node<T, U>)> {
        let leaf_degree = self.entries.len();
        // node has room; can insert
        if self.len < leaf_degree {
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
            let mut split_node = LeafNode::new(leaf_degree);
            for index in 0..leaf_degree {
                if let Some(ref mut entry) = self.entries[index] {
                    if new_entry < *entry {
                        mem::swap(entry, &mut new_entry);
                    }
                }
                if index > leaf_degree / 2 {
                    mem::swap(&mut self.entries[index], &mut split_node.entries[index - leaf_degree / 2 - 1]);
                }
            }
            split_node.entries[(leaf_degree - 1) / 2] = Some(new_entry);
            let split_key = match split_node.entries[0] {
                Some(ref mut entry) => entry.key.clone(),
                _ => unreachable!(),
            };
            let split_node = Node::Leaf(LeafNode {
                len: (self.len + 1) / 2,
                entries: split_node.entries,
                next_leaf: self.next_leaf,
            });
            self.len = (self.len + 2) / 2;
            Some((split_key, split_node))
        }
    }

    pub fn remove_at(&mut self, remove_index: usize) -> Entry<T, U> {
        assert!(remove_index < self.len);
        self.len -= 1;
        for index in remove_index..self.len {
            self.entries.swap(index, index + 1);
        }

        match self.entries[self.len].take() {
            Some(entry) => entry,
            _ => unreachable!(),
        }
    }

    pub fn remove(&mut self, key: &T) -> Option<Entry<T, U>> {
        for index in 0..self.len {
            if let Some(entry) = self.entries[index].take() {
                if *key == entry.key && index + 1 < self.len {
                    self.entries.swap(index, index + 1);
                }
            }
        }
        self.len -= 1;
        self.entries[self.len].take()
    }

    pub fn merge(&mut self, node: &mut LeafNode<T, U>) {
        assert!(self.len + node.len <= self.entries.len());
        self.next_leaf = node.next_leaf;
        for index in 0..node.len {
            self.entries[self.len + index] = node.entries[index].take();
        }
        self.len += node.len;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Node<T: Ord + Clone + Debug, U: Debug> {
    Internal(InternalNode<T, U>),
    Leaf(LeafNode<T, U>),
    Free(Option<u64>),
}

impl<T: Ord + Clone + Debug, U: Debug> Node<T, U> {
    #[inline]
    pub fn get_max_size(leaf_degree: usize, internal_degree: usize) -> usize {
        cmp::max(
            LeafNode::<T, U>::get_max_size(leaf_degree),
            InternalNode::<T, U>::get_max_size(internal_degree),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;
    use super::InternalNode;

    #[test]
    fn test_internal_node_remove_at_left() {
        let mut n = InternalNode::<u32, u32> {
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
        let mut n = InternalNode::<u32, u32> {
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
}
