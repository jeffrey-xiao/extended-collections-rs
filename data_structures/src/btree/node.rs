use btree::{INTERNAL_DEGREE, LEAF_DEGREE};
use entry::Entry;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;

#[derive(Serialize, Deserialize, Debug)]
pub struct InternalNode<T: Ord + Clone + Debug, U: Debug> {
    pub len: usize,
    pub keys: [Option<T>; INTERNAL_DEGREE],
    pub pointers: [u64; INTERNAL_DEGREE + 1],
    pub _marker: PhantomData<U>,
}

impl<T: Ord + Clone + Debug, U: Debug> InternalNode<T, U> {
    pub fn insert(&mut self, mut new_key: T, mut new_pointer: u64, is_right: bool) -> Option<(T, Node<T, U>)> {
        let offset = is_right as usize;
        // node has room; can insert
        if self.len < INTERNAL_DEGREE {
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
            let mut split_node_keys = init_array!(Option<T>, INTERNAL_DEGREE, None);
            let mut split_node_pointers = init_array!(u64, INTERNAL_DEGREE + 1, 0);
            let mut index = 0;
            while index < INTERNAL_DEGREE {
                if let Some(ref mut key) = self.keys[index] {
                    if new_key < *key {
                        mem::swap(&mut new_key, key);
                        mem::swap(&mut new_pointer, &mut self.pointers[index + offset]);
                    }
                }
                if index > (INTERNAL_DEGREE + 1) / 2 {
                    mem::swap(&mut self.keys[index], &mut split_node_keys[index - (INTERNAL_DEGREE + 1) / 2 - 1]);
                    mem::swap(&mut self.pointers[index + offset], &mut split_node_pointers[index - (INTERNAL_DEGREE + 1) / 2 - (1 - offset)]);
                }
                index += 1;
            }
            split_node_keys[(INTERNAL_DEGREE - 2) / 2] = Some(new_key);
            split_node_pointers[(INTERNAL_DEGREE - 2) / 2 + offset] = new_pointer;
            let split_key = match mem::replace(&mut self.keys[(INTERNAL_DEGREE + 1) / 2], None) {
                Some(key) => key,
                _ => unreachable!(),
            };
            mem::swap(&mut self.pointers[(INTERNAL_DEGREE + 1) / 2 + 1], &mut split_node_pointers[(1 - offset)]);
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
        assert!(self.len + node.len + 1 <= INTERNAL_DEGREE);
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
    pub entries: [Option<Entry<T, U>>; LEAF_DEGREE],
    pub next_leaf: Option<u64>,
}

impl<T: Ord + Clone + Debug, U: Debug> LeafNode<T, U> {
    pub fn insert(&mut self, mut new_entry: Entry<T, U>) -> Option<(T, Node<T, U>)> {
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
            for index in 0..LEAF_DEGREE {
                if let Some(ref mut entry) = self.entries[index] {
                    if new_entry < *entry {
                        mem::swap(entry, &mut new_entry);
                    }
                }
                if index > LEAF_DEGREE / 2 {
                    mem::swap(&mut self.entries[index], &mut split_node_entries[index - LEAF_DEGREE / 2 - 1]);
                }
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
        assert!(self.len + node.len <= LEAF_DEGREE);
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
    pub fn new_leaf_node() -> Self {
        Node::Leaf(LeafNode {
            len: 0,
            entries: init_array!(Option<Entry<T, U>>, LEAF_DEGREE, None),
            next_leaf: None,
        })
    }

    pub fn new_internal_node() -> Self {
        Node::Internal(InternalNode {
            len: 0,
            keys: init_array!(Option<T>, INTERNAL_DEGREE, None),
            pointers: init_array!(u64, INTERNAL_DEGREE + 1, 0),
            _marker: PhantomData,
        })
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
            keys: [Some(0), Some(1), Some(2)],
            pointers: [0, 1, 2, 3],
            _marker: PhantomData,
        };
        assert_eq!(n.remove_at(1, false), (1, 1));
        assert_eq!(n.len, 2);
        assert_eq!(n.keys, [Some(0), Some(2), None]);
        assert_eq!(n.pointers, [0, 2, 3, 0]);
    }

    #[test]
    fn test_internal_node_remove_at_right() {
        let mut n = InternalNode::<u32, u32> {
            len: 3,
            keys: [Some(0), Some(1), Some(2)],
            pointers: [0, 1, 2, 3],
            _marker: PhantomData,
        };
        assert_eq!(n.remove_at(1, true), (1, 2));
        assert_eq!(n.len, 2);
        assert_eq!(n.keys, [Some(0), Some(2), None]);
        assert_eq!(n.pointers, [0, 1, 3, 0]);
    }
}
