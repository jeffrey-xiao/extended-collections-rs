use std::mem;
use std::fmt::Debug;
use std::cmp::Ordering;

pub type Key = Vec<u8>;

#[derive(Debug)]
pub struct Node<T> {
    key: Key,
    value: Option<T>,
    next: Tree<T>,
    child: Tree<T>,
}

impl<T> Node<T> {
    pub fn new(key: Key, value: Option<T>) -> Self {
        Self {
            key,
            value: value,
            next: None,
            child: None,
        }
    }

    pub fn contains(&self, byte: u8) -> bool {
        self.get(byte).is_some()
    }

    pub fn get(&self, byte: u8) -> &Tree<T> {
        fn get_inner<T>(tree: &Tree<T>, byte: u8) -> &Tree<T> {
            match *tree {
                Some(ref node) if node.key[0] != byte => get_inner(&node.next, byte),
                _ => tree,
            }
        }
        get_inner(&self.child, byte)
    }

    pub fn get_mut(&mut self, byte: u8) -> &mut Tree<T> {
        fn get_mut_inner<T>(tree: &mut Tree<T>, byte: u8) -> &mut Tree<T> {
            match *tree {
                Some(ref mut node) if node.key[0] != byte => get_mut_inner(&mut node.next, byte),
                _ => tree,
            }
        }
        get_mut_inner(&mut self.child, byte)
    }

    pub fn insert_child(&mut self, child: Node<T>) {
        fn insert_inner<T>(tree: &mut Tree<T>, mut new_node: Box<Node<T>>) {
            match *tree {
                Some(ref mut node) => {
                    if node.key[0] > new_node.key[0] {
                        mem::swap(node, &mut new_node);
                        new_node.next = node.next.take();
                        node.next = Some(new_node);
                    } else {
                        insert_inner(&mut node.next, new_node);
                    }
                },
                None => *tree = Some(new_node),
            }
        }
        insert_inner(&mut self.child, Box::new(child));
    }

    pub fn merge(&mut self) {
        if let Some(mut child_node) = self.child.take() {
            if self.value.is_none() && child_node.next.is_none() {
                self.key.append(&mut child_node.key);
                self.value = child_node.value.take();
                self.child = child_node.child.take();
            } else {
                self.child = Some(child_node);
            }
        }
    }
}

pub type Tree<T> = Option<Box<Node<T>>>;

pub fn insert<T>(tree: &mut Tree<T>, mut key: Key, value: T) -> Option<T> {
    let node = match *tree {
        Some(ref mut node) => node,
        _ => unreachable!(),
    };
    let split_index = node.key.iter().zip(key.iter()).position(|pair| pair.0 != pair.1);
    match split_index {
        Some(split_index) => {
            let mut split_key = node.key.split_off(split_index);
            mem::swap(&mut split_key, &mut node.key);
            let mut split = mem::replace(&mut **node, Node::new(split_key, None));
            let mut child = Node::new(key.split_off(split_index), Some(value));

            node.next = split.next.take();
            node.insert_child(split);
            node.insert_child(child);
            None
        },
        None => match node.key.len().cmp(&key.len()) {
            Ordering::Less => {
                key = key.split_off(node.key.len());
                let byte = key[0];
                if node.contains(byte) {
                    insert(node.get_mut(byte), key, value)
                } else {
                    node.insert_child(Node::new(key, Some(value)));
                    None
                }
            },
            Ordering::Greater => {
                let mut split_key = node.key.split_off(key.len());
                mem::swap(&mut split_key, &mut node.key);
                let mut split = mem::replace(&mut **node, Node::new(split_key, None));
                node.next = split.next.take();
                node.value = Some(value);
                node.insert_child(split);
                None
            },
            Ordering::Equal => mem::replace(&mut node.value, Some(value)),
        }
    }
}

pub fn remove<T: Debug>(tree: &mut Tree<T>, key: &Key, mut index: usize) -> Option<(Key, T)> {
    let mut next_tree = None;
    let ret;
    {
        let node = match *tree {
            Some(ref mut node) => node,
            None => return None,
        };
        let split_index = node.key.iter().zip(key[index..].iter()).position(|pair| pair.0 != pair.1);
        match split_index {
            Some(_) => return None,
            None => match node.key.len().cmp(&(key.len() - index)) {
                Ordering::Less => {
                    index += node.key.len();
                    let byte = key[index];
                    ret = remove(node.get_mut(byte), key, index);
                    node.merge();
                    if node.value.is_none() && node.child.is_none() {
                        next_tree = Some(node.next.take());
                    }
                },
                Ordering::Greater => return None,
                Ordering::Equal => {
                    ret = node.value.take().map(|value| (key.clone(), value));
                    node.merge();
                    if node.value.is_none() && node.child.is_none() {
                        next_tree = Some(node.next.take());
                    }
                }
            }
        }
    }
    if let Some(next_tree) = next_tree {
        *tree = next_tree;
    }
    ret
}

pub fn get<'a, T: Debug>(tree: &'a Tree<T>, key: &Key, mut index: usize) -> Option<&'a T> {
    let node = match *tree {
        Some(ref node) => node,
        None => return None,
    };
    let split_index = node.key.iter().zip(key[index..].iter()).position(|pair| pair.0 != pair.1);
    match split_index {
        Some(_) => None,
        None => match node.key.len().cmp(&(key.len() - index)) {
            Ordering::Less => {
                index += node.key.len();
                get(node.get(key[index]), key, index)
            },
            Ordering::Greater => None,
            Ordering::Equal => node.value.as_ref(),
        }
    }
}

pub fn get_mut<'a, T>(tree: &'a mut Tree<T>, key: &Key, mut index: usize) -> Option<&'a mut T> {
    let node = match *tree {
        Some(ref mut node) => node,
        None => return None,
    };
    let split_index = node.key.iter().zip(key[index..].iter()).position(|pair| pair.0 != pair.1);
    match split_index {
        Some(_) => None,
        None => match node.key.len().cmp(&(key.len() - index)) {
            Ordering::Less => {
                index += node.key.len();
                get_mut(node.get_mut(key[index]), key, index)
            },
            Ordering::Greater => None,
            Ordering::Equal => node.value.as_mut(),
        }
    }
}

#[derive(Debug)]
pub struct RadixMap<T: Debug> {
    root: Tree<T>,
    len: usize,
}

impl<T: Debug> RadixMap<T> {
    pub fn new() -> Self {
        Self {
            root: Some(Box::new(Node::new(Vec::new(), None))),
            len: 0,
        }
    }

    pub fn insert(&mut self, key: Key, value: T) -> Option<T> {
        self.len += 1;
        insert(&mut self.root, key, value).and_then(|value| {
            self.len -= 1;
            Some(value)
        })
    }

    pub fn remove(&mut self, key: &Key) -> Option<(Key, T)> {
        remove(&mut self.root, key, 0).and_then(|value| {
            self.len -= 1;
            Some(value)
        })
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        self.get(key).is_some()
    }

    pub fn get(&self, key: &Key) -> Option<&T> {
        get(&self.root, key, 0)
    }

    pub fn get_mut(&mut self, key: &Key) -> Option<&mut T> {
        get_mut(&mut self.root, key, 0)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        self.root = Some(Box::new(Node::new(Vec::new(), None)));
    }

    pub fn ceil(&self, key: &Key) -> Option<Key> {
        None
    }

    pub fn min(&self) -> Option<Key> {
        None
    }

    pub fn max(&self) -> Option<Key> {
        None
    }

    pub fn iter(&self) -> RadixMapIter<T> {
        RadixMapIter {
            prefix: Vec::new(),
            current: &self.root,
            stack: Vec::new(),
        }
    }
}

impl<T: Debug> IntoIterator for RadixMap<T> {
    type Item = (Key, T);
    type IntoIter = RadixMapIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            prefix: Vec::new(),
            current: self.root,
            stack: Vec::new(),
        }
    }
}

impl<'a, T: 'a + Debug> IntoIterator for &'a RadixMap<T> {
    type Item = (Key, &'a T);
    type IntoIter = RadixMapIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An owning iterator for `RadixMap<T>`.
///
/// This iterator traverse the elements of the map in lexographic order and yields owned entries.
pub struct RadixMapIntoIter<T: Debug> {
    prefix: Key,
    current: Tree<T>,
    stack: Vec<(Tree<T>, usize)>,
}

impl<T: Debug> Iterator for RadixMapIntoIter<T> {
    type Item = (Key, T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while let Some(node) = self.current.take() {
                let unboxed_node = *node;
                let Node { mut key, value, next, mut child } = unboxed_node;
                let key_len = key.len();
                self.prefix.append(&mut key);
                self.current = child.take();
                self.stack.push((next, key_len));
                if let Some(value) = value {
                    return Some((self.prefix.clone(), value));
                }
            }
            match self.stack.pop() {
                Some((next_tree, key_len)) => {
                    let new_len = self.prefix.len() - key_len;
                    self.prefix.split_off(new_len);
                    self.current = next_tree;
                },
                None => return None,
            }
        }
    }
}

/// An iterator for `RadixMap<T>`.
///
/// This iterator traverse the elements of the map in lexographic order and yields immutable
/// references.
pub struct RadixMapIter<'a, T: 'a + Debug> {
    prefix: Key,
    current: &'a Tree<T>,
    stack: Vec<(&'a Tree<T>, usize)>,
}

impl<'a, T: 'a + Debug> Iterator for RadixMapIter<'a, T> {
    type Item = (Key, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while let Some(ref node) = *self.current {
                let Node { ref key, ref value, ref next, ref child } = **node;
                let key_len = key.len();
                self.prefix.extend_from_slice(&mut key.as_slice());
                self.current = &child;
                self.stack.push((next, key_len));
                if let Some(ref value) = *value {
                    return Some((self.prefix.clone(), value));
                }
            }
            match self.stack.pop() {
                Some((next_tree, key_len)) => {
                    let new_len = self.prefix.len() - key_len;
                    self.prefix.split_off(new_len);
                    self.current = next_tree;
                },
                None => return None,
            }
        }
    }
}
