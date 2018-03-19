use radix::node::{Key, Node};
use radix::tree;

#[derive(Debug)]
pub struct RadixMap<T> {
    root: tree::Tree<T>,
    len: usize,
}

impl<T> RadixMap<T> {
    pub fn new() -> Self {
        Self {
            root: Some(Box::new(Node::new(Vec::new(), None))),
            len: 0,
        }
    }

    pub fn insert(&mut self, key: Key, value: T) -> Option<T> {
        self.len += 1;
        tree::insert(&mut self.root, key, value).and_then(|value| {
            self.len -= 1;
            Some(value)
        })
    }

    pub fn remove(&mut self, key: &Key) -> Option<(Key, T)> {
        tree::remove(&mut self.root, key, 0).and_then(|value| {
            self.len -= 1;
            Some(value)
        })
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        self.get(key).is_some()
    }

    pub fn get(&self, key: &Key) -> Option<&T> {
        tree::get(&self.root, key, 0)
    }

    pub fn get_mut(&mut self, key: &Key) -> Option<&mut T> {
        tree::get_mut(&mut self.root, key, 0)
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

    pub fn iter_mut(&mut self) -> RadixMapIterMut<T> {
        RadixMapIterMut {
            prefix: Vec::new(),
            current: self.root.as_mut().map(|node| &mut **node),
            stack: Vec::new(),
        }
    }
}

impl<T> IntoIterator for RadixMap<T> {
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

impl<'a, T: 'a> IntoIterator for &'a RadixMap<T> {
    type Item = (Key, &'a T);
    type IntoIter = RadixMapIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: 'a> IntoIterator for &'a mut RadixMap<T> {
    type Item = (Key, &'a mut T);
    type IntoIter = RadixMapIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// An owning iterator for `RadixMap<T>`.
///
/// This iterator traverse the elements of the map in lexographic order and yields owned entries.
pub struct RadixMapIntoIter<T> {
    prefix: Key,
    current: tree::Tree<T>,
    stack: Vec<(tree::Tree<T>, usize)>,
}

impl<T> Iterator for RadixMapIntoIter<T> {
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
pub struct RadixMapIter<'a, T: 'a> {
    prefix: Key,
    current: &'a tree::Tree<T>,
    stack: Vec<(&'a tree::Tree<T>, usize)>,
}

impl<'a, T: 'a> Iterator for RadixMapIter<'a, T> {
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

/// A mutable iterator for `RadixMap<T>`.
///
/// This iterator traverse the elements of the map in lexographic order and yields mutable
/// references.
pub struct RadixMapIterMut<'a, T: 'a> {
    prefix: Key,
    current: Option<&'a mut Node<T>>,
    stack: Vec<(&'a mut tree::Tree<T>, usize)>,
}

impl<'a, T: 'a> Iterator for RadixMapIterMut<'a, T> {
    type Item = (Key, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while let Some(node) = self.current.take() {
                let Node { ref key, ref mut value, ref mut next, ref mut child } = *node;
                let key_len = key.len();
                self.prefix.extend_from_slice(&mut key.as_slice());
                self.current = child.as_mut().map(|node| &mut **node);
                self.stack.push((next, key_len));
                if value.is_some() {
                    return value.as_mut().map(|value| (self.prefix.clone(), value));
                }
            }
            match self.stack.pop() {
                Some((next_tree, key_len)) => {
                    let new_len = self.prefix.len() - key_len;
                    self.prefix.split_off(new_len);
                    self.current = next_tree.as_mut().map(|node| &mut **node);
                },
                None => return None,
            }
        }
    }
}
