use radix::node::{Key, Node};
use radix::tree;
use std::ops::{Index, IndexMut};

/// An ordered map implemented by a radix tree.
///
/// A radix tree is a space optimized trie where nodes are merged with its parent if it is the only
/// child or if it contains a value. This particular implementation of a radix tree accepts keys as
/// byte vectors for flexibility. Additionally, in order to conserve memory usage, the children of
/// a node are represented as a singly linked list rather than an array of pointers. The
/// performance of this radix tree is relatively fast given that the keys are fairly sparse. The
/// children should be represented as a fixed length array of size 256 if the tree is dense for
/// performance.
///
/// # Examples
/// ```
/// use data_structures::radix::RadixMap;
///
/// let mut map = RadixMap::new();
/// map.insert(String::from("foo").into_bytes(), 0);
/// map.insert(String::from("foobar").into_bytes(), 1);
///
/// assert_eq!(map[&String::from("foo").into_bytes()], 0);
/// assert_eq!(map.get(&String::from("baz").into_bytes()), None);
/// assert_eq!(map.len(), 2);
///
/// assert_eq!(map.min(), Some(String::from("foo").into_bytes()));
///
/// assert_eq!(
///     map.get_longest_prefix(&String::from("foob").into_bytes()),
///     vec![String::from("foobar").into_bytes()],
/// );
///
/// map[&String::from("foo").into_bytes()] = 2;
/// assert_eq!(
///     map.remove(&String::from("foo").into_bytes()),
///     Some((String::from("foo").into_bytes(), 2)),
/// );
/// ```
pub struct RadixMap<T> {
    root: tree::Tree<T>,
    len: usize,
}

impl<T> RadixMap<T> {
    /// Constructs a new, empty `RadixMap<T>`
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let map: RadixMap<u32> = RadixMap::new();
    /// ```
    pub fn new() -> Self {
        RadixMap {
            root: Some(Box::new(Node::new(Vec::new(), None))),
            len: 0,
        }
    }

    /// Inserts a key-value pair into the map. If the key already exists in the map, it will return
    /// and replace the old key-value pair.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// assert_eq!(map.insert(String::from("foo").into_bytes(), 1), None);
    /// assert_eq!(map.get(&String::from("foo").into_bytes()), Some(&1));
    /// assert_eq!(
    ///     map.insert(String::from("foo").into_bytes(), 2),
    ///     Some((String::from("foo").into_bytes(), 1)),
    /// );
    /// assert_eq!(map.get(&String::from("foo").into_bytes()), Some(&2));
    /// ```
    pub fn insert(&mut self, key: Key, value: T) -> Option<(Key, T)> {
        self.len += 1;
        let ret = tree::insert(&mut self.root, key, value);
        if ret.is_some() {
            self.len -= 1;
        }
        ret
    }

    /// Removes a key-value pair from the map. If the key exists in the map, it will return the
    /// associated key-value pair. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// assert_eq!(
    ///     map.remove(&String::from("foo").into_bytes()),
    ///     Some((String::from("foo").into_bytes(), 1)),
    /// );
    /// assert_eq!(map.remove(&String::from("foobar").into_bytes()), None);
    /// ```
    pub fn remove(&mut self, key: &Key) -> Option<(Key, T)> {
        tree::remove(&mut self.root, key, 0).and_then(|value| {
            self.len -= 1;
            Some(value)
        })
    }

    /// Checks if a key exists in the map.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// assert!(map.contains_key(&String::from("foo").into_bytes()));
    /// assert!(!map.contains_key(&String::from("foobar").into_bytes()));
    /// ```
    pub fn contains_key(&self, key: &Key) -> bool {
        self.get(key).is_some()
    }

    /// Returns an immutable reference to the value associated with a particular key. It will
    /// return `None` if the key does not exist in the map.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// assert_eq!(map.get(&String::from("foobar").into_bytes()), None);
    /// assert_eq!(map.get(&String::from("foo").into_bytes()), Some(&1));
    /// ```
    pub fn get(&self, key: &Key) -> Option<&T> {
        tree::get(&self.root, key, 0)
    }

    /// Returns a mutable reference to the value associated with a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// *map.get_mut(&String::from("foo").into_bytes()).unwrap() = 2;
    /// assert_eq!(map.get(&String::from("foo").into_bytes()), Some(&2));
    /// ```
    pub fn get_mut(&mut self, key: &Key) -> Option<&mut T> {
        tree::get_mut(&mut self.root, key, 0)
    }

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// assert_eq!(map.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let map: RadixMap<u32> = RadixMap::new();
    /// assert!(map.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears the map, removing all values.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// map.insert(String::from("foobar").into_bytes(), 2);
    /// map.clear();
    /// assert_eq!(map.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.root = Some(Box::new(Node::new(Vec::new(), None)));
        self.len = 0;
    }

    /// Returns all keys that share the longest common prefix with the specified key.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 0);
    /// map.insert(String::from("foobar").into_bytes(), 1);
    ///
    /// assert_eq!(
    ///     map.get_longest_prefix(&String::from("foob").into_bytes()),
    ///     vec![String::from("foobar").into_bytes()],
    /// );
    /// ```
    pub fn get_longest_prefix(&self, key: &Key) -> Vec<Key> {
        let mut curr_key = Vec::new();
        let mut keys = Vec::new();
        tree::get_longest_prefix(&self.root, key, 0, &mut curr_key, &mut keys);
        keys
    }

    /// Returns the minimum lexographic key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// map.insert(String::from("foobar").into_bytes(), 3);
    /// assert_eq!(map.min(), Some(String::from("foo").into_bytes()));
    /// ```
    pub fn min(&self) -> Option<Key> {
        tree::min(&self.root, Vec::new())
    }

    /// Returns the maximum lexographic key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// map.insert(String::from("foobar").into_bytes(), 3);
    /// assert_eq!(map.max(), Some(String::from("foobar").into_bytes()));
    /// ```
    pub fn max(&self) -> Option<Key> {
        tree::max(&self.root, Vec::new())
    }

    /// Returns an iterator over the map. The iterator will yield key-value pairs in lexographic
    /// order.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// map.insert(String::from("foobar").into_bytes(), 2);
    ///
    /// let mut iterator = map.iter();
    /// assert_eq!(iterator.next(), Some((String::from("foo").into_bytes(), &1)));
    /// assert_eq!(iterator.next(), Some((String::from("foobar").into_bytes(), &2)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> RadixMapIter<T> {
        RadixMapIter {
            prefix: Vec::new(),
            current: &self.root,
            stack: Vec::new(),
        }
    }

    /// Returns a mutable iterator over the map. The iterator will yield key-value pairs in
    /// lexographic order.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert(String::from("foo").into_bytes(), 1);
    /// map.insert(String::from("foobar").into_bytes(), 2);
    ///
    /// for (key, value) in &mut map {
    ///   *value += 1;
    /// }
    ///
    /// let mut iterator = map.iter_mut();
    /// assert_eq!(iterator.next(), Some((String::from("foo").into_bytes(), &mut 2)));
    /// assert_eq!(iterator.next(), Some((String::from("foobar").into_bytes(), &mut 3)));
    /// assert_eq!(iterator.next(), None);
    /// ```
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

impl<T> Default for RadixMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T> Index<&'a Key> for RadixMap<T> {
    type Output = T;
    fn index(&self, key: &Key) -> &Self::Output {
        self.get(key).expect("Key does not exist.")
    }
}

impl<'a, T> IndexMut<&'a Key> for RadixMap<T> {
    fn index_mut(&mut self, key: &Key) -> &mut Self::Output {
        self.get_mut(key).expect("Key does not exist.")
    }
}

#[cfg(test)]
mod tests {
    use super::RadixMap;
    use radix::node::Key;

    fn get_bytes(key: &str) -> Key {
        String::from(key).into_bytes()
    }

    #[test]
    fn test_len_empty() {
        let map: RadixMap<u32> = RadixMap::new();
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let map: RadixMap<u32> = RadixMap::new();
        assert!(map.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let map: RadixMap<u32> = RadixMap::new();
        assert_eq!(map.min(), None);
        assert_eq!(map.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut map = RadixMap::new();
        assert_eq!(map.insert(get_bytes("aaaa"), 0), None);
        assert_eq!(map.insert(get_bytes("aabb"), 1), None);

        assert_eq!(map.insert(get_bytes("bb"), 2), None);
        assert_eq!(map.insert(get_bytes("bbbb"), 3), None);

        assert_eq!(map.insert(get_bytes("cccc"), 5), None);
        assert_eq!(map.insert(get_bytes("cc"), 4), None);

        assert_eq!(
            map.iter().collect::<Vec<(Key, &u32)>>(),
            [
                (get_bytes("aaaa"), &0),
                (get_bytes("aabb"), &1),
                (get_bytes("bb"), &2),
                (get_bytes("bbbb"), &3),
                (get_bytes("cc"), &4),
                (get_bytes("cccc"), &5),
            ]
        );
    }

    #[test]
    fn test_insert_replace() {
        let mut map = RadixMap::new();
        assert_eq!(map.insert(get_bytes("a"), 0), None);
        assert_eq!(
            map.insert(get_bytes("a"), 1),
            Some((get_bytes("a"), 0)),
        );
        assert_eq!(map.get(&get_bytes("a")), Some(&1));
    }

    #[test]
    fn test_remove() {
        let mut map = RadixMap::new();
        map.insert(get_bytes("aaaa"), 0);
        map.insert(get_bytes("aabb"), 1);

        map.insert(get_bytes("bb"), 2);
        map.insert(get_bytes("bbbb"), 4);
        map.insert(get_bytes("bbaa"), 3);

        map.insert(get_bytes("cccc"), 6);
        map.insert(get_bytes("ccdd"), 7);
        map.insert(get_bytes("cc"), 5);

        assert_eq!(map.remove(&get_bytes("a")), None);

        assert_eq!(map.remove(&get_bytes("aaaa")), Some((get_bytes("aaaa"), 0)));
        assert_eq!(map.remove(&get_bytes("aabb")), Some((get_bytes("aabb"), 1)));

        assert_eq!(map.remove(&get_bytes("bb")), Some((get_bytes("bb"), 2)));
        assert_eq!(map.remove(&get_bytes("bbbb")), Some((get_bytes("bbbb"), 4)));
        assert_eq!(map.remove(&get_bytes("bbaa")), Some((get_bytes("bbaa"), 3)));

        assert_eq!(map.remove(&get_bytes("cccc")), Some((get_bytes("cccc"), 6)));
        assert_eq!(map.remove(&get_bytes("ccdd")), Some((get_bytes("ccdd"), 7)));
        assert_eq!(map.remove(&get_bytes("cc")), Some((get_bytes("cc"), 5)));

        assert_eq!(map.remove(&get_bytes("a")), None);
    }

    #[test]
    fn test_contains_key() {
        let mut map = RadixMap::new();
        assert_eq!(map.insert(get_bytes("a"), 0), None);
        assert!(map.contains_key(&get_bytes("a")));
    }

    #[test]
    fn test_get_mut() {
        let mut map = RadixMap::new();
        map.insert(get_bytes("a"), 1);
        {
            let value = map.get_mut(&get_bytes("a"));
            *value.unwrap() = 3;
        }
        assert_eq!(map.get(&get_bytes("a")), Some(&3));
    }

    #[test]
    fn test_get_none() {
        let mut map = RadixMap::new();
        map.insert(get_bytes("aa"), 1);

        assert_eq!(map.get(&get_bytes("a")), None);
        assert_eq!(map.get(&get_bytes("b")), None);
        assert_eq!(map.get_mut(&get_bytes("a")), None);
        assert_eq!(map.get_mut(&get_bytes("b")), None);
    }

    #[test]
    fn test_get_longest_prefix() {
        let mut map = RadixMap::new();
        map.insert(get_bytes("aaaa"), 0);
        assert_eq!(
            map.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aaaa")],
        );

        let mut map = RadixMap::new();
        map.insert(get_bytes("aaaa"), 0);
        map.insert(get_bytes("aaab"), 1);
        assert_eq!(
            map.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aaaa"), get_bytes("aaab")],
        );

        let mut map = RadixMap::new();
        map.insert(get_bytes("aaa"), 0);
        map.insert(get_bytes("aaaa"), 1);
        map.insert(get_bytes("aaab"), 2);
        assert_eq!(
            map.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aaa"), get_bytes("aaaa"), get_bytes("aaab")],
        );

        let mut map = RadixMap::new();
        map.insert(get_bytes("aa"), 0);
        assert_eq!(
            map.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aa")],
        );

        let mut map = RadixMap::new();
        map.insert(get_bytes("aaba"), 0);
        map.insert(get_bytes("aabb"), 1);
        assert_eq!(
            map.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aaba"), get_bytes("aabb")],
        );

        let mut map = RadixMap::new();
        map.insert(get_bytes("b"), 0);
        assert_eq!(map.get_longest_prefix(&get_bytes("aaa")).len(), 0);
    }

    #[test]
    fn test_min_max() {
        let mut map = RadixMap::new();

        map.insert(get_bytes("a"), 0);
        map.insert(get_bytes("aa"), 1);
        map.insert(get_bytes("ba"), 3);
        map.insert(get_bytes("bb"), 4);

        assert_eq!(map.min(), Some(get_bytes("a")));
        assert_eq!(map.max(), Some(get_bytes("bb")));
    }

    #[test]
    fn test_into_iter() {
        let mut map = RadixMap::new();
        map.insert(get_bytes("a"), 2);
        map.insert(get_bytes("ab"), 6);
        map.insert(get_bytes("aa"), 4);

        assert_eq!(
            map.into_iter().collect::<Vec<(Key, u32)>>(),
            vec![(get_bytes("a"), 2), (get_bytes("aa"), 4), (get_bytes("ab"), 6)],
        );
    }

    #[test]
    fn test_iter() {
        let mut map = RadixMap::new();
        map.insert(get_bytes("a"), 2);
        map.insert(get_bytes("ab"), 6);
        map.insert(get_bytes("aa"), 4);

        assert_eq!(
            (&map).into_iter().collect::<Vec<(Key, &u32)>>(),
            vec![(get_bytes("a"), &2), (get_bytes("aa"), &4), (get_bytes("ab"), &6)],
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut map = RadixMap::new();
        map.insert(get_bytes("a"), 2);
        map.insert(get_bytes("ab"), 6);
        map.insert(get_bytes("aa"), 4);

        for (_, value) in &mut map {
            *value += 1;
        }

        assert_eq!(
            (&map).into_iter().collect::<Vec<(Key, &u32)>>(),
            vec![(get_bytes("a"), &3), (get_bytes("aa"), &5), (get_bytes("ab"), &7)],
        );
    }
}
