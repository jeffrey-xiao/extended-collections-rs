use radix::node::Node;
use radix::tree;
use std::ops::{Index, IndexMut};

/// An ordered map implemented using a radix tree.
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
///
/// ```
/// use extended_collections::radix::RadixMap;
///
/// let mut map = RadixMap::new();
/// map.insert("foo".as_bytes(), 0);
/// map.insert("foobar".as_bytes(), 1);
///
/// assert_eq!(map["foo".as_bytes()], 0);
/// assert_eq!(map.get("baz".as_bytes()), None);
/// assert_eq!(map.len(), 2);
///
/// assert_eq!(map.min(), Some(String::from("foo").into_bytes()));
///
/// assert_eq!(
///     map.get_longest_prefix("foob".as_bytes()),
///     vec![String::from("foobar").into_bytes()],
/// );
///
/// map["foo".as_bytes()] = 2;
/// assert_eq!(
///     map.remove("foo".as_bytes()),
///     Some((String::from("foo").into_bytes(), 2)),
/// );
/// ```
pub struct RadixMap<T> {
    root: tree::Tree<T>,
    len: usize,
}

impl<T> RadixMap<T> {
    /// Constructs a new, empty `RadixMap<T>`.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
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
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// assert_eq!(map.insert("foo".as_bytes(), 1), None);
    /// assert_eq!(map.get("foo".as_bytes()), Some(&1));
    /// assert_eq!(
    ///     map.insert("foo".as_bytes(), 2),
    ///     Some((String::from("foo").into_bytes(), 1)),
    /// );
    /// assert_eq!(map.get("foo".as_bytes()), Some(&2));
    /// ```
    pub fn insert(&mut self, key: &[u8], value: T) -> Option<(Vec<u8>, T)> {
        self.len += 1;
        let ret = tree::insert(&mut self.root, key, value).map(|value| (key.to_vec(), value));
        if ret.is_some() {
            self.len -= 1;
        }
        ret
    }

    /// Removes a key-value pair from the map. If the key exists in the map, it will return the
    /// associated key-value pair. Otherwise it will return `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// assert_eq!(
    ///     map.remove("foo".as_bytes()),
    ///     Some((String::from("foo").into_bytes(), 1)),
    /// );
    /// assert_eq!(map.remove("foobar".as_bytes()), None);
    /// ```
    pub fn remove(&mut self, key: &[u8]) -> Option<(Vec<u8>, T)> {
        tree::remove(&mut self.root, key, 0).and_then(|value| {
            self.len -= 1;
            Some(value)
        })
    }

    /// Checks if a key exists in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// assert!(map.contains_key("foo".as_bytes()));
    /// assert!(!map.contains_key("foobar".as_bytes()));
    /// ```
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Returns an immutable reference to the value associated with a particular key. It will
    /// return `None` if the key does not exist in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// assert_eq!(map.get("foobar".as_bytes()), None);
    /// assert_eq!(map.get("foo".as_bytes()), Some(&1));
    /// ```
    pub fn get(&self, key: &[u8]) -> Option<&T> {
        tree::get(&self.root, key, 0)
    }

    /// Returns a mutable reference to the value associated with a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// *map.get_mut("foo".as_bytes()).unwrap() = 2;
    /// assert_eq!(map.get("foo".as_bytes()), Some(&2));
    /// ```
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut T> {
        tree::get_mut(&mut self.root, key, 0)
    }

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// assert_eq!(map.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
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
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// map.insert("foobar".as_bytes(), 2);
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
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 0);
    /// map.insert("foobar".as_bytes(), 1);
    ///
    /// assert_eq!(
    ///     map.get_longest_prefix("foob".as_bytes()),
    ///     vec!["foobar".as_bytes()],
    /// );
    /// ```
    pub fn get_longest_prefix(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        tree::get_longest_prefix(&self.root, key, 0, Vec::new(), &mut keys);
        keys
    }

    /// Returns the minimum lexographic key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// map.insert("foobar".as_bytes(), 3);
    /// assert_eq!(map.min(), Some(String::from("foo").into_bytes()));
    /// ```
    pub fn min(&self) -> Option<Vec<u8>> {
        tree::min(&self.root, Vec::new())
    }

    /// Returns the maximum lexographic key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// map.insert("foobar".as_bytes(), 3);
    /// assert_eq!(map.max(), Some(String::from("foobar").into_bytes()));
    /// ```
    pub fn max(&self) -> Option<Vec<u8>> {
        tree::max(&self.root, Vec::new())
    }

    /// Returns an iterator over the map. The iterator will yield key-value pairs in lexographic
    /// order.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// map.insert("foobar".as_bytes(), 2);
    ///
    /// let mut iterator = map.iter();
    /// assert_eq!(
    ///     iterator.next(),
    ///     Some((String::from("foo").into_bytes(), &1)),
    /// );
    /// assert_eq!(
    ///     iterator.next(),
    ///     Some((String::from("foobar").into_bytes(), &2)),
    /// );
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
    ///
    /// ```
    /// use extended_collections::radix::RadixMap;
    ///
    /// let mut map = RadixMap::new();
    /// map.insert("foo".as_bytes(), 1);
    /// map.insert("foobar".as_bytes(), 2);
    ///
    /// for (key, value) in &mut map {
    ///     *value += 1;
    /// }
    ///
    /// let mut iterator = map.iter_mut();
    /// assert_eq!(
    ///     iterator.next(),
    ///     Some((String::from("foo").into_bytes(), &mut 2)),
    /// );
    /// assert_eq!(
    ///     iterator.next(),
    ///     Some((String::from("foobar").into_bytes(), &mut 3)),
    /// );
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
    type IntoIter = RadixMapIntoIter<T>;
    type Item = (Vec<u8>, T);

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            prefix: Vec::new(),
            current: self.root,
            stack: Vec::new(),
        }
    }
}

impl<'a, T> IntoIterator for &'a RadixMap<T>
where
    T: 'a,
{
    type IntoIter = RadixMapIter<'a, T>;
    type Item = (Vec<u8>, &'a T);

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut RadixMap<T>
where
    T: 'a,
{
    type IntoIter = RadixMapIterMut<'a, T>;
    type Item = (Vec<u8>, &'a mut T);

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// An owning iterator for `RadixMap<T>`.
///
/// This iterator traverse the elements of the map in lexographic order and yields owned entries.
pub struct RadixMapIntoIter<T> {
    prefix: Vec<u8>,
    current: tree::Tree<T>,
    stack: Vec<(tree::Tree<T>, usize)>,
}

impl<T> Iterator for RadixMapIntoIter<T> {
    type Item = (Vec<u8>, T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while let Some(node) = self.current.take() {
                let unboxed_node = *node;
                let Node {
                    mut key,
                    value,
                    next,
                    mut child,
                } = unboxed_node;
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
pub struct RadixMapIter<'a, T>
where
    T: 'a,
{
    prefix: Vec<u8>,
    current: &'a tree::Tree<T>,
    stack: Vec<(&'a tree::Tree<T>, usize)>,
}

impl<'a, T> Iterator for RadixMapIter<'a, T>
where
    T: 'a,
{
    type Item = (Vec<u8>, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while let Some(ref node) = self.current {
                let Node {
                    ref key,
                    ref value,
                    ref next,
                    ref child,
                } = **node;
                let key_len = key.len();
                self.prefix.extend_from_slice(key.as_slice());
                self.current = child;
                self.stack.push((next, key_len));
                if let Some(ref value) = value {
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
pub struct RadixMapIterMut<'a, T>
where
    T: 'a,
{
    prefix: Vec<u8>,
    current: Option<&'a mut Node<T>>,
    stack: Vec<(&'a mut tree::Tree<T>, usize)>,
}

impl<'a, T> Iterator for RadixMapIterMut<'a, T>
where
    T: 'a,
{
    type Item = (Vec<u8>, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            while let Some(node) = self.current.take() {
                let Node {
                    ref key,
                    ref mut value,
                    ref mut next,
                    ref mut child,
                } = node;
                let key_len = key.len();

                self.prefix.extend_from_slice(key.as_slice());
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

impl<'a, T> Index<&'a [u8]> for RadixMap<T> {
    type Output = T;

    fn index(&self, key: &[u8]) -> &Self::Output {
        self.get(key).expect("Error: key does not exist.")
    }
}

impl<'a, T> IndexMut<&'a [u8]> for RadixMap<T> {
    fn index_mut(&mut self, key: &[u8]) -> &mut Self::Output {
        self.get_mut(key).expect("Error: key does not exist.")
    }
}

#[cfg(test)]
mod tests {
    use super::RadixMap;

    fn get_bytes_vec(key: &str) -> Vec<u8> {
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
        assert_eq!(map.insert("aaaa".as_bytes(), 0), None);
        assert_eq!(map.insert("aabb".as_bytes(), 1), None);

        assert_eq!(map.insert("bb".as_bytes(), 2), None);
        assert_eq!(map.insert("bbbb".as_bytes(), 3), None);

        assert_eq!(map.insert("cccc".as_bytes(), 5), None);
        assert_eq!(map.insert("cc".as_bytes(), 4), None);

        assert_eq!(
            map.iter().collect::<Vec<(Vec<u8>, &u32)>>(),
            [
                (get_bytes_vec("aaaa"), &0),
                (get_bytes_vec("aabb"), &1),
                (get_bytes_vec("bb"), &2),
                (get_bytes_vec("bbbb"), &3),
                (get_bytes_vec("cc"), &4),
                (get_bytes_vec("cccc"), &5),
            ]
        );
    }

    #[test]
    fn test_insert_replace() {
        let mut map = RadixMap::new();
        assert_eq!(map.insert("a".as_bytes(), 0), None);
        assert_eq!(map.insert("a".as_bytes(), 1), Some((get_bytes_vec("a"), 0)));
        assert_eq!(map.get("a".as_bytes()), Some(&1));
    }

    #[test]
    fn test_remove() {
        let mut map = RadixMap::new();
        map.insert("aaaa".as_bytes(), 0);
        map.insert("aabb".as_bytes(), 1);

        map.insert("bbb".as_bytes(), 2);
        map.insert("bbbb".as_bytes(), 4);
        map.insert("bbaa".as_bytes(), 3);

        map.insert("cccc".as_bytes(), 6);
        map.insert("ccdd".as_bytes(), 7);
        map.insert("ccc".as_bytes(), 5);

        assert_eq!(map.remove("non-existent".as_bytes()), None);

        assert_eq!(
            map.remove("aaaa".as_bytes()),
            Some((get_bytes_vec("aaaa"), 0)),
        );
        assert_eq!(
            map.remove("aabb".as_bytes()),
            Some((get_bytes_vec("aabb"), 1)),
        );

        assert_eq!(
            map.remove("bbb".as_bytes()),
            Some((get_bytes_vec("bbb"), 2))
        );
        assert_eq!(
            map.remove("bbbb".as_bytes()),
            Some((get_bytes_vec("bbbb"), 4)),
        );
        assert_eq!(
            map.remove("bbaa".as_bytes()),
            Some((get_bytes_vec("bbaa"), 3)),
        );

        assert_eq!(
            map.remove("cccc".as_bytes()),
            Some((get_bytes_vec("cccc"), 6)),
        );
        assert_eq!(
            map.remove("ccdd".as_bytes()),
            Some((get_bytes_vec("ccdd"), 7)),
        );
        assert_eq!(
            map.remove("ccc".as_bytes()),
            Some((get_bytes_vec("ccc"), 5))
        );

        assert_eq!(map.remove("non-existent".as_bytes()), None);
    }

    #[test]
    fn test_contains_key() {
        let mut map = RadixMap::new();
        assert_eq!(map.insert("a".as_bytes(), 0), None);
        assert!(map.contains_key("a".as_bytes()));
    }

    #[test]
    fn test_get_mut() {
        let mut map = RadixMap::new();
        map.insert("a".as_bytes(), 1);
        {
            let value = map.get_mut("a".as_bytes());
            *value.unwrap() = 3;
        }
        assert_eq!(map.get("a".as_bytes()), Some(&3));
    }

    #[test]
    fn test_get_none() {
        let mut map = RadixMap::new();
        map.insert("aa".as_bytes(), 1);

        assert_eq!(map.get("a".as_bytes()), None);
        assert_eq!(map.get("b".as_bytes()), None);
        assert_eq!(map.get_mut("a".as_bytes()), None);
        assert_eq!(map.get_mut("b".as_bytes()), None);
    }

    #[test]
    fn test_get_longest_prefix() {
        let mut map = RadixMap::new();
        map.insert("aaaa".as_bytes(), 0);
        assert_eq!(
            map.get_longest_prefix("aaa".as_bytes()),
            vec![get_bytes_vec("aaaa")],
        );

        let mut map = RadixMap::new();
        map.insert("aaaa".as_bytes(), 0);
        map.insert("aaab".as_bytes(), 1);
        assert_eq!(
            map.get_longest_prefix("aaa".as_bytes()),
            vec![get_bytes_vec("aaaa"), get_bytes_vec("aaab")],
        );

        let mut map = RadixMap::new();
        map.insert("aaa".as_bytes(), 0);
        map.insert("aaaa".as_bytes(), 1);
        map.insert("aaab".as_bytes(), 2);
        assert_eq!(
            map.get_longest_prefix("aaa".as_bytes()),
            vec![
                get_bytes_vec("aaa"),
                get_bytes_vec("aaaa"),
                get_bytes_vec("aaab"),
            ],
        );

        let mut map = RadixMap::new();
        map.insert("aa".as_bytes(), 0);
        assert_eq!(
            map.get_longest_prefix("aaa".as_bytes()),
            vec![get_bytes_vec("aa")],
        );

        let mut map = RadixMap::new();
        map.insert("aaba".as_bytes(), 0);
        map.insert("aabb".as_bytes(), 1);
        assert_eq!(
            map.get_longest_prefix("aaa".as_bytes()),
            vec![get_bytes_vec("aaba"), get_bytes_vec("aabb")],
        );

        let mut map = RadixMap::new();
        map.insert("b".as_bytes(), 0);
        assert_eq!(map.get_longest_prefix("aaa".as_bytes()).len(), 0);
    }

    #[test]
    fn test_min_max() {
        let mut map = RadixMap::new();

        map.insert("a".as_bytes(), 0);
        map.insert("aa".as_bytes(), 1);
        map.insert("ba".as_bytes(), 3);
        map.insert("bb".as_bytes(), 4);

        assert_eq!(map.min(), Some(get_bytes_vec("a")));
        assert_eq!(map.max(), Some(get_bytes_vec("bb")));
    }

    #[test]
    fn test_into_iter() {
        let mut map = RadixMap::new();
        map.insert("a".as_bytes(), 2);
        map.insert("ab".as_bytes(), 6);
        map.insert("aa".as_bytes(), 4);

        assert_eq!(
            map.into_iter().collect::<Vec<(Vec<u8>, u32)>>(),
            vec![
                (get_bytes_vec("a"), 2),
                (get_bytes_vec("aa"), 4),
                (get_bytes_vec("ab"), 6),
            ],
        );
    }

    #[test]
    fn test_iter() {
        let mut map = RadixMap::new();
        map.insert("a".as_bytes(), 2);
        map.insert("ab".as_bytes(), 6);
        map.insert("aa".as_bytes(), 4);

        assert_eq!(
            (&map).into_iter().collect::<Vec<(Vec<u8>, &u32)>>(),
            vec![
                (get_bytes_vec("a"), &2),
                (get_bytes_vec("aa"), &4),
                (get_bytes_vec("ab"), &6),
            ],
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut map = RadixMap::new();
        map.insert("a".as_bytes(), 2);
        map.insert("ab".as_bytes(), 6);
        map.insert("aa".as_bytes(), 4);

        for (_, value) in &mut map {
            *value += 1;
        }

        assert_eq!(
            (&map).into_iter().collect::<Vec<(Vec<u8>, &u32)>>(),
            vec![
                (get_bytes_vec("a"), &3),
                (get_bytes_vec("aa"), &5),
                (get_bytes_vec("ab"), &7),
            ],
        );
    }
}
