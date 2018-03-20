use radix::node::Key;
use radix::map::{RadixMap, RadixMapIntoIter, RadixMapIter};

/// An ordered set implemented by a radix tree.
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
/// use data_structures::radix::RadixSet;
///
/// let mut set = RadixSet::new();
/// set.insert(String::from("foo").into_bytes());
/// set.insert(String::from("foobar").into_bytes());
///
/// assert_eq!(set.len(), 2);
///
/// assert_eq!(set.min(), Some(String::from("foo").into_bytes()));
///
/// assert_eq!(
///     set.get_longest_prefix(&String::from("foob").into_bytes()),
///     vec![String::from("foobar").into_bytes()],
/// );
///
/// assert_eq!(
///     set.remove(&String::from("foo").into_bytes()),
///     Some(String::from("foo").into_bytes()),
/// );
/// ```
pub struct RadixSet {
    map: RadixMap<()>,
}

impl RadixSet {
    /// Constructs a new, empty `RadixSet`
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let set = RadixSet::new();
    /// ```
    pub fn new() -> Self {
        RadixSet {
            map: RadixMap::new(),
        }
    }

    /// Inserts a key into the set. If the key already exists in the set, it will return and
    /// replace the old key.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// assert_eq!(set.insert(String::from("foo").into_bytes()), None);
    /// assert!(set.contains(&String::from("foo").into_bytes()));
    /// assert_eq!(
    ///     set.insert(String::from("foo").into_bytes()),
    ///     Some(String::from("foo").into_bytes()),
    /// );
    /// ```
    pub fn insert(&mut self, key: Key) -> Option<Key> {
        self.map.insert(key, ()).map(|pair| pair.0)
    }

    /// Removes a key from the set. If the key exists in the set, it will return the associated
    /// key. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(String::from("foo").into_bytes());
    /// assert_eq!(
    ///     set.remove(&String::from("foo").into_bytes()),
    ///     Some(String::from("foo").into_bytes()),
    /// );
    /// assert_eq!(set.remove(&String::from("foobar").into_bytes()), None);
    /// ```
    pub fn remove(&mut self, key: &Key) -> Option<Key> {
        self.map.remove(key).map(|pair| pair.0)
    }

    /// Checks if a key exists in the set.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(String::from("foo").into_bytes());
    /// assert!(set.contains(&String::from("foo").into_bytes()));
    /// assert!(!set.contains(&String::from("foobar").into_bytes()));
    /// ```
    pub fn contains(&self, key: &Key) -> bool {
        self.map.contains_key(key)
    }

    /// Returns the number of elements in the set.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(String::from("foo").into_bytes());
    /// assert_eq!(set.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the set is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let set = RadixSet::new();
    /// assert!(set.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Clears the set, removing all values.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(String::from("foo").into_bytes());
    /// set.insert(String::from("foobar").into_bytes());
    /// set.clear();
    /// assert_eq!(set.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Returns all keys that share the longest common prefix with the specified key.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(String::from("foo").into_bytes());
    /// set.insert(String::from("foobar").into_bytes());
    ///
    /// assert_eq!(
    ///     set.get_longest_prefix(&String::from("foob").into_bytes()),
    ///     vec![String::from("foobar").into_bytes()],
    /// );
    /// ```
    pub fn get_longest_prefix(&self, key: &Key) -> Vec<Key> {
        self.map.get_longest_prefix(key)
    }

    /// Returns the minimum lexographic key of the set. Returns `None` if the set is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(String::from("foo").into_bytes());
    /// set.insert(String::from("foobar").into_bytes());
    /// assert_eq!(set.min(), Some(String::from("foo").into_bytes()));
    /// ```
    pub fn min(&self) -> Option<Key> {
        self.map.min()
    }

    /// Returns the maximum lexographic key of the set. Returns `None` if the set is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(String::from("foo").into_bytes());
    /// set.insert(String::from("foobar").into_bytes());
    /// assert_eq!(set.max(), Some(String::from("foobar").into_bytes()));
    /// ```
    pub fn max(&self) -> Option<Key> {
        self.map.max()
    }

    /// Returns an iterator over the set. The iterator will yield keys in lexographic order.
    ///
    /// # Examples
    /// ```
    /// use data_structures::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(String::from("foo").into_bytes());
    /// set.insert(String::from("foobar").into_bytes());
    ///
    /// let mut iterator = set.iter();
    /// assert_eq!(iterator.next(), Some(String::from("foo").into_bytes()));
    /// assert_eq!(iterator.next(), Some(String::from("foobar").into_bytes()));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> RadixSetIter {
        RadixSetIter {
            map_iter: self.map.iter(),
        }
    }
}

impl IntoIterator for RadixSet {
    type Item = Key;
    type IntoIter = RadixSetIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            map_iter: self.map.into_iter(),
        }
    }
}

impl<'a> IntoIterator for &'a RadixSet {
    type Item = Key;
    type IntoIter = RadixSetIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An owning iterator for `RadixSet`.
///
/// This iterator traverse the elements of the set in lexographic order and yields owned keys.
pub struct RadixSetIntoIter {
    map_iter: RadixMapIntoIter<()>,
}

impl Iterator for RadixSetIntoIter {
    type Item = Key;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

/// An iterator for `RadixSet`.
///
/// This iterator traverse the elements of the set in lexographic order and yields owned keys.
pub struct RadixSetIter<'a> {
    map_iter: RadixMapIter<'a, ()>,
}

impl<'a> Iterator for RadixSetIter<'a> {
    type Item = Key;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

impl Default for RadixSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::RadixSet;
    use radix::node::Key;

    fn get_bytes(key: &str) -> Key {
        String::from(key).into_bytes()
    }

    #[test]
    fn test_len_empty() {
        let set = RadixSet::new();
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let set = RadixSet::new();
        assert!(set.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let set = RadixSet::new();
        assert_eq!(set.min(), None);
        assert_eq!(set.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut set = RadixSet::new();
        assert_eq!(set.insert(get_bytes("aaaa")), None);
        assert_eq!(set.insert(get_bytes("aabb")), None);

        assert_eq!(set.insert(get_bytes("bb")), None);
        assert_eq!(set.insert(get_bytes("bbbb")), None);

        assert_eq!(set.insert(get_bytes("cccc")), None);
        assert_eq!(set.insert(get_bytes("cc")), None);

        assert_eq!(
            set.iter().collect::<Vec<Key>>(),
            [
                get_bytes("aaaa"),
                get_bytes("aabb"),
                get_bytes("bb"),
                get_bytes("bbbb"),
                get_bytes("cc"),
                get_bytes("cccc"),
            ]
        );
    }

    #[test]
    fn test_insert_replace() {
        let mut set = RadixSet::new();
        assert_eq!(set.insert(get_bytes("a")), None);
        assert_eq!(
            set.insert(get_bytes("a")),
            Some(get_bytes("a")),
        );
    }

    #[test]
    fn test_remove() {
        let mut set = RadixSet::new();
        set.insert(get_bytes("aaaa"));
        set.insert(get_bytes("aabb"));

        set.insert(get_bytes("bb"));
        set.insert(get_bytes("bbbb"));
        set.insert(get_bytes("bbaa"));

        set.insert(get_bytes("cccc"));
        set.insert(get_bytes("ccdd"));
        set.insert(get_bytes("cc"));

        assert_eq!(set.remove(&get_bytes("a")), None);

        assert_eq!(set.remove(&get_bytes("aaaa")), Some(get_bytes("aaaa")));
        assert_eq!(set.remove(&get_bytes("aabb")), Some(get_bytes("aabb")));

        assert_eq!(set.remove(&get_bytes("bb")), Some(get_bytes("bb")));
        assert_eq!(set.remove(&get_bytes("bbbb")), Some(get_bytes("bbbb")));
        assert_eq!(set.remove(&get_bytes("bbaa")), Some(get_bytes("bbaa")));

        assert_eq!(set.remove(&get_bytes("cccc")), Some(get_bytes("cccc")));
        assert_eq!(set.remove(&get_bytes("ccdd")), Some(get_bytes("ccdd")));
        assert_eq!(set.remove(&get_bytes("cc")), Some(get_bytes("cc")));

        assert_eq!(set.remove(&get_bytes("a")), None);
    }

    #[test]
    fn test_contains_key() {
        let mut set = RadixSet::new();
        assert_eq!(set.insert(get_bytes("a")), None);
        assert!(set.contains(&get_bytes("a")));
    }

    #[test]
    fn test_get_longest_prefix() {
        let mut set = RadixSet::new();
        set.insert(get_bytes("aaaa"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aaaa")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes("aaaa"));
        set.insert(get_bytes("aaab"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aaaa"), get_bytes("aaab")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes("aaa"));
        set.insert(get_bytes("aaaa"));
        set.insert(get_bytes("aaab"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aaa"), get_bytes("aaaa"), get_bytes("aaab")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes("aa"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aa")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes("aaba"));
        set.insert(get_bytes("aabb"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes("aaa")),
            vec![get_bytes("aaba"), get_bytes("aabb")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes("b"));
        assert_eq!(set.get_longest_prefix(&get_bytes("aaa")).len(), 0);
    }

    #[test]
    fn test_min_max() {
        let mut set = RadixSet::new();

        set.insert(get_bytes("a"));
        set.insert(get_bytes("aa"));
        set.insert(get_bytes("ba"));
        set.insert(get_bytes("bb"));

        assert_eq!(set.min(), Some(get_bytes("a")));
        assert_eq!(set.max(), Some(get_bytes("bb")));
    }

    #[test]
    fn test_into_iter() {
        let mut set = RadixSet::new();
        set.insert(get_bytes("a"));
        set.insert(get_bytes("ab"));
        set.insert(get_bytes("aa"));

        assert_eq!(
            set.into_iter().collect::<Vec<Key>>(),
            vec![get_bytes("a"), get_bytes("aa"), get_bytes("ab")],
        );
    }

    #[test]
    fn test_iter() {
        let mut set = RadixSet::new();
        set.insert(get_bytes("a"));
        set.insert(get_bytes("ab"));
        set.insert(get_bytes("aa"));

        assert_eq!(
            (&set).into_iter().collect::<Vec<Key>>(),
            vec![get_bytes("a"), get_bytes("aa"), get_bytes("ab")],
        );
    }
}
