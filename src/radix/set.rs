use radix::map::{RadixMap, RadixMapIntoIter, RadixMapIter};

/// An ordered set implemented using a radix tree.
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
/// use extended_collections::radix::RadixSet;
///
/// let mut set = RadixSet::new();
/// set.insert(b"foo");
/// set.insert(b"foobar");
///
/// assert_eq!(set.len(), 2);
///
/// assert_eq!(set.min(), Some(String::from("foo").into_bytes()));
///
/// assert_eq!(
///     set.get_longest_prefix(b"foob"),
///     vec![String::from("foobar").into_bytes()],
/// );
///
/// assert_eq!(set.remove(b"foo"), Some(String::from("foo").into_bytes()),);
/// ```
pub struct RadixSet {
    map: RadixMap<()>,
}

impl RadixSet {
    /// Constructs a new, empty `RadixSet`.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
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
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// assert_eq!(set.insert(b"foo"), None);
    /// assert!(set.contains(b"foo"));
    /// assert_eq!(set.insert(b"foo"), Some(String::from("foo").into_bytes()),);
    /// ```
    pub fn insert(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.insert(key, ()).map(|pair| pair.0)
    }

    /// Removes a key from the set. If the key exists in the set, it will return the associated
    /// key. Otherwise it will return `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(b"foo");
    /// assert_eq!(set.remove(b"foo"), Some(String::from("foo").into_bytes()),);
    /// assert_eq!(set.remove(b"foobar"), None);
    /// ```
    pub fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.remove(key).map(|pair| pair.0)
    }

    /// Checks if a key exists in the set.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(b"foo");
    /// assert!(set.contains(b"foo"));
    /// assert!(!set.contains(b"foobar"));
    /// ```
    pub fn contains(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }

    /// Returns the number of elements in the set.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(b"foo");
    /// assert_eq!(set.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the set is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
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
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(b"foo");
    /// set.insert(b"foobar");
    /// set.clear();
    /// assert_eq!(set.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Returns all keys that share the longest common prefix with the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(b"foo");
    /// set.insert(b"foobar");
    ///
    /// assert_eq!(
    ///     set.get_longest_prefix(b"foob"),
    ///     vec![String::from("foobar").into_bytes()],
    /// );
    /// ```
    pub fn get_longest_prefix(&self, key: &[u8]) -> Vec<Vec<u8>> {
        self.map.get_longest_prefix(key)
    }

    /// Returns the minimum lexographic key of the set. Returns `None` if the set is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(b"foo");
    /// set.insert(b"foobar");
    /// assert_eq!(set.min(), Some(String::from("foo").into_bytes()));
    /// ```
    pub fn min(&self) -> Option<Vec<u8>> {
        self.map.min()
    }

    /// Returns the maximum lexographic key of the set. Returns `None` if the set is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(b"foo");
    /// set.insert(b"foobar");
    /// assert_eq!(set.max(), Some(String::from("foobar").into_bytes()));
    /// ```
    pub fn max(&self) -> Option<Vec<u8>> {
        self.map.max()
    }

    /// Returns an iterator over the set. The iterator will yield keys in lexographic order.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::radix::RadixSet;
    ///
    /// let mut set = RadixSet::new();
    /// set.insert(b"foo");
    /// set.insert(b"foobar");
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
    type IntoIter = RadixSetIntoIter;
    type Item = Vec<u8>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            map_iter: self.map.into_iter(),
        }
    }
}

impl<'a> IntoIterator for &'a RadixSet {
    type IntoIter = RadixSetIter<'a>;
    type Item = Vec<u8>;

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
    type Item = Vec<u8>;

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
    type Item = Vec<u8>;

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

    fn get_bytes_slice(key: &str) -> &[u8] {
        key.as_bytes()
    }

    fn get_bytes_vec(key: &str) -> Vec<u8> {
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
        assert_eq!(set.insert(get_bytes_slice("aaaa")), None);
        assert_eq!(set.insert(get_bytes_slice("aabb")), None);

        assert_eq!(set.insert(get_bytes_slice("bb")), None);
        assert_eq!(set.insert(get_bytes_slice("bbbb")), None);

        assert_eq!(set.insert(get_bytes_slice("cccc")), None);
        assert_eq!(set.insert(get_bytes_slice("cc")), None);

        assert_eq!(
            set.iter().collect::<Vec<Vec<u8>>>(),
            [
                get_bytes_vec("aaaa"),
                get_bytes_vec("aabb"),
                get_bytes_vec("bb"),
                get_bytes_vec("bbbb"),
                get_bytes_vec("cc"),
                get_bytes_vec("cccc"),
            ]
        );
    }

    #[test]
    fn test_insert_replace() {
        let mut set = RadixSet::new();
        assert_eq!(set.insert(get_bytes_slice("a")), None);
        assert_eq!(set.insert(get_bytes_slice("a")), Some(get_bytes_vec("a")));
    }

    #[test]
    fn test_remove() {
        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("aaaa"));
        set.insert(get_bytes_slice("aabb"));

        set.insert(get_bytes_slice("bbb"));
        set.insert(get_bytes_slice("bbbb"));
        set.insert(get_bytes_slice("bbaa"));

        set.insert(get_bytes_slice("cccc"));
        set.insert(get_bytes_slice("ccdd"));
        set.insert(get_bytes_slice("ccc"));

        assert_eq!(set.remove(get_bytes_slice("non-existent")), None);

        assert_eq!(
            set.remove(get_bytes_slice("aaaa")),
            Some(get_bytes_vec("aaaa"))
        );
        assert_eq!(
            set.remove(get_bytes_slice("aabb")),
            Some(get_bytes_vec("aabb"))
        );

        assert_eq!(
            set.remove(get_bytes_slice("bbb")),
            Some(get_bytes_vec("bbb"))
        );
        assert_eq!(
            set.remove(get_bytes_slice("bbbb")),
            Some(get_bytes_vec("bbbb"))
        );
        assert_eq!(
            set.remove(get_bytes_slice("bbaa")),
            Some(get_bytes_vec("bbaa"))
        );

        assert_eq!(
            set.remove(get_bytes_slice("cccc")),
            Some(get_bytes_vec("cccc"))
        );
        assert_eq!(
            set.remove(get_bytes_slice("ccdd")),
            Some(get_bytes_vec("ccdd"))
        );
        assert_eq!(
            set.remove(get_bytes_slice("ccc")),
            Some(get_bytes_vec("ccc"))
        );

        assert_eq!(set.remove(get_bytes_slice("non-existent")), None);
    }

    #[test]
    fn test_contains_key() {
        let mut set = RadixSet::new();
        assert_eq!(set.insert(get_bytes_slice("a")), None);
        assert!(set.contains(get_bytes_slice("a")));
    }

    #[test]
    fn test_get_longest_prefix() {
        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("aaaa"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes_slice("aaa")),
            vec![get_bytes_vec("aaaa")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("aaaa"));
        set.insert(get_bytes_slice("aaab"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes_slice("aaa")),
            vec![get_bytes_vec("aaaa"), get_bytes_vec("aaab")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("aaa"));
        set.insert(get_bytes_slice("aaaa"));
        set.insert(get_bytes_slice("aaab"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes_slice("aaa")),
            vec![
                get_bytes_vec("aaa"),
                get_bytes_vec("aaaa"),
                get_bytes_vec("aaab"),
            ],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("aa"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes_slice("aaa")),
            vec![get_bytes_vec("aa")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("aaba"));
        set.insert(get_bytes_slice("aabb"));
        assert_eq!(
            set.get_longest_prefix(&get_bytes_slice("aaa")),
            vec![get_bytes_vec("aaba"), get_bytes_vec("aabb")],
        );

        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("b"));
        assert_eq!(set.get_longest_prefix(&get_bytes_slice("aaa")).len(), 0);
    }

    #[test]
    fn test_min_max() {
        let mut set = RadixSet::new();

        set.insert(get_bytes_slice("a"));
        set.insert(get_bytes_slice("aa"));
        set.insert(get_bytes_slice("ba"));
        set.insert(get_bytes_slice("bb"));

        assert_eq!(set.min(), Some(get_bytes_vec("a")));
        assert_eq!(set.max(), Some(get_bytes_vec("bb")));
    }

    #[test]
    fn test_into_iter() {
        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("a"));
        set.insert(get_bytes_slice("ab"));
        set.insert(get_bytes_slice("aa"));

        assert_eq!(
            set.into_iter().collect::<Vec<Vec<u8>>>(),
            vec![get_bytes_vec("a"), get_bytes_vec("aa"), get_bytes_vec("ab")],
        );
    }

    #[test]
    fn test_iter() {
        let mut set = RadixSet::new();
        set.insert(get_bytes_slice("a"));
        set.insert(get_bytes_slice("ab"));
        set.insert(get_bytes_slice("aa"));

        assert_eq!(
            (&set).into_iter().collect::<Vec<Vec<u8>>>(),
            vec![get_bytes_vec("a"), get_bytes_vec("aa"), get_bytes_vec("ab")],
        );
    }
}
