use avl_tree::map::{AvlMap, AvlMapIntoIter, AvlMapIter};

/// An ordered set implemented using a avl_tree.
///
/// An avl tree is a self-balancing binary search tree that maintains the invariant that the
/// heights of two child subtrees of any node differ by at most one.
///
/// # Examples
/// ```
/// use extended_collections::avl_tree::AvlSet;
///
/// let mut set = AvlSet::new();
/// set.insert(0);
/// set.insert(3);
///
/// assert_eq!(set.len(), 2);
///
/// assert_eq!(set.min(), Some(&0));
/// assert_eq!(set.ceil(&2), Some(&3));
///
/// assert_eq!(set.remove(&0), Some(0));
/// assert_eq!(set.remove(&1), None);
/// ```
pub struct AvlSet<T> {
    map: AvlMap<T, ()>,
}

impl<T> AvlSet<T>
where
    T: Ord,
{
    /// Constructs a new, empty `AvlSet<T>`
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let set: AvlSet<u32> = AvlSet::new();
    /// ```
    pub fn new() -> Self {
        AvlSet {
            map: AvlMap::new(),
        }
    }

    /// Inserts a key into the set. If the key already exists in the set, it will return and
    /// replace the key.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// assert_eq!(set.insert(1), None);
    /// assert!(set.contains(&1));
    /// assert_eq!(set.insert(1), Some(1));
    /// ```
    pub fn insert(&mut self, key: T) -> Option<T> {
        self.map.insert(key, ()).map(|pair| pair.0)
    }

    /// Removes a key from the set. If the key exists in the set, it will return the associated
    /// key. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// assert_eq!(set.remove(&1), Some(1));
    /// assert_eq!(set.remove(&1), None);
    /// ```
    pub fn remove(&mut self, key: &T) -> Option<T> {
        self.map.remove(key).map(|pair| pair.0)
    }

    /// Checks if a key exists in the set.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// assert!(!set.contains(&0));
    /// assert!(set.contains(&1));
    /// ```
    pub fn contains(&self, key: &T) -> bool {
        self.map.contains_key(key)
    }

    /// Returns the number of elements in the set.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// assert_eq!(set.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the set is empty.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let set: AvlSet<u32> = AvlSet::new();
    /// assert!(set.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Clears the set, removing all values.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// set.insert(2);
    /// set.clear();
    /// assert_eq!(set.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Returns a key in the set that is less than or equal to a particular key. Returns `None` if
    /// such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// assert_eq!(set.floor(&0), None);
    /// assert_eq!(set.floor(&2), Some(&1));
    /// ```
    pub fn floor(&self, key: &T) -> Option<&T> {
        self.map.floor(key)
    }

    /// Returns a key in the set that is greater than or equal to a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// assert_eq!(set.ceil(&0), Some(&1));
    /// assert_eq!(set.ceil(&2), None);
    /// ```
    pub fn ceil(&self, key: &T) -> Option<&T> {
        self.map.ceil(key)
    }

    /// Returns the minimum key of the set. Returns `None` if the set is empty.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// set.insert(3);
    /// assert_eq!(set.min(), Some(&1));
    /// ```
    pub fn min(&self) -> Option<&T> {
        self.map.min()
    }

    /// Returns the maximum key of the set. Returns `None` if the set is empty.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// set.insert(3);
    /// assert_eq!(set.max(), Some(&3));
    /// ```
    pub fn max(&self) -> Option<&T> {
        self.map.max()
    }

    /// Returns an iterator over the set. The iterator will yield keys using in-order traversal.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::avl_tree::AvlSet;
    ///
    /// let mut set = AvlSet::new();
    /// set.insert(1);
    /// set.insert(3);
    ///
    /// let mut iterator = set.iter();
    /// assert_eq!(iterator.next(), Some(&1));
    /// assert_eq!(iterator.next(), Some(&3));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> AvlSetIter<T> {
        AvlSetIter {
            map_iter: self.map.iter(),
        }
    }
}

impl<T> IntoIterator for AvlSet<T>
where
    T: Ord,
{
    type Item = T;
    type IntoIter = AvlSetIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            map_iter: self.map.into_iter(),
        }
    }
}

impl<'a, T> IntoIterator for &'a AvlSet<T>
where
    T: 'a + Ord,
{
    type Item = &'a T;
    type IntoIter = AvlSetIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An owning iterator for `AvlSet<T>`.
///
/// This iterator traverses the elements of the set in-order and yields owned keys.
pub struct AvlSetIntoIter<T> {
    map_iter: AvlMapIntoIter<T, ()>,
}

impl<T> Iterator for AvlSetIntoIter<T>
where
    T: Ord,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

/// An iterator for `AvlSet<T>`.
///
/// This iterator traverses the elements of the set in-order and yields immutable references.
pub struct AvlSetIter<'a, T>
where
    T: 'a,
{
    map_iter: AvlMapIter<'a, T, ()>,
}

impl<'a, T> Iterator for AvlSetIter<'a, T>
where
    T: 'a + Ord,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

impl<T> Default for AvlSet<T>
where
    T: Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::AvlSet;

    #[test]
    fn test_len_empty() {
        let set: AvlSet<u32> = AvlSet::new();
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let set: AvlSet<u32> = AvlSet::new();
        assert!(set.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let set: AvlSet<u32> = AvlSet::new();
        assert_eq!(set.min(), None);
        assert_eq!(set.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut set = AvlSet::new();
        assert_eq!(set.insert(1), None);
        assert!(set.contains(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut set = AvlSet::new();
        assert_eq!(set.insert(1), None);
        assert_eq!(set.insert(1), Some(1));
    }

    #[test]
    fn test_remove() {
        let mut set = AvlSet::new();
        set.insert(1);
        assert_eq!(set.remove(&1), Some(1));
        assert!(!set.contains(&1));
    }

    #[test]
    fn test_min_max() {
        let mut set = AvlSet::new();
        set.insert(1);
        set.insert(3);
        set.insert(5);

        assert_eq!(set.min(), Some(&1));
        assert_eq!(set.max(), Some(&5));
    }

    #[test]
    fn test_floor_ceil() {
        let mut set = AvlSet::new();
        set.insert(1);
        set.insert(3);
        set.insert(5);

        assert_eq!(set.floor(&0), None);
        assert_eq!(set.floor(&2), Some(&1));
        assert_eq!(set.floor(&4), Some(&3));
        assert_eq!(set.floor(&6), Some(&5));

        assert_eq!(set.ceil(&0), Some(&1));
        assert_eq!(set.ceil(&2), Some(&3));
        assert_eq!(set.ceil(&4), Some(&5));
        assert_eq!(set.ceil(&6), None);
    }

    #[test]
    fn test_into_iter() {
        let mut set = AvlSet::new();
        set.insert(1);
        set.insert(5);
        set.insert(3);

        assert_eq!(
            set.into_iter().collect::<Vec<u32>>(),
            vec![1, 3, 5],
        );
    }

    #[test]
    fn test_iter() {
        let mut set = AvlSet::new();
        set.insert(1);
        set.insert(5);
        set.insert(3);

        assert_eq!(
            set.iter().collect::<Vec<&u32>>(),
            vec![&1, &3, &5],
        );
    }
}
