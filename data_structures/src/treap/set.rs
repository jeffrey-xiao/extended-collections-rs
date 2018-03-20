use std::ops::{Add, Sub};
use treap::map::{TreapMap, TreapMapIntoIter, TreapMapIter};

/// An ordered set implemented by a treap.
///
/// A treap is a tree that satisfies both the binary search tree property and a heap property. Each
/// node has a key and a priority. The key of any node is greater than all keys in its
/// left subtree and less than all keys occuring in its right subtree. The priority of a node is
/// greater than the priority of all nodes in its subtrees. By randomly generating priorities, the
/// expected height of the tree is proportional to the logarithm of the number of keys.
///
/// # Examples
/// ```
/// use data_structures::treap::TreapSet;
///
/// let mut set = TreapSet::new();
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
pub struct TreapSet<T: Ord> {
    map: TreapMap<T, ()>,
}

impl<T: Ord> TreapSet<T> {
    /// Constructs a new, empty `TreapSet<T>`
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let set: TreapSet<u32> = TreapSet::new();
    /// ```
    pub fn new() -> Self {
        TreapSet {
            map: TreapMap::new(),
        }
    }

    /// Inserts a key into the set. If the key already exists in the set, it will return and
    /// replace the key.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
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
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
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
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
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
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
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
    /// use data_structures::treap::TreapSet;
    ///
    /// let set: TreapSet<u32> = TreapSet::new();
    /// assert!(set.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Clears the set, removing all values.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
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
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
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
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
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
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
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
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
    /// set.insert(1);
    /// set.insert(3);
    /// assert_eq!(set.max(), Some(&3));
    /// ```
    pub fn max(&self) -> Option<&T> {
        self.map.max()
    }

    /// Splits the set and returns the right part of the set. If `inclusive` is true, then the set
    /// will retain the given key if it exists. Otherwise, the right part of the set will contain
    /// the key if it exists.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
    /// set.insert(1);
    /// set.insert(2);
    /// set.insert(3);
    ///
    /// let split = set.split_off(&2, true);
    /// assert!(set.contains(&1));
    /// assert!(set.contains(&2));
    /// assert!(split.contains(&3));
    /// ```
    pub fn split_off(&mut self, key: &T, inclusive: bool) -> Self {
        TreapSet { map: self.map.split_off(key, inclusive) }
    }

    /// Returns the union of two set. The `+` operator is implemented to take the union of two
    /// sets.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut n = TreapSet::new();
    /// n.insert(1);
    /// n.insert(2);
    ///
    /// let mut m = TreapSet::new();
    /// m.insert(2);
    /// m.insert(3);
    ///
    /// let union = TreapSet::union(n, m);
    /// assert_eq!(
    ///     union.iter().collect::<Vec<&u32>>(),
    ///     vec![&1, &2, &3],
    /// );
    /// ```
    pub fn union(left: Self, right: Self) -> Self {
        TreapSet {
            map: TreapMap::union(left.map, right.map)
        }
    }

    /// Returns the intersection of two sets.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut n = TreapSet::new();
    /// n.insert(1);
    /// n.insert(2);
    ///
    /// let mut m = TreapSet::new();
    /// m.insert(2);
    /// m.insert(3);
    ///
    /// let intersection = TreapSet::intersection(n, m);
    /// assert_eq!(
    ///     intersection.iter().collect::<Vec<&u32>>(),
    ///     vec![&2],
    /// );
    /// ```
    pub fn intersection(left: Self, right: Self) -> Self {
        TreapSet {
            map: TreapMap::intersection(left.map, right.map)
        }
    }

    /// Returns the difference of `left` and `right`. The `-` operator is implemented to take the
    /// difference of two sets.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut n = TreapSet::new();
    /// n.insert(1);
    /// n.insert(2);
    ///
    /// let mut m = TreapSet::new();
    /// m.insert(2);
    /// m.insert(3);
    ///
    /// let difference = TreapSet::difference(n, m);
    /// assert_eq!(
    ///     difference.iter().collect::<Vec<&u32>>(),
    ///     vec![&1],
    /// );
    /// ```
    pub fn difference(left: Self, right: Self) -> Self {
        TreapSet {
            map: TreapMap::difference(left.map, right.map)
        }
    }

    /// Returns the symmetric difference of `left` and `right`. The returned set will contain all
    /// keys that exist in one set, but not both sets.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut n = TreapSet::new();
    /// n.insert(1);
    /// n.insert(2);
    ///
    /// let mut m = TreapSet::new();
    /// m.insert(2);
    /// m.insert(3);
    ///
    /// let symmetric_difference = TreapSet::symmetric_difference(n, m);
    /// assert_eq!(
    ///     symmetric_difference.iter().collect::<Vec<&u32>>(),
    ///     vec![&1, &3],
    /// );
    /// ```
    pub fn symmetric_difference(left: Self, right: Self) -> Self {
        TreapSet {
            map: TreapMap::symmetric_difference(left.map, right.map)
        }
    }

    /// Returns an iterator over the set. The iterator will yield keys using in-order traversal.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut set = TreapSet::new();
    /// set.insert(1);
    /// set.insert(3);
    ///
    /// let mut iterator = set.iter();
    /// assert_eq!(iterator.next(), Some(&1));
    /// assert_eq!(iterator.next(), Some(&3));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> TreapSetIter<T> {
        TreapSetIter {
            map_iter: self.map.iter(),
        }
    }
}

impl<T: Ord> IntoIterator for TreapSet<T> {
    type Item = T;
    type IntoIter = TreapSetIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            map_iter: self.map.into_iter(),
        }
    }
}

impl<'a, T: 'a + Ord> IntoIterator for &'a TreapSet<T> {
    type Item = &'a T;
    type IntoIter = TreapSetIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An owning iterator for `TreapSet<T>`.
///
/// This iterator traverses the elements of the set in-order and yields owned keys.
pub struct TreapSetIntoIter<T: Ord> {
    map_iter: TreapMapIntoIter<T, ()>,
}

impl<T: Ord> Iterator for TreapSetIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

/// An iterator for `TreapSet<T>`.
///
/// This iterator traverses the elements of the set in-order and yields immutable references.
pub struct TreapSetIter<'a, T: 'a + Ord> {
    map_iter: TreapMapIter<'a, T, ()>,
}

impl<'a, T: 'a + Ord> Iterator for TreapSetIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

impl<T: Ord> Default for TreapSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Ord> Add for TreapSet<T> {
    type Output = TreapSet<T>;

    fn add(self, other: TreapSet<T>) -> TreapSet<T> {
        Self::union(self, other)
    }
}

impl<T: Ord> Sub for TreapSet<T> {
    type Output = TreapSet<T>;

    fn sub(self, other: TreapSet<T>) -> TreapSet<T> {
        Self::difference(self, other)
    }
}

#[cfg(test)]
mod tests {
    use super::TreapSet;

    #[test]
    fn test_len_empty() {
        let set: TreapSet<u32> = TreapSet::new();
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let set: TreapSet<u32> = TreapSet::new();
        assert!(set.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let set: TreapSet<u32> = TreapSet::new();
        assert_eq!(set.min(), None);
        assert_eq!(set.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut set = TreapSet::new();
        assert_eq!(set.insert(1), None);
        assert!(set.contains(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut set = TreapSet::new();
        assert_eq!(set.insert(1), None);
        assert_eq!(set.insert(1), Some(1));
    }

    #[test]
    fn test_remove() {
        let mut set = TreapSet::new();
        set.insert(1);
        assert_eq!(set.remove(&1), Some(1));
        assert!(!set.contains(&1));
    }

    #[test]
    fn test_min_max() {
        let mut set = TreapSet::new();
        set.insert(1);
        set.insert(3);
        set.insert(5);

        assert_eq!(set.min(), Some(&1));
        assert_eq!(set.max(), Some(&5));
    }

    #[test]
    fn test_floor_ceil() {
        let mut set = TreapSet::new();
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
    fn test_split_off_inclusive() {
        let mut set = TreapSet::new();
        set.insert(1);
        set.insert(2);
        set.insert(3);

        let split = set.split_off(&2, true);
        assert_eq!(
            set.iter().collect::<Vec<&u32>>(),
            vec![&1, &2],
        );
        assert_eq!(
            split.iter().collect::<Vec<&u32>>(),
            vec![&3],
        );
    }

    #[test]
    fn test_split_off_not_inclusive() {
        let mut set = TreapSet::new();
        set.insert(1);
        set.insert(2);
        set.insert(3);

        let split = set.split_off(&2, false);
        assert_eq!(
            set.iter().collect::<Vec<&u32>>(),
            vec![&1],
        );
        assert_eq!(
            split.iter().collect::<Vec<&u32>>(),
            vec![&2, &3],
        );
    }

    #[test]
    fn test_union() {
        let mut n = TreapSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = TreapSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let union = n + m;

        assert_eq!(
            union.iter().collect::<Vec<&u32>>(),
            vec![&1, &2, &3, &4, &5],
        );
        assert_eq!(union.len(), 5);
    }

    #[test]
    fn test_intersection() {
        let mut n = TreapSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = TreapSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let intersection = TreapSet::intersection(n, m);

        assert_eq!(
            intersection.iter().collect::<Vec<&u32>>(),
            vec![&3],
        );
        assert_eq!(intersection.len(), 1);
    }

    #[test]
    fn test_difference() {
        let mut n = TreapSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = TreapSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let difference = n - m;

        assert_eq!(
            difference.iter().collect::<Vec<&u32>>(),
            vec![&1, &2],
        );
        assert_eq!(difference.len(), 2);
    }

    #[test]
    fn test_symmetric_difference() {
        let mut n = TreapSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = TreapSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let symmetric_difference = TreapSet::symmetric_difference(n, m);

        assert_eq!(
            symmetric_difference.iter().collect::<Vec<&u32>>(),
            vec![&1, &2, &4, &5],
        );
        assert_eq!(symmetric_difference.len(), 4);
    }

    #[test]
    fn test_into_iter() {
        let mut set = TreapSet::new();
        set.insert(1);
        set.insert(5);
        set.insert(3);

        assert_eq!(
            set.into_iter().collect::<Vec<u32>>(),
            vec![1, 3, 5]
        );
    }

    #[test]
    fn test_iter() {
        let mut set = TreapSet::new();
        set.insert(1);
        set.insert(5);
        set.insert(3);

        assert_eq!(
            set.iter().collect::<Vec<&u32>>(),
            vec![&1, &3, &5],
        );
    }
}
