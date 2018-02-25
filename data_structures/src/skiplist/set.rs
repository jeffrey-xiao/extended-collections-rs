use std::ops::{Add, Sub};
use skiplist::map::{SkipMap, SkipMapIntoIter, SkipMapIter};

/// An ordered set implemented by a skiplist.
///
/// A skiplist is a probabilistic data structure that allows for binary search tree operations by
/// maintaining a linked hierarchy of subsequences. The first subsequence is essentially a sorted
/// linked list of all the elements that it contains. Each successive subsequence contains
/// approximately half the elements of the previous subsequence. Using the sparser subsequences,
/// elements can be skipped and searching, insertion, and deletion of keys can be done in
/// approximately logarithm time.
///
/// # Examples
/// ```
/// use data_structures::skiplist::SkipSet;
///
/// let mut set = SkipSet::new();
/// set.insert(0);
/// set.insert(3);
///
/// assert_eq!(set.size(), 2);
///
/// assert_eq!(set.min(), Some(&0));
/// assert_eq!(set.ceil(&2), Some(&3));
///
/// assert_eq!(set.remove(&0), Some(0));
/// assert_eq!(set.remove(&1), None);
/// ```
pub struct SkipSet<T: Ord> {
    map: SkipMap<T, ()>,
}

impl<T: Ord> SkipSet<T> {
    /// Constructs a new, empty `SkipSet<T>`
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let set: SkipSet<u32> = SkipSet::new();
    /// ```
    pub fn new() -> Self {
        SkipSet {
            map: SkipMap::new(),
        }
    }

    /// Inserts a key into the set. If the key already exists in the set, it will return and
    /// replace the key.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
    /// assert_eq!(set.insert(1), None);
    /// assert_eq!(set.contains(&1), true);
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
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
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
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
    /// set.insert(1);
    /// assert_eq!(set.contains(&0), false);
    /// assert_eq!(set.contains(&1), true);
    /// ```
    pub fn contains(&self, key: &T) -> bool {
        self.map.contains_key(key)
    }

    /// Returns the size of the set.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
    /// set.insert(1);
    /// assert_eq!(set.size(), 1);
    /// ```
    pub fn size(&self) -> usize {
        self.map.size()
    }

    /// Returns `true` if the set is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let set: SkipSet<u32> = SkipSet::new();
    /// assert!(set.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.map.size() == 0
    }

    /// Clears the set, removing all values.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
    /// set.insert(1);
    /// set.insert(2);
    /// set.clear();
    /// assert_eq!(set.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.map.clear();
    }


    /// Returns a key in the set that is greater than or equal to a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
    /// set.insert(1);
    /// assert_eq!(set.ceil(&0), Some(&1));
    /// assert_eq!(set.ceil(&2), None);
    /// ```
    pub fn ceil(&self, key: &T) -> Option<&T> {
        self.map.ceil(key)
    }


    /// Returns a key in the set that is less than or equal to a particular key. Returns `None` if
    /// such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
    /// set.insert(1);
    /// assert_eq!(set.floor(&0), None);
    /// assert_eq!(set.floor(&2), Some(&1));
    /// ```
    pub fn floor(&self, key: &T) -> Option<&T> {
        self.map.floor(key)
    }

    /// Returns the minimum key of the set. Returns `None` if the set is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
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
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
    /// set.insert(1);
    /// set.insert(3);
    /// assert_eq!(set.max(), Some(&3));
    /// ```
    pub fn max(&self) -> Option<&T> {
        self.map.max()
    }

    /// Returns the union of two set. The `+` operator is implemented to take the union of two
    /// sets.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut n = SkipSet::new();
    /// n.insert(1);
    /// n.insert(2);
    ///
    /// let mut m = SkipSet::new();
    /// m.insert(2);
    /// m.insert(3);
    ///
    /// let union = SkipSet::union(n, m);
    /// assert_eq!(
    ///     union.iter().collect::<Vec<&u32>>(),
    ///     vec![&1, &2, &3],
    /// );
    /// ```
    pub fn union(left: Self, right: Self) -> Self {
        SkipSet {
            map: SkipMap::union(left.map, right.map)
        }
    }

    /// Returns the intersection of two sets.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut n = SkipSet::new();
    /// n.insert(1);
    /// n.insert(2);
    ///
    /// let mut m = SkipSet::new();
    /// m.insert(2);
    /// m.insert(3);
    ///
    /// let intersection = SkipSet::intersection(n, m);
    /// assert_eq!(
    ///     intersection.iter().collect::<Vec<&u32>>(),
    ///     vec![&2],
    /// );
    /// ```
    pub fn intersection(left: Self, right: Self) -> Self {
        SkipSet {
            map: SkipMap::intersection(left.map, right.map)
        }
    }

    /// Returns the difference of `left` and `right`. The `-` operator is implemented to take the
    /// difference of two sets.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut n = SkipSet::new();
    /// n.insert(1);
    /// n.insert(2);
    ///
    /// let mut m = SkipSet::new();
    /// m.insert(2);
    /// m.insert(3);
    ///
    /// let difference = SkipSet::difference(n, m);
    /// assert_eq!(
    ///     difference.iter().collect::<Vec<&u32>>(),
    ///     vec![&1],
    /// );
    /// ```
    pub fn difference(left: Self, right: Self) -> Self {
        SkipSet {
            map: SkipMap::difference(left.map, right.map)
        }
    }

    /// Returns the symmetric difference of `left` and `right`. The returned set will contain all
    /// keys that exist in one set, but not both sets.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut n = SkipSet::new();
    /// n.insert(1);
    /// n.insert(2);
    ///
    /// let mut m = SkipSet::new();
    /// m.insert(2);
    /// m.insert(3);
    ///
    /// let symmetric_difference = SkipSet::symmetric_difference(n, m);
    /// assert_eq!(
    ///     symmetric_difference.iter().collect::<Vec<&u32>>(),
    ///     vec![&1, &3],
    /// );
    /// ```
    pub fn symmetric_difference(left: Self, right: Self) -> Self {
        SkipSet {
            map: SkipMap::symmetric_difference(left.map, right.map)
        }
    }

    /// Returns an iterator over the set. The iterator will yield key-value pairs in ascending
    /// order.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipSet;
    ///
    /// let mut set = SkipSet::new();
    /// set.insert(1);
    /// set.insert(3);
    ///
    /// let mut iterator = set.iter();
    /// assert_eq!(iterator.next(), Some(&1));
    /// assert_eq!(iterator.next(), Some(&3));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> SkipSetIter<T> {
        SkipSetIter {
            map_iter: self.map.iter(),
        }
    }
}

impl<T: Ord> IntoIterator for SkipSet<T> {
    type Item = T;
    type IntoIter = SkipSetIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        SkipSetIntoIter {
            map_iter: self.map.into_iter(),
        }
    }
}

impl<'a, T: 'a + Ord> IntoIterator for &'a SkipSet<T> {
    type Item = &'a T;
    type IntoIter = SkipSetIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An owning iterator for `SkipSet<T>`
///
/// This iterator traverses the elements of a set in ascending order and yields owned keys.
pub struct SkipSetIntoIter<T: Ord> {
    map_iter: SkipMapIntoIter<T, ()>,
}

impl<T: Ord> Iterator for SkipSetIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

/// An iterator for `SkipSet<T>`
///
/// This iterator traverses the elements of a set in ascending order and yields immutable
/// references.
pub struct SkipSetIter<'a, T: 'a + Ord> {
    map_iter: SkipMapIter<'a, T, ()>,
}

impl<'a, T: 'a + Ord> Iterator for SkipSetIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

impl<T: Ord> Default for SkipSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Ord> Add for SkipSet<T> {
    type Output = SkipSet<T>;

    fn add(self, other: SkipSet<T>) -> SkipSet<T> {
        Self::union(self, other)
    }
}

impl<T: Ord> Sub for SkipSet<T> {
    type Output = SkipSet<T>;

    fn sub(self, other: SkipSet<T>) -> SkipSet<T> {
        Self::difference(self, other)
    }
}

#[cfg(test)]
mod tests {
    use super::SkipSet;

    #[test]
    fn test_size_empty() {
        let set: SkipSet<u32> = SkipSet::new();
        assert_eq!(set.size(), 0);
    }

    #[test]
    fn test_is_empty() {
        let set: SkipSet<u32> = SkipSet::new();
        assert!(set.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let set: SkipSet<u32> = SkipSet::new();
        assert_eq!(set.min(), None);
        assert_eq!(set.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut set = SkipSet::new();
        set.insert(1);
        assert!(set.contains(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut set = SkipSet::new();
        let ret_1 = set.insert(1);
        let ret_2 = set.insert(1);
        assert_eq!(ret_1, None);
        assert_eq!(ret_2, Some(1));
    }

    #[test]
    fn test_remove() {
        let mut set = SkipSet::new();
        set.insert(1);
        let ret = set.remove(&1);
        assert!(!set.contains(&1));
        assert_eq!(ret, Some(1));
    }

    #[test]
    fn test_min_max() {
        let mut set = SkipSet::new();
        set.insert(1);
        set.insert(3);
        set.insert(5);

        assert_eq!(set.min(), Some(&1));
        assert_eq!(set.max(), Some(&5));
    }

    #[test]
    fn test_floor_ceil() {
        let mut set = SkipSet::new();
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
    fn test_union() {
        let mut n = SkipSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = SkipSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let union = n + m;

        assert_eq!(
            union.iter().collect::<Vec<&u32>>(),
            vec![&1, &2, &3, &4, &5],
        );
        assert_eq!(union.size(), 5);
    }

    #[test]
    fn test_intersection() {
        let mut n = SkipSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = SkipSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let intersection = SkipSet::intersection(n, m);

        assert_eq!(
            intersection.iter().collect::<Vec<&u32>>(),
            vec![&3],
        );
        assert_eq!(intersection.size(), 1);
    }

    #[test]
    fn test_difference() {
        let mut n = SkipSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = SkipSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let difference = n - m;

        assert_eq!(
            difference.iter().collect::<Vec<&u32>>(),
            vec![&1, &2],
        );
        assert_eq!(difference.size(), 2);
    }

    #[test]
    fn test_symmetric_difference() {
        let mut n = SkipSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = SkipSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let symmetric_difference = SkipSet::symmetric_difference(n, m);

        assert_eq!(
            symmetric_difference.iter().collect::<Vec<&u32>>(),
            vec![&1, &2, &4, &5],
        );
        assert_eq!(symmetric_difference.size(), 4);
    }

    #[test]
    fn test_into_iter() {
        let mut set = SkipSet::new();
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
        let mut set = SkipSet::new();
        set.insert(1);
        set.insert(5);
        set.insert(3);

        assert_eq!(
            set.iter().collect::<Vec<&u32>>(),
            vec![&1, &3, &5],
        );
    }
}
