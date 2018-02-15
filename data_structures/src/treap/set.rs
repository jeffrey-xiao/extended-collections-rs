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
/// let mut t = TreapSet::new();
/// t.insert(0);
/// t.insert(3);
///
/// assert_eq!(t.size(), 2);
///
/// assert_eq!(t.min(), Some(&0));
/// assert_eq!(t.ceil(&2), Some(&3));
///
/// assert_eq!(t.remove(&0), Some(0));
/// assert_eq!(t.remove(&1), None);
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
    /// let mut t: TreapSet<u32> = TreapSet::new();
    /// ```
    pub fn new() -> Self {
        TreapSet {
            map: TreapMap::new(),
        }
    }

    /// Inserts a key into the treap. If the key already exists in the treap, it will
    /// return and replace the key.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// assert_eq!(t.insert(1), None);
    /// assert_eq!(t.contains(&1), true);
    /// assert_eq!(t.insert(1), Some(1));
    /// ```
    pub fn insert(&mut self, key: T) -> Option<T> {
        self.map.insert(key, ()).map(|pair| pair.0)
    }

    /// Removes a key from the treap. If the key exists in the treap, it will return
    /// the associated key. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// t.insert(1);
    /// assert_eq!(t.remove(&1), Some(1));
    /// assert_eq!(t.remove(&1), None);
    /// ```
    pub fn remove(&mut self, key: &T) -> Option<T> {
        self.map.remove(key).map(|pair| pair.0)
    }

    /// Checks if a key exists in the treap.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// t.insert(1);
    /// assert_eq!(t.contains(&0), false);
    /// assert_eq!(t.contains(&1), true);
    /// ```
    pub fn contains(&self, key: &T) -> bool {
        self.map.contains(key)
    }

    /// Returns the size of the treap.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// t.insert(1);
    /// assert_eq!(t.size(), 1);
    /// ```
    pub fn size(&self) -> usize {
        self.map.size()
    }


    /// Returns a key in the treap that is greater than or equal to a particular key. Returns
    /// `None` if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// t.insert(1);
    /// assert_eq!(t.ceil(&0), Some(&1));
    /// assert_eq!(t.ceil(&2), None);
    /// ```
    pub fn ceil(&self, key: &T) -> Option<&T> {
        self.map.ceil(key)
    }


    /// Returns a key in the treap that is less than or equal to a particular key. Returns
    /// `None` if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// t.insert(1);
    /// assert_eq!(t.floor(&0), None);
    /// assert_eq!(t.floor(&2), Some(&1));
    /// ```
    pub fn floor(&self, key: &T) -> Option<&T> {
        self.map.floor(key)
    }

    /// Returns the minimum key of the treap. Returns `None` if the treap is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// t.insert(1);
    /// t.insert(3);
    /// assert_eq!(t.min(), Some(&1));
    /// ```
    pub fn min(&self) -> Option<&T> {
        self.map.min()
    }

    /// Returns the maximum key of the treap. Returns `None` if the treap is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// t.insert(1);
    /// t.insert(3);
    /// assert_eq!(t.max(), Some(&3));
    /// ```
    pub fn max(&self) -> Option<&T> {
        self.map.max()
    }

    /// Returns the union of two treaps. The `+` operator is implemented to take the union of two
    /// treaps.
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

    /// Returns the intersection of two treaps.
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
    /// let inter = TreapSet::inter(n, m);
    /// assert_eq!(
    ///     inter.iter().collect::<Vec<&u32>>(),
    ///     vec![&2],
    /// );
    /// ```
    pub fn inter(left: Self, right: Self) -> Self {
        TreapSet {
            map: TreapMap::inter(left.map, right.map)
        }
    }

    /// Returns `left` subtracted by `right`. The `-` operator is implemented to take the
    /// difference of two treaps.
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
    /// let subtract = TreapSet::subtract(n, m);
    /// assert_eq!(
    ///     subtract.iter().collect::<Vec<&u32>>(),
    ///     vec![&1],
    /// );
    /// ```
    pub fn subtract(left: Self, right: Self) -> Self {
        TreapSet {
            map: TreapMap::sub(left.map, right.map)
        }
    }

    /// Returns an iterator over the treap. The iterator will yield key-value pairs using in-order
    /// traversal.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapSet;
    ///
    /// let mut t = TreapSet::new();
    /// t.insert(1);
    /// t.insert(3);
    ///
    /// let mut iterator = t.iter();
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
        TreapSetIntoIter {
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

/// An owning iterator for `TreapSet<T>`
///
/// This iterator traverses the elements of a treap in-order and yields owned keys.
pub struct TreapSetIntoIter<T: Ord> {
    map_iter: TreapMapIntoIter<T, ()>,
}

impl<T: Ord> Iterator for TreapSetIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter.next().map(|pair| pair.0)
    }
}

/// An iterator for `TreapSet<T>`
///
/// This iterator traverses the elements of a treap in-order and yields immutable references.
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
        Self::subtract(self, other)
    }
}

#[cfg(test)]
mod tests {
    use super::TreapSet;

    #[test]
    fn test_size_empty() {
        let tree: TreapSet<u32> = TreapSet::new();
        assert_eq!(tree.size(), 0);
    }

    #[test]
    fn test_min_max_empty() {
        let tree: TreapSet<u32> = TreapSet::new();
        assert_eq!(tree.min(), None);
        assert_eq!(tree.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut tree = TreapSet::new();
        tree.insert(1);
        assert!(tree.contains(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut tree = TreapSet::new();
        let ret_1 = tree.insert(1);
        let ret_2 = tree.insert(1);
        assert_eq!(ret_1, None);
        assert_eq!(ret_2, Some(1));
    }

    #[test]
    fn test_remove() {
        let mut tree = TreapSet::new();
        tree.insert(1);
        let ret = tree.remove(&1);
        assert!(!tree.contains(&1));
        assert_eq!(ret, Some(1));
    }

    #[test]
    fn test_min_max() {
        let mut tree = TreapSet::new();
        tree.insert(1);
        tree.insert(3);
        tree.insert(5);

        assert_eq!(tree.min(), Some(&1));
        assert_eq!(tree.max(), Some(&5));
    }

    #[test]
    fn test_floor_ceil() {
        let mut tree = TreapSet::new();
        tree.insert(1);
        tree.insert(3);
        tree.insert(5);

        assert_eq!(tree.floor(&0), None);
        assert_eq!(tree.floor(&2), Some(&1));
        assert_eq!(tree.floor(&4), Some(&3));
        assert_eq!(tree.floor(&6), Some(&5));

        assert_eq!(tree.ceil(&0), Some(&1));
        assert_eq!(tree.ceil(&2), Some(&3));
        assert_eq!(tree.ceil(&4), Some(&5));
        assert_eq!(tree.ceil(&6), None);
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
        assert_eq!(union.size(), 5);
    }

    #[test]
    fn test_inter() {
        let mut n = TreapSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = TreapSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let inter = TreapSet::inter(n, m);

        assert_eq!(
            inter.iter().collect::<Vec<&u32>>(),
            vec![&3],
        );
        assert_eq!(inter.size(), 1);
    }

    #[test]
    fn test_subtract() {
        let mut n = TreapSet::new();
        n.insert(1);
        n.insert(2);
        n.insert(3);

        let mut m = TreapSet::new();
        m.insert(3);
        m.insert(4);
        m.insert(5);

        let sub = n - m;

        assert_eq!(
            sub.iter().collect::<Vec<&u32>>(),
            vec![&1, &2],
        );
        assert_eq!(sub.size(), 2);
    }

    #[test]
    fn test_into_iter() {
        let mut tree = TreapSet::new();
        tree.insert(1);
        tree.insert(5);
        tree.insert(3);

        assert_eq!(
            tree.into_iter().collect::<Vec<u32>>(),
            vec![1, 3, 5]
        );
    }

    #[test]
    fn test_iter() {
        let mut tree = TreapSet::new();
        tree.insert(1);
        tree.insert(5);
        tree.insert(3);

        assert_eq!(
            tree.iter().collect::<Vec<&u32>>(),
            vec![&1, &3, &5],
        );
    }
}
