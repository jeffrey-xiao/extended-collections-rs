use rand::Rng;
use rand::XorShiftRng;
use std::ops::{Add, Sub, Index, IndexMut};
use treap::entry::{Entry};
use treap::node::Node;
use treap::tree;

/// An ordered map implemented by a treap.
///
/// A treap is a tree that satisfies both the binary search tree property and a heap property. Each
/// node has a key, a value, and a priority. The key of any node is greater than all keys in its
/// left subtree and less than all keys occuring in its right subtree. The priority of a node is
/// greater than the priority of all nodes in its subtrees. By randomly generating priorities, the
/// expected height of the tree is proportional to the logarithm of the number of keys.
///
/// # Examples
/// ```
/// use data_structures::treap::TreapMap;
///
/// let mut t = TreapMap::new();
/// t.insert(0, 1);
/// t.insert(3, 4);
///
/// assert_eq!(t[0], 1);
/// assert_eq!(t.get(&1), None);
/// assert_eq!(t.size(), 2);
///
/// assert_eq!(t.min(), Some(&0));
/// assert_eq!(t.ceil(&2), Some(&3));
///
/// t[0] = 2;
/// assert_eq!(t.remove(&0), Some((0, 2)));
/// assert_eq!(t.remove(&1), None);
/// ```
pub struct TreapMap<T: Ord, U> {
    tree: tree::Tree<T, U>,
    rng: XorShiftRng,
}

impl<T: Ord, U> TreapMap<T, U> {
    /// Constructs a new, empty `TreapMap<T, U>`
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let t: TreapMap<u32, u32> = TreapMap::new();
    /// ```
    pub fn new() -> Self {
        TreapMap {
            tree: None,
            rng: XorShiftRng::new_unseeded(),
        }
    }

    /// Inserts a key-value pair into the map. If the key already exists in the map, it will
    /// return and replace the old key-value pair.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// assert_eq!(t.insert(1, 1), None);
    /// assert_eq!(t.get(&1), Some(&1));
    /// assert_eq!(t.insert(1, 2), Some((1, 1)));
    /// assert_eq!(t.get(&1), Some(&2));
    /// ```
    pub fn insert(&mut self, key: T, value: U) -> Option<(T, U)> {
        let &mut TreapMap { ref mut tree, ref mut rng } = self;
        let new_node = Node {
            entry: Entry { key, value },
            priority: rng.next_u32(),
            size: 1,
            left: None,
            right: None,
        };
        tree::insert(tree, new_node).and_then(|entry| {
            let Entry { key, value } = entry;
            Some((key, value))
        })
    }

    /// Removes a key-value pair from the map. If the key exists in the map, it will return
    /// the associated key-value pair. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.remove(&1), Some((1, 1)));
    /// assert_eq!(t.remove(&1), None);
    /// ```
    pub fn remove(&mut self, key: &T) -> Option<(T, U)> {
        let &mut TreapMap { ref mut tree, .. } = self;
        tree::remove(tree, key).and_then(|entry| {
            let Entry { key, value } = entry;
            Some((key, value))
        })
    }

    /// Checks if a key exists in the map.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.contains_key(&0), false);
    /// assert_eq!(t.contains_key(&1), true);
    /// ```
    pub fn contains_key(&self, key: &T) -> bool {
        tree::contains(&self.tree, key)
    }

    /// Returns an immutable reference to the value associated with a particular key. It will
    /// return `None` if the key does not exist in the map.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.get(&0), None);
    /// assert_eq!(t.get(&1), Some(&1));
    /// ```
    pub fn get(&self, key: &T) -> Option<&U> {
        tree::get(&self.tree, key).map(|entry| &entry.value)
    }

    /// Returns a mutable reference to the value associated with a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// *t.get_mut(&1).unwrap() = 2;
    /// assert_eq!(t.get(&1), Some(&2));
    /// ```
    pub fn get_mut(&mut self, key: &T) -> Option<&mut U> {
        tree::get_mut(&mut self.tree, key).map(|entry| &mut entry.value)
    }

    /// Returns the size of the map.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.size(), 1);
    /// ```
    pub fn size(&self) -> usize {
        match self.tree {
            None => 0,
            Some(ref node) => node.size(),
        }
    }

    /// Returns `true` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let t: TreapMap<u32, u32> = TreapMap::new();
    /// assert!(t.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Clears the map, removing all values.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// t.insert(2, 2);
    /// t.clear();
    /// assert_eq!(t.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.tree = None;
    }

    /// Returns a key in the map that is greater than or equal to a particular key. Returns
    /// `None` if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.ceil(&0), Some(&1));
    /// assert_eq!(t.ceil(&2), None);
    /// ```
    pub fn ceil(&self, key: &T) -> Option<&T> {
        tree::ceil(&self.tree, key).map(|entry| &entry.key)
    }


    /// Returns a key in the map that is less than or equal to a particular key. Returns
    /// `None` if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.floor(&0), None);
    /// assert_eq!(t.floor(&2), Some(&1));
    /// ```
    pub fn floor(&self, key: &T) -> Option<&T> {
        tree::floor(&self.tree, key).map(|entry| &entry.key)
    }

    /// Returns the minimum key of the map. Returns `None` if the treap is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// t.insert(3, 3);
    /// assert_eq!(t.min(), Some(&1));
    /// ```
    pub fn min(&self) -> Option<&T> {
        tree::min(&self.tree).map(|entry| &entry.key)
    }

    /// Returns the maximum key of the map. Returns `None` if the treap is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// t.insert(3, 3);
    /// assert_eq!(t.max(), Some(&3));
    /// ```
    pub fn max(&self) -> Option<&T> {
        tree::max(&self.tree).map(|entry| &entry.key)
    }

    /// Returns the union of two maps. If there is a key that is found in both `left` and
    /// `right`, the union will contain the value associated with the key in `left`. The `+`
    /// operator is implemented to take the union of two maps.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut n = TreapMap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = TreapMap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let union = TreapMap::union(n, m);
    /// assert_eq!(
    ///     union.iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1), (&2, &2), (&3, &3)],
    /// );
    /// ```
    pub fn union(left: Self, right: Self) -> Self {
        let TreapMap { tree: left_tree, rng } = left;
        let TreapMap { tree: right_tree, .. } = right;
        TreapMap { tree: tree::union(left_tree, right_tree, false), rng }
    }

    /// Returns the intersection of two maps. If there is a key that is found in both `left` and
    /// `right`, the intersection will contain the value associated with the key in `left`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut n = TreapMap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = TreapMap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let intersection = TreapMap::intersection(n, m);
    /// assert_eq!(
    ///     intersection.iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&2, &2)],
    /// );
    /// ```
    pub fn intersection(left: Self, right: Self) -> Self {
        let TreapMap { tree: left_tree, rng } = left;
        TreapMap { tree: tree::intersection(left_tree, right.tree, false), rng }
    }

    /// Returns the difference of `left` and `right`. The returned map will contain all entries that
    /// do not have a key in `right`. The `-` operator is implemented to take the difference of two
    /// maps.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut n = TreapMap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = TreapMap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let difference = TreapMap::difference(n, m);
    /// assert_eq!(
    ///     difference.iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1)],
    /// );
    /// ```
    pub fn difference(left: Self, right: Self) -> Self {
        let TreapMap { tree: left_tree, rng } = left;
        TreapMap { tree: tree::difference(left_tree, right.tree, false, false), rng }
    }

    /// Returns the symmetric difference of `left` and `right`. The returned map will contain all
    /// entries that exist in one map, but not both maps.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut n = TreapMap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = TreapMap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let symmetric_difference = TreapMap::symmetric_difference(n, m);
    /// assert_eq!(
    ///     symmetric_difference.iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1), (&3, &3)],
    /// );
    /// ```
    pub fn symmetric_difference(left: Self, right:Self) -> Self {
        let TreapMap { tree: left_tree, rng } = left;
        let TreapMap { tree: right_tree, .. } = right;
        TreapMap { tree: tree::difference(left_tree, right_tree, false, true), rng }
    }

    /// Returns an iterator over the map. The iterator will yield key-value pairs using in-order
    /// traversal.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// t.insert(2, 2);
    ///
    /// let mut iterator = t.iter();
    /// assert_eq!(iterator.next(), Some((&1, &1)));
    /// assert_eq!(iterator.next(), Some((&2, &2)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> TreapMapIter<T, U> {
        TreapMapIter {
            current: &self.tree,
            stack: Vec::new(),
        }
    }

    /// Returns a mutable iterator over the map. The iterator will yield key-value pairs using
    /// in-order traversal.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// t.insert(2, 2);
    ///
    /// for (key, value) in &mut t {
    ///   *value += 1;
    /// }
    ///
    /// let mut iterator = t.iter_mut();
    /// assert_eq!(iterator.next(), Some((&1, &mut 2)));
    /// assert_eq!(iterator.next(), Some((&2, &mut 3)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter_mut(&mut self) -> TreapMapIterMut<T, U> {
        TreapMapIterMut {
            current: self.tree.as_mut().map(|node| &mut **node),
            stack: Vec::new(),
        }
    }
}

impl<T: Ord, U> IntoIterator for TreapMap<T, U> {
    type Item = (T, U);
    type IntoIter = TreapMapIntoIter<T, U>;

    fn into_iter(self) -> Self::IntoIter {
        TreapMapIntoIter {
            current: self.tree,
            stack: Vec::new(),
        }
    }
}

impl<'a, T: 'a + Ord, U: 'a> IntoIterator for &'a TreapMap<T, U> {
    type Item = (&'a T, &'a U);
    type IntoIter = TreapMapIter<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: 'a + Ord, U: 'a> IntoIterator for &'a mut TreapMap<T, U> {
    type Item = (&'a T, &'a mut U);
    type IntoIter = TreapMapIterMut<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// An owning iterator for `TreapMap<T, U>`
///
/// This iterator traverses the elements of a map in-order and yields owned entries.
pub struct TreapMapIntoIter<T: Ord, U> {
    current: tree::Tree<T, U>,
    stack: Vec<Node<T, U>>,
}

impl<T: Ord, U> Iterator for TreapMapIntoIter<T, U> {
    type Item = (T, U);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mut node) = self.current.take() {
            self.current = node.left.take();
            self.stack.push(*node);
        }
        self.stack.pop().map(|node| {
            let Node {
                entry: Entry { key, value },
                right,
                ..
            } = node;
            self.current = right;
            (key, value)
        })
    }
}

/// An iterator for `TreapMap<T, U>`
///
/// This iterator traverses the elements of a map in-order and yields immutable references.
pub struct TreapMapIter<'a, T: 'a + Ord, U: 'a> {
    current: &'a tree::Tree<T, U>,
    stack: Vec<&'a Node<T, U>>,
}

impl<'a, T: 'a + Ord, U: 'a> Iterator for TreapMapIter<'a, T, U> {
    type Item = (&'a T, &'a U);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ref node) = *self.current {
            self.current = &node.left;
            self.stack.push(node);
        }
        self.stack.pop().map(|node| {
            let &Node {
                entry: Entry { ref key, ref value },
                ref right,
                ..
            } = node;
            self.current = right;
            (key, value)
        })
    }
}


/// A mutable iterator for `TreapMap<T, U>`
///
/// This iterator traverses the elements of a map in-order and yields mutable references.
pub struct TreapMapIterMut<'a, T: 'a + Ord, U: 'a> {
    current: Option<&'a mut Node<T, U>>,
    stack: Vec<Option<(&'a mut Entry<T, U>, Option<&'a mut Node<T, U>>)>>,
}

impl<'a, T: 'a + Ord, U: 'a> Iterator for TreapMapIterMut<'a, T, U> {
    type Item = (&'a T, &'a mut U);

    fn next(&mut self) -> Option<Self::Item> {
        let TreapMapIterMut { ref mut current, ref mut stack } = *self;
        while current.is_some() {
            stack.push(current.take().map(|node| {
                *current = node.left.as_mut().map(|node| &mut **node);
                (&mut node.entry, node.right.as_mut().map(|node| &mut **node))
            }));
        }
        stack.pop().and_then(|pair_opt| {
            match pair_opt {
                Some(pair) => {
                    let (entry, right) = pair;
                    let &mut Entry { ref key, ref mut value } = entry;
                    *current = right;
                    Some((key, value))
                },
                None => None,
            }
        })
    }
}

impl<T: Ord, U> Default for TreapMap<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Ord, U> Add for TreapMap<T, U> {
    type Output = TreapMap<T, U>;

    fn add(self, other: TreapMap<T, U>) -> TreapMap<T, U> {
        Self::union(self, other)
    }
}

impl<T: Ord, U> Sub for TreapMap<T, U> {
    type Output = TreapMap<T, U>;

    fn sub(self, other: TreapMap<T, U>) -> TreapMap<T, U> {
        Self::difference(self, other)
    }
}

impl<T: Ord, U> Index<T> for TreapMap<T, U> {
    type Output = U;
    fn index(&self, key: T) -> &Self::Output {
        self.get(&key).unwrap()
    }
}

impl<T: Ord, U> IndexMut<T> for TreapMap<T, U> {
    fn index_mut(&mut self, key: T) -> &mut Self::Output {
        self.get_mut(&key).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::TreapMap;

    #[test]
    fn test_size_empty() {
        let tree: TreapMap<u32, u32> = TreapMap::new();
        assert_eq!(tree.size(), 0);
    }

    #[test]
    fn test_is_empty() {
        let tree: TreapMap<u32, u32> = TreapMap::new();
        assert!(tree.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let tree: TreapMap<u32, u32> = TreapMap::new();
        assert_eq!(tree.min(), None);
        assert_eq!(tree.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut tree = TreapMap::new();
        tree.insert(1, 1);
        assert!(tree.contains_key(&1));
        assert_eq!(tree.get(&1), Some(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut tree = TreapMap::new();
        let ret_1 = tree.insert(1, 1);
        let ret_2 = tree.insert(1, 3);
        assert_eq!(tree.get(&1), Some(&3));
        assert_eq!(ret_1, None);
        assert_eq!(ret_2, Some((1, 1)));
    }

    #[test]
    fn test_remove() {
        let mut tree = TreapMap::new();
        tree.insert(1, 1);
        let ret = tree.remove(&1);
        assert!(!tree.contains_key(&1));
        assert_eq!(ret, Some((1, 1)));
    }

    #[test]
    fn test_min_max() {
        let mut tree = TreapMap::new();
        tree.insert(1, 1);
        tree.insert(3, 3);
        tree.insert(5, 5);

        assert_eq!(tree.min(), Some(&1));
        assert_eq!(tree.max(), Some(&5));
    }

    #[test]
    fn test_get_mut() {
        let mut tree = TreapMap::new();
        tree.insert(1, 1);
        {
            let value = tree.get_mut(&1);
            *value.unwrap() = 3;
        }
        assert_eq!(tree.get(&1), Some(&3));
    }

    #[test]
    fn test_floor_ceil() {
        let mut tree = TreapMap::new();
        tree.insert(1, 1);
        tree.insert(3, 3);
        tree.insert(5, 5);

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
        let mut n = TreapMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = TreapMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let union = n + m;

        assert_eq!(
            union.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2), (&3, &3), (&4, &4), (&5, &5)],
        );
        assert_eq!(union.size(), 5);
    }

    #[test]
    fn test_intersection() {
        let mut n = TreapMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = TreapMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let intersection = TreapMap::intersection(n, m);

        assert_eq!(
            intersection.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&3, &3)],
        );
        assert_eq!(intersection.size(), 1);
    }

    #[test]
    fn test_difference() {
        let mut n = TreapMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = TreapMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let difference = n - m;

        assert_eq!(
            difference.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2)],
        );
        assert_eq!(difference.size(), 2);
    }

    #[test]
    fn test_symmetric_difference() {
        let mut n = TreapMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = TreapMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let symmetric_difference = TreapMap::symmetric_difference(n, m);

        assert_eq!(
            symmetric_difference.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2), (&4, &4), (&5, &5)],
        );
        assert_eq!(symmetric_difference.size(), 4);
    }

    #[test]
    fn test_into_iter() {
        let mut tree = TreapMap::new();
        tree.insert(1, 2);
        tree.insert(5, 6);
        tree.insert(3, 4);

        assert_eq!(
            tree.into_iter().collect::<Vec<(u32, u32)>>(),
            vec![(1, 2), (3, 4), (5, 6)],
        );
    }

    #[test]
    fn test_iter() {
        let mut tree = TreapMap::new();
        tree.insert(1, 2);
        tree.insert(5, 6);
        tree.insert(3, 4);

        assert_eq!(
            tree.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &2), (&3, &4), (&5, &6)],
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut tree = TreapMap::new();
        tree.insert(1, 2);
        tree.insert(5, 6);
        tree.insert(3, 4);

        for (_, value) in &mut tree {
            *value += 1;
        }

        assert_eq!(
            tree.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &3), (&3, &5), (&5, &7)],
        );
    }
}
