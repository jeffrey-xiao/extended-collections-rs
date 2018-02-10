use rand::Rng;
use rand::XorShiftRng;
use std::ops::{Add, Sub};
use treap::entry::{MapEntry};
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
/// assert_eq!(t.get(&0), Some(&1));
/// assert_eq!(t.get(&1), None);
/// assert_eq!(t.size(), 2);
///
/// assert_eq!(t.min(), Some(&0));
/// assert_eq!(t.ceil(&2), Some(&3));
///
/// *t.get_mut(&0).unwrap() = 2;
/// assert_eq!(t.remove(&0), Some((0, 2)));
/// assert_eq!(t.remove(&1), None);
/// ```
pub struct TreapMap<T: Ord, U> {
    root: tree::Tree<MapEntry<T, U>>,
    rng: XorShiftRng,
    size: usize,
}

impl<T: Ord, U> TreapMap<T, U> {
    /// Constructs a new, empty `TreapMap<T, U>`
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t: TreapMap<u32, u32> = TreapMap::new();
    /// ```
    pub fn new() -> Self {
        TreapMap {
            root: None,
            rng: XorShiftRng::new_unseeded(),
            size: 0,
        }
    }

    /// Inserts a key-value pair into the treap. If the key already exists in the treap, it will
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
        let &mut TreapMap { ref mut root, ref mut rng, ref mut size } = self;
        let new_node = Node {
            entry: MapEntry { key, value },
            priority: rng.next_u32(),
            left: None,
            right: None,
        };
        match tree::insert(root, new_node) {
            Some(MapEntry { key, value }) => Some((key, value)),
            None => {
                *size += 1;
                None
            },
        }
    }

    /// Removes a key-value pair from the treap. If the key exists in the treap, it will return
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
        let &mut TreapMap { ref mut root, ref mut size, .. } = self;
        let (old_node_opt, r_tree) = tree::split(root, key);
        tree::merge(root, r_tree);
        match old_node_opt {
            Some(old_node) => {
                let unboxed_old_node = *old_node;
                let Node { entry: MapEntry { key, value }, .. } = unboxed_old_node;
                *size -= 1;
                Some((key, value))
            },
            None => None,
        }
    }

    /// Checks if a key exists in the treap.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.contains(&0), false);
    /// assert_eq!(t.contains(&1), true);
    /// ```
    pub fn contains(&self, key: &T) -> bool {
        let &TreapMap { ref root, .. } = self;
        tree::contains(root, key)
    }

    /// Returns an immutable reference to the value associated with a particular key. It will
    /// return `None` if the key does not exist in the treap.
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
        let &TreapMap { ref root, .. } = self;
        tree::get(root, key).map(|entry| &entry.value)
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
        let &mut TreapMap { ref mut root, .. } = self;
        tree::get_mut(root, key).map(|entry| &mut entry.value)
    }

    /// Returns the size of the treap.
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
        let &TreapMap { ref size, .. } = self;
        *size
    }


    /// Returns a key in the treap that is greater than or equal to a particular key. Returns
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
        let &TreapMap { ref root, .. } = self;
        tree::ceil(root, key).map(|entry| &entry.key)
    }


    /// Returns a key in the treap that is less than or equal to a particular key. Returns
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
        let &TreapMap { ref root, .. } = self;
        tree::floor(root, key).map(|entry| &entry.key)
    }

    /// Returns the minimum key of the treap. Returns `None` if the treap is empty.
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
        let &TreapMap { ref root, .. } = self;
        tree::min(root).map(|entry| &entry.key)
    }

    /// Returns the maximum key of the treap. Returns `None` if the treap is empty.
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
        let &TreapMap { ref root, .. } = self;
        tree::max(root).map(|entry| &entry.key)
    }

    /// Returns the union of two treaps. If there is a key that is found in both `left` and
    /// `right`, the union will contain the value associated with the key in `left`. The `+`
    /// operator is implemented to take the union of two treaps.
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
    ///     union.into_iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1), (&2, &2), (&3, &3)],
    /// );
    /// ```
    pub fn union(left: Self, right: Self) -> Self {
        let TreapMap { root: left_tree, rng, size: left_size } = left;
        let TreapMap { root: right_tree, size: right_size, .. } = right;
        let (root, dups) = tree::union(left_tree, right_tree, false);
        TreapMap { root, rng, size: left_size + right_size - dups }
    }

    /// Returns the intersection of two treaps. If there is a key that is found in both `left` and
    /// `right`, the union will contain the value associated with the key in `left`.
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
    /// let inter = TreapMap::inter(n, m);
    /// assert_eq!(
    ///     inter.into_iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&2, &2)],
    /// );
    /// ```
    pub fn inter(left: Self, right: Self) -> Self {
        let TreapMap { root: left_tree, rng, .. } = left;
        let TreapMap { root: right_tree, .. } = right;
        let (root, dups) = tree::inter(left_tree, right_tree, false);
        TreapMap { root, rng, size: dups }
    }

    /// Returns `left` subtracted by `right`. The returned treap will contain all entries that do
    /// not have a key in `right`. The `-` operator is implemented to take the difference of two
    /// treaps.
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
    /// let subtract = TreapMap::subtract(n, m);
    /// assert_eq!(
    ///     subtract.into_iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1)],
    /// );
    /// ```
    pub fn subtract(left: Self, right: Self) -> Self {
        let TreapMap { root: left_tree, rng, size } = left;
        let TreapMap { root: right_tree, .. } = right;
        let (root, dups) = tree::subtract(left_tree, right_tree, false);
        TreapMap { root, rng, size: size - dups }
    }

    /// Returns an iterator over the treap. The iterator will yield key-value pairs using in-order
    /// traversal.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapMap;
    ///
    /// let mut t = TreapMap::new();
    /// t.insert(1, 1);
    /// t.insert(3, 3);
    ///
    /// let mut iterator = t.iter();
    /// assert_eq!(iterator.next(), Some((&1, &1)));
    /// assert_eq!(iterator.next(), Some((&3, &3)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> TreapMapIterator<T, U> {
        let &TreapMap { ref root, .. } = self;
        TreapMapIterator {
            current: root,
            stack: Vec::new(),
        }
    }
}

impl<'a, T: 'a + Ord, U: 'a> IntoIterator for &'a TreapMap<T, U> {
    type Item = (&'a T, &'a U);
    type IntoIter = TreapMapIterator<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An iterator for `TreapMap<T, U>`
///
/// This iterator traverses the elements of a treap in-order.
pub struct TreapMapIterator<'a, T: 'a + Ord, U: 'a> {
    current: &'a tree::Tree<MapEntry<T, U>>,
    stack: Vec<&'a Node<MapEntry<T, U>>>,
}

impl<'a, T: 'a + Ord, U: 'a> Iterator for TreapMapIterator<'a, T, U> {
    type Item = (&'a T, &'a U);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ref node) = *self.current {
            self.stack.push(node);
            self.current = &node.left;
        }
        self.stack.pop().map(|node| {
            let &Node {
                entry: MapEntry { ref key, ref value },
                ref right,
                ..
            } = node;
            self.current = right;
            (key, value)
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
        Self::subtract(self, other)
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
    fn test_min_max_empty() {
        let tree: TreapMap<u32, u32> = TreapMap::new();
        assert_eq!(tree.min(), None);
        assert_eq!(tree.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut tree = TreapMap::new();
        tree.insert(1, 1);
        assert!(tree.contains(&1));
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
        assert!(!tree.contains(&1));
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
            union.into_iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2), (&3, &3), (&4, &4), (&5, &5)],
        );
        assert_eq!(union.size(), 5);
    }

    #[test]
    fn test_inter() {
        let mut n = TreapMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = TreapMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let inter = TreapMap::inter(n, m);

        assert_eq!(
            inter.into_iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&3, &3)],
        );
        assert_eq!(inter.size(), 1);
    }

    #[test]
    fn test_subtract() {
        let mut n = TreapMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = TreapMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let sub = n - m;

        assert_eq!(
            sub.into_iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2)],
        );
        assert_eq!(sub.size(), 2);
    }

    #[test]
    fn test_iter() {
        let mut tree = TreapMap::new();
        tree.insert(1, 2);
        tree.insert(5, 6);
        tree.insert(3, 4);

        assert_eq!(
            tree.into_iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &2), (&3, &4), (&5, &6)]
        );
    }
}
