use rand::Rng;
use rand::XorShiftRng;
use std::ops::{Add, Sub};
use treap::entry::{SetEntry};
use treap::node::Node;
use treap::tree;

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
    root: tree::Tree<SetEntry<T>>,
    rng: XorShiftRng,
    size: usize,
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
            root: None,
            rng: XorShiftRng::new_unseeded(),
            size: 0,
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
        let &mut TreapSet { ref mut root, ref mut rng, ref mut size } = self;
        let new_node = Node {
            entry: SetEntry(key),
            priority: rng.next_u32(),
            left: None,
            right: None,
        };
        match tree::insert(root, new_node) {
            Some(SetEntry(key)) => Some(key),
            None => {
                *size += 1;
                None
            },
        }
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
        let &mut TreapSet { ref mut root, ref mut size, .. } = self;
        tree::remove(root, key).and_then(|entry| {
            *size -= 1;
            Some(entry.0)
        })
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
        let &TreapSet { ref root, .. } = self;
        tree::contains(root, key)
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
        let &TreapSet { ref size, .. } = self;
        *size
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
        let &TreapSet { ref root, .. } = self;
        tree::ceil(root, key).map(|entry| &entry.0)
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
        let &TreapSet { ref root, .. } = self;
        tree::floor(root, key).map(|entry| &entry.0)
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
        let &TreapSet { ref root, .. } = self;
        tree::min(root).map(|entry| &entry.0)
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
        let &TreapSet { ref root, .. } = self;
        tree::max(root).map(|entry| &entry.0)
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
    ///     union.into_iter().collect::<Vec<&u32>>(),
    ///     vec![&1, &2, &3],
    /// );
    /// ```
    pub fn union(left: Self, right: Self) -> Self {
        let TreapSet { root: left_tree, rng, size: left_size } = left;
        let TreapSet { root: right_tree, size: right_size, .. } = right;
        let (root, dups) = tree::union(left_tree, right_tree, false);
        TreapSet { root, rng, size: left_size + right_size - dups }
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
    ///     inter.into_iter().collect::<Vec<&u32>>(),
    ///     vec![&2],
    /// );
    /// ```
    pub fn inter(left: Self, right: Self) -> Self {
        let TreapSet { root: left_tree, rng, .. } = left;
        let TreapSet { root: right_tree, .. } = right;
        let (root, dups) = tree::inter(left_tree, right_tree, false);
        TreapSet { root, rng, size: dups }
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
    ///     subtract.into_iter().collect::<Vec<&u32>>(),
    ///     vec![&1],
    /// );
    /// ```
    pub fn subtract(left: Self, right: Self) -> Self {
        let TreapSet { root: left_tree, rng, size } = left;
        let TreapSet { root: right_tree, .. } = right;
        let (root, dups) = tree::subtract(left_tree, right_tree, false);
        TreapSet { root, rng, size: size - dups }
    }

    /// Returns an iterator over the treap. The iterator will yield keys using in-order traversal.
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
    pub fn iter(&self) -> TreapSetIterator<T> {
        let &TreapSet { ref root, .. } = self;
        TreapSetIterator {
            current: root,
            stack: Vec::new(),
        }
    }
}

impl<'a, T: 'a + Ord> IntoIterator for &'a TreapSet<T> {
    type Item = &'a T;
    type IntoIter = TreapSetIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An iterator for `TreapSet<T>`
///
/// This iterator traverses the elements of a treap in-order.
pub struct TreapSetIterator<'a, T: 'a + Ord> {
    current: &'a tree::Tree<SetEntry<T>>,
    stack: Vec<&'a Node<SetEntry<T>>>,
}

impl<'a, T: 'a + Ord> Iterator for TreapSetIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ref node) = *self.current {
            self.stack.push(node);
            self.current = &node.left;
        }
        self.stack.pop().map(|node| {
            let &Node {
                entry: SetEntry(ref key),
                ref right,
                ..
            } = node;
            self.current = right;
            key
        })
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
            union.into_iter().collect::<Vec<&u32>>(),
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
            inter.into_iter().collect::<Vec<&u32>>(),
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
            sub.into_iter().collect::<Vec<&u32>>(),
            vec![&1, &2],
        );
        assert_eq!(sub.size(), 2);
    }

    #[test]
    fn test_iter() {
        let mut tree = TreapSet::new();
        tree.insert(1);
        tree.insert(5);
        tree.insert(3);

        assert_eq!(
            tree.into_iter().collect::<Vec<&u32>>(),
            vec![&1, &3, &5]
        );
    }
}
