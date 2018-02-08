use std::mem;
use std::vec::Vec;
use rand;

/// A struct representing an internal node of a treap.
struct Node <T: PartialOrd, U> {
    key: T,
    value: U,
    priority: u32,
    size: usize,
    left: Tree<T, U>,
    right: Tree<T, U>,
}

type Tree<T, U> = Option<Box<Node<T, U>>>;

/// An ordered map implemented by a treap.
///
/// A treap is a tree that satisfies both the binary search
/// tree property and a heap property. Each node has a key, a value, and a priority. The key of any
/// node is greather than all keys in its left subtree and less than all keys occuring in its right
/// subtree. The priority of a node is greater than the priority of all nodes in its subtrees. By
/// randomly generating priorities, the expected height of the tree is proportional to the
/// logarithm of the number of keys.
///
/// # Examples
/// ```
/// use data_structures::Treap;
///
/// let mut t = Treap::new();
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
pub struct Treap<T: PartialOrd, U>(Tree<T, U>);

impl<T: PartialOrd, U> Treap<T, U> {
    /// Constructs a new, empty `Treap<T, U>`
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t: Treap<u32, u32> = Treap::new();
    /// ```
    pub fn new() -> Self { Treap(None) }

    fn update(tree: &mut Tree<T, U>) {
        if let Some(ref mut node) = *tree {
            let &mut Node{ ref mut size, ref left, ref right, .. } = &mut **node;
            *size = 1;
            if let Some(ref l_node) = *left {
                *size += l_node.size;
            }
            if let Some(ref r_node) = *right {
                *size += r_node.size;
            }
        }
    }

    fn merge(l_tree: &mut Tree<T, U>, r_tree: Tree<T, U>) {
        match (l_tree.take(), r_tree) {
            (Some(mut l_node), Some(mut r_node)) => {
                if l_node.priority > r_node.priority {
                    Self::merge(&mut l_node.right, Some(r_node));
                    *l_tree = Some(l_node);
                } else {
                    let mut new_tree = Some(l_node);
                    Self::merge(&mut new_tree, r_node.left.take());
                    r_node.left = new_tree;
                    *l_tree = Some(r_node);
                }
            },
            (new_tree, None) => *l_tree = new_tree,
            (None, new_tree) => *l_tree = new_tree,
        }
        Self::update(l_tree);
    }

    fn split(tree: &mut Tree<T, U>, k: &T) -> (Tree<T, U>, Tree<T, U>) {
        match tree.take() {
            Some(mut node) => {
                let mut ret;
                if node.key < *k {
                    ret = Self::split(&mut node.right, k);
                    *tree = Some(node);
                } else if node.key > *k {
                    let mut res = Self::split(&mut node.left, k);
                    *tree = node.left.take();
                    node.left = res.1;
                    ret = (res.0, Some(node));
                } else {
                    *tree = node.left.take();
                    let right = node.right.take();
                    ret = (Some(node), right);
                }
                Self::update(tree);
                Self::update(&mut ret.1);
                ret
            },
            None => (None, None),
        }
    }

    /// Inserts a key-value pair into the treap. If the key already exists in the treap, it will
    /// return and replace the old key-value pair.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// assert_eq!(t.insert(1, 1), None);
    /// assert_eq!(t.get(&1), Some(&1));
    /// assert_eq!(t.insert(1, 2), Some((1, 1)));
    /// assert_eq!(t.get(&1), Some(&2));
    /// ```
    pub fn insert(&mut self, key: T, value: U) -> Option<(T, U)> {
        let &mut Treap(ref mut tree) = self;

        let (old_node_opt, r_tree) = Self::split(tree, &key);

        let new_node = Some(Box::new(Node {
            key: key,
            value: value,
            priority: rand::random::<u32>(),
            size: 1,
            left: None,
            right: None,
        }));
        Self::merge(tree, new_node);
        Self::merge(tree, r_tree);
        match old_node_opt {
            Some(old_node) => {
                let unboxed_old_node = *old_node;
                let Node { key, value, .. } = unboxed_old_node;
                Some((key, value))
            }
            None => None,
        }
    }

    /// Removes a key-value pair from the treap. If the key exists in the treap, it will return
    /// the associated key-value pair. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.remove(&1), Some((1, 1)));
    /// assert_eq!(t.remove(&1), None);
    /// ```
    pub fn remove(&mut self, key: &T) -> Option<(T, U)> {
        let &mut Treap(ref mut tree) = self;
        let (old_node_opt, r_tree) = Self::split(tree, key);
        Self::merge(tree, r_tree);
        match old_node_opt {
            Some(old_node) => {
                let unboxed_old_node = *old_node;
                let Node { key, value, .. } = unboxed_old_node;
                Some((key, value))
            }
            None => None,
        }
    }

    fn tree_contains(tree: &Tree<T, U>, key: &T) -> bool {
        match *tree {
            Some(ref node) => {
                if key == &node.key {
                    true
                } else if key < &node.key {
                    Self::tree_contains(&node.left, key)
                } else {
                    Self::tree_contains(&node.right, key)
                }
            },
            None => false,
        }
    }

    /// Checks if a key exists in the treap.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.contains(&0), false);
    /// assert_eq!(t.contains(&1), true);
    /// ```
    pub fn contains(&self, key: &T) -> bool {
         let &Treap(ref tree) = self;
         Self::tree_contains(tree, key)
    }

    fn tree_get<'a>(tree: &'a Tree<T, U>, key: &T) -> Option<&'a U> {
        match *tree {
            Some(ref node) => {
                if key == &node.key {
                    Some(&node.value)
                } else if key < &node.key {
                    Self::tree_get(&node.left, key)
                } else {
                    Self::tree_get(&node.right, key)
                }
            }
            None => None,
        }
    }

    /// Returns an immutable reference to the value associated with a particular key. It will
    /// return `None` if the key does not exist in the treap.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.get(&0), None);
    /// assert_eq!(t.get(&1), Some(&1));
    /// ```
    pub fn get(&self, key: &T) -> Option<&U> {
        let &Treap(ref tree) = self;
        Self::tree_get(tree, key)
    }

    fn tree_get_mut<'a>(tree: &'a mut Tree<T, U>, key: &T) -> Option<&'a mut U> {
        match *tree {
            Some(ref mut node) => {
                if key == &node.key {
                    Some(&mut node.value)
                } else if key < &node.key {
                    Self::tree_get_mut(&mut node.left, key)
                } else {
                    Self::tree_get_mut(&mut node.right, key)
                }
            }
            None => None,
        }
    }

    /// Returns a mutable reference to the value associated with a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// *t.get_mut(&1).unwrap() = 2;
    /// assert_eq!(t.get(&1), Some(&2));
    /// ```
    pub fn get_mut(&mut self, key: &T) -> Option<&mut U> {
        let &mut Treap(ref mut tree) = self;
        Self::tree_get_mut(tree, key)
    }

    /// Returns the size of the treap.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.size(), 1);
    /// ```
    pub fn size(&self) -> usize {
        let &Treap(ref tree) = self;
        match *tree {
            Some(ref node) => node.size,
            None => 0,
        }
    }

    fn tree_ceil<'a>(tree: &'a Tree<T, U>, key: &T) -> Option<&'a T> {
        match *tree {
            Some(ref node) => {
                if &node.key == key {
                    Some(&node.key)
                } else if &node.key < key {
                    Self::tree_ceil(&node.right, key)
                } else {
                    let res = Self::tree_ceil(&node.left, key);
                    if res.is_some() {
                        res
                    } else {
                        Some(&node.key)
                    }
                }
            },
            None => None,
        }
    }

    /// Returns a key in the treap that is greater than or equal to a particular key. Returns
    /// `None` if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.ceil(&0), Some(&1));
    /// assert_eq!(t.ceil(&2), None);
    /// ```
    pub fn ceil(&self, key: &T) -> Option<&T> {
        let &Treap(ref tree) = self;
        Self::tree_ceil(tree, key)
    }

    fn tree_floor<'a>(tree: &'a Tree<T, U>, key: &T) -> Option<&'a T> {
        match *tree {
            Some(ref node) => {
                if &node.key == key {
                    Some(&node.key)
                } else if &node.key > key {
                    Self::tree_floor(&node.left, key)
                } else {
                    let res = Self::tree_floor(&node.right, key);
                    if res.is_some() {
                        res
                    } else {
                        Some(&node.key)
                    }
                }
            },
            None => None,
        }
    }

    /// Returns a key in the treap that is less than or equal to a particular key. Returns
    /// `None` if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// assert_eq!(t.floor(&0), None);
    /// assert_eq!(t.floor(&2), Some(&1));
    /// ```
    pub fn floor(&self, key: &T) -> Option<&T> {
        let &Treap(ref tree) = self;
        Self::tree_floor(tree, key)
    }

    fn tree_min(tree: &Tree<T, U>) -> Option<&T> {
        match *tree {
            Some(ref node) => {
                if node.left.is_some() {
                    Self::tree_min(&node.left)
                } else {
                    Some(&node.key)
                }
            },
            None => None,
        }
    }

    /// Returns the minimum key of the treap. Returns `None` if the treap is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// t.insert(3, 3);
    /// assert_eq!(t.min(), Some(&1));
    /// ```
    pub fn min(&self) -> Option<&T> {
        let &Treap(ref tree) = self;
        Self::tree_min(tree)
    }

    fn tree_max(tree: &Tree<T, U>) -> Option<&T> {
        match *tree {
            Some(ref node) => {
                if node.right.is_some() {
                    Self::tree_max(&node.right)
                } else {
                    Some(&node.key)
                }
            },
            None => None,
        }
    }

    /// Returns the maximum key of the treap. Returns `None` if the treap is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// t.insert(3, 3);
    /// assert_eq!(t.max(), Some(&3));
    /// ```
    pub fn max(&self) -> Option<&T> {
        let &Treap(ref tree) = self;
        Self::tree_max(tree)
    }

    fn tree_union(left_tree: Tree<T, U>, right_tree: Tree<T, U>) -> Tree<T, U> {
        match (left_tree, right_tree) {
            (None, None) => None,
            (None, right_tree) => right_tree,
            (left_tree, None) => left_tree,
            (Some(mut left_node), Some(mut right_node)) => {
                let mut swapped = false;
                if left_node.priority < right_node.priority {
                    mem::swap(&mut left_node, &mut right_node);
                    swapped = true;
                }
                {
                    let &mut Node { left: ref mut left_subtree, right: ref mut right_subtree, ref mut key, ref mut value, .. } = &mut *left_node;
                    let mut right_left_subtree = Some(right_node);
                    let (duplicate_opt, right_right_subtree) = Self::split(&mut right_left_subtree, key);
                    if swapped {
                        *left_subtree = Self::tree_union(right_left_subtree, left_subtree.take());
                        *right_subtree = Self::tree_union(right_right_subtree, right_subtree.take());
                        if let Some(duplicate_node) = duplicate_opt {
                            *value = duplicate_node.value;
                        }
                    } else {
                        *left_subtree = Self::tree_union(left_subtree.take(),  right_left_subtree);
                        *right_subtree = Self::tree_union(right_subtree.take(), right_right_subtree);
                    }
                }
                Some(left_node)
            }
        }
    }

    /// Returns the union of two treaps. If there is a key that is found in both `left` and
    /// `right`, the union will contain the value associated with the key in `left`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut n = Treap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = Treap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let union = Treap::union(n, m);
    /// assert_eq!(
    ///     union.into_iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1), (&2, &2), (&3, &3)],
    /// );
    /// ```
    pub fn union(left: Self, right: Self) -> Self {
        let Treap(left_tree) = left;
        let Treap(right_tree) = right;
        Treap(Self::tree_union(left_tree, right_tree))
    }

    /// Returns an iterator over the treap. The iterator will yield key-value pairs using in-order
    /// traversal.
    ///
    /// # Examples
    /// ```
    /// use data_structures::Treap;
    ///
    /// let mut t = Treap::new();
    /// t.insert(1, 1);
    /// t.insert(3, 3);
    ///
    /// let mut iterator = t.iter();
    /// assert_eq!(iterator.next(), Some((&1, &1)));
    /// assert_eq!(iterator.next(), Some((&3, &3)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> TreapIterator<T, U> {
        let &Treap(ref tree) = self;
        TreapIterator { current: tree, stack: Vec::new() }
    }
}

impl<'a, T: 'a + PartialOrd, U: 'a> IntoIterator for &'a Treap<T, U> {
    type Item = (&'a T, &'a U);
    type IntoIter = TreapIterator<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }

}

/// An iterator for `Treap<T, U>`
///
/// This iterator traverses the elements of a treap in-order.
pub struct TreapIterator<'a, T: 'a + PartialOrd, U: 'a> {
    current: &'a Tree<T, U>,
    stack: Vec<&'a Node<T, U>>,
}

impl<'a, T: 'a + PartialOrd, U: 'a> Iterator for TreapIterator<'a, T, U> {
    type Item = (&'a T, &'a U);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ref node) = *self.current {
            self.stack.push(node);
            self.current = &node.left;
        }
        self.stack.pop().map(|node| {
            let &Node { ref key, ref value, ref right, .. } = node;
            self.current = right;
            (key, value)
        })
    }
}

impl<T: PartialOrd, U> Default for Treap<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Treap;

    #[test]
    fn test_size_empty() {
        let tree: Treap<u32, u32> = Treap::new();
        assert_eq!(tree.size(), 0);
    }

    #[test]
    fn test_min_max_empty() {
        let tree: Treap<u32, u32> = Treap::new();
        assert_eq!(tree.min(), None);
        assert_eq!(tree.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        assert!(tree.contains(&1));
        assert_eq!(tree.get(&1), Some(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut tree = Treap::new();
        let ret_1 = tree.insert(1, 1);
        let ret_2 = tree.insert(1, 3);
        assert_eq!(tree.get(&1), Some(&3));
        assert_eq!(ret_1, None);
        assert_eq!(ret_2, Some((1, 1)));
    }

    #[test]
    fn test_remove() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        let ret = tree.remove(&1);
        assert!(!tree.contains(&1));
        assert_eq!(ret, Some((1, 1)));
    }

    #[test]
    fn test_min_max() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        tree.insert(3, 3);
        tree.insert(5, 5);

        assert_eq!(tree.min(), Some(&1));
        assert_eq!(tree.max(), Some(&5));
    }

    #[test]
    fn test_get_mut() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        {
            let value = tree.get_mut(&1);
            *value.unwrap() = 3;
        }
        assert_eq!(tree.get(&1), Some(&3));
    }

    #[test]
    fn test_floor_ceil() {
        let mut tree = Treap::new();
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
        let mut n = Treap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = Treap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        assert_eq!(
            Treap::union(n, m).into_iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2), (&3, &3), (&4, &4), (&5, &5)],
        );
    }

    #[test]
    fn test_iter() {
        let mut tree = Treap::new();
        tree.insert(1, 2);
        tree.insert(5, 6);
        tree.insert(3, 4);

        assert_eq!(tree.into_iter().collect::<Vec<(&u32, &u32)>>(), vec![(&1, &2), (&3, &4), (&5, &6)]);
    }
}
