use rand::Rng;
use rand::XorShiftRng;
use std::ops::{Add, Index, IndexMut};
use treap::implicit_tree;
use treap::node::ImplicitNode;

/// A list implemented by an implicit treap.
///
/// A treap is a tree that satisfies both the binary search tree property and a heap property. Each
/// node has a key, a value, and a priority. The key of any node is greater than all keys in its
/// left subtree and less than all keys occuring in its right subtree. The priority of a node is
/// greater than the priority of all nodes in its subtrees. By randomly generating priorities, the
/// expected height of the tree is proportional to the logarithm of the number of keys.
///
/// An implicit treap is a treap where the key of a node is implicitly determined by the size of
/// its left subtree. This property allows the list get, remove, and insert at an arbitrary index
/// in O(log N) time.
///
/// # Examples
/// ```
/// use data_structures::treap::TreapList;
///
/// let mut t = TreapList::new();
/// t.insert(0, 1);
/// t.push_back(2);
/// t.push_front(3);
///
/// assert_eq!(t.get(0), Some(&3));
/// assert_eq!(t.get(3), None);
/// assert_eq!(t.size(), 3);
///
/// *t.get_mut(0).unwrap() += 1;
/// assert_eq!(t.pop_front(), 4);
/// assert_eq!(t.pop_back(), 2);
/// ```
pub struct TreapList<T> {
    tree: implicit_tree::Tree<T>,
    rng: XorShiftRng,
}

impl<T> TreapList<T> {
    /// Constructs a new, empty `TreapList<T>`
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let t: TreapList<u32> = TreapList::new();
    /// ```
    pub fn new() -> Self {
        TreapList {
            tree: None,
            rng: XorShiftRng::new_unseeded(),
        }
    }

    /// Inserts a value into the list at a particular index, shifting elements one position to the
    /// right if needed.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.insert(0, 1);
    /// t.insert(0, 2);
    /// assert_eq!(t.get(0), Some(&2));
    /// assert_eq!(t.get(1), Some(&1));
    /// ```
    pub fn insert(&mut self, index: usize, value: T) {
        let TreapList { ref mut tree, ref mut rng } = *self;
        implicit_tree::insert(tree, index + 1, ImplicitNode {
            value,
            priority: rng.next_u32(),
            size: 1,
            left: None,
            right: None,
        })
    }

    /// Removes a value at a particular index from the list. Returns the value at the index.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.insert(0, 1);
    /// assert_eq!(t.remove(0), 1);
    /// ```
    pub fn remove(&mut self, index: usize) -> T {
        implicit_tree::remove(&mut self.tree, index + 1)
    }

    /// Inserts a value at the front of the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.push_front(1);
    /// t.push_front(2);
    /// assert_eq!(t.get(0), Some(&2));
    /// ```
    pub fn push_front(&mut self, value: T) {
        self.insert(0, value);
    }

    /// Inserts a value at the back of the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.push_back(1);
    /// t.push_back(2);
    /// assert_eq!(t.get(0), Some(&1));
    /// ```
    pub fn push_back(&mut self, value: T) {
        let index = self.size();
        self.insert(index, value);
    }

    /// Removes a value at the front of the list.
    ///
    /// # Panics
    /// Panics if list is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.push_back(1);
    /// t.push_back(2);
    /// assert_eq!(t.pop_front(), 1);
    /// ```
    pub fn pop_front(&mut self) -> T {
        self.remove(0)
    }

    /// Removes a value at the back of the list.
    ///
    /// # Panics
    /// Panics if list is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.push_back(1);
    /// t.push_back(2);
    /// assert_eq!(t.pop_back(), 2);
    /// ```
    pub fn pop_back(&mut self) -> T {
        let index = self.size() - 1;
        self.remove(index)
    }

    /// Returns an immutable reference to the value at a particular index. Returns `None` if the
    /// index is out of bounds.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.insert(0, 1);
    /// assert_eq!(t.get(0), Some(&1));
    /// assert_eq!(t.get(1), None);
    /// ```
    pub fn get(&self, index: usize) -> Option<&T> {
        implicit_tree::get(&self.tree, index + 1)
    }

    /// Returns a mutable reference to the value at a particular index. Returns `None` if the
    /// index is out of bounds.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.insert(0, 1);
    /// *t.get_mut(0).unwrap() = 2;
    /// assert_eq!(t.get(0), Some(&2));
    /// ```
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        implicit_tree::get_mut(&mut self.tree, index + 1)
    }

    /// Returns the size of the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.insert(0, 1);
    /// assert_eq!(t.size(), 1);
    /// ```
    pub fn size(&self) -> usize {
        implicit_tree::size(&self.tree)
    }

    /// Returns `true` if the list is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let t: TreapList<u32> = TreapList::new();
    /// assert!(t.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.tree.is_none()
    }

    /// Clears the list, removing all values.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.insert(0, 1);
    /// t.insert(1, 2);
    /// t.clear();
    /// assert_eq!(t.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.tree = None;
    }

    /// Returns an iterator over the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.insert(0, 1);
    /// t.insert(1, 2);
    ///
    /// let mut iterator = t.iter();
    /// assert_eq!(iterator.next(), Some(&1));
    /// assert_eq!(iterator.next(), Some(&2));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> TreapListIter<T> {
        TreapListIter {
            current: &self.tree,
            stack: Vec::new(),
        }
    }

    /// Returns an iterator over the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::treap::TreapList;
    ///
    /// let mut t = TreapList::new();
    /// t.insert(0, 1);
    /// t.insert(1, 2);
    ///
    /// for value in &mut t {
    ///   *value += 1;
    /// }
    ///
    /// let mut iterator = t.iter();
    /// assert_eq!(iterator.next(), Some(&2));
    /// assert_eq!(iterator.next(), Some(&3));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter_mut(&mut self) -> TreapListIterMut<T> {
        TreapListIterMut {
            current: self.tree.as_mut().map(|node| &mut **node),
            stack: Vec::new(),
        }
    }
}

impl<T> IntoIterator for TreapList<T> {
    type Item = T;
    type IntoIter = TreapListIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        TreapListIntoIter {
            current: self.tree,
            stack: Vec::new(),
        }
    }
}

impl<'a, T: 'a> IntoIterator for &'a TreapList<T> {
    type Item = &'a T;
    type IntoIter = TreapListIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: 'a> IntoIterator for &'a mut TreapList<T> {
    type Item = &'a mut T;
    type IntoIter = TreapListIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// An owning iterator for `TreapList<T>`
///
/// This iterator traverses the elements of a treap in-order and yields owned entries.
pub struct TreapListIntoIter<T> {
    current: implicit_tree::Tree<T>,
    stack: Vec<ImplicitNode<T>>,
}

impl<T> Iterator for TreapListIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mut node) = self.current.take() {
            self.current = node.left.take();
            self.stack.push(*node);
        }
        self.stack.pop().map(|node| {
            let ImplicitNode { value, right, .. } = node;
            self.current = right;
            value
        })
    }
}

/// An iterator for `TreapList<T>`
///
/// This iterator traverses the elements of a treap in-order and yields immutable references.
pub struct TreapListIter<'a, T: 'a> {
    current: &'a implicit_tree::Tree<T>,
    stack: Vec<&'a ImplicitNode<T>>,
}

impl<'a, T: 'a> Iterator for TreapListIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ref node) = *self.current {
            self.current = &node.left;
            self.stack.push(node);
        }
        self.stack.pop().map(|node| {
            let &ImplicitNode { ref value, ref right, .. } = node;
            self.current = right;
            value
        })
    }
}


/// A mutable iterator for `TreapList<T>`
///
/// This iterator traverses the elements of a treap in-order and yields mutable references.
pub struct TreapListIterMut<'a, T: 'a> {
    current: Option<&'a mut ImplicitNode<T>>,
    stack: Vec<Option<(&'a mut T, Option<&'a mut ImplicitNode<T>>)>>,
}

impl<'a, T: 'a> Iterator for TreapListIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        let TreapListIterMut { ref mut current, ref mut stack } = *self;
        while current.is_some() {
            stack.push(current.take().map(|node| {
                *current = node.left.as_mut().map(|node| &mut **node);
                (&mut node.value, node.right.as_mut().map(|node| &mut **node))
            }));
        }
        stack.pop().and_then(|pair_opt| {
            match pair_opt {
                Some(pair) => {
                    let (value, right) = pair;
                    *current = right;
                    Some(value)
                },
                None => None,
            }
        })
    }
}

impl<T> Default for TreapList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Add for TreapList<T> {
    type Output = TreapList<T>;

    fn add(mut self, other: TreapList<T>) -> TreapList<T> {
        implicit_tree::merge(&mut self.tree, other.tree);
        TreapList {
            tree: self.tree.take(),
            rng: self.rng,
        }
    }
}

impl<T> Index<usize> for TreapList<T> {
    type Output = T;
    fn index(&self, key: usize) -> &Self::Output {
        self.get(key).unwrap()
    }
}

impl<T> IndexMut<usize> for TreapList<T> {
    fn index_mut(&mut self, key: usize) -> &mut Self::Output {
        self.get_mut(key).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::TreapList;

    #[test]
    fn test_size_empty() {
        let tree: TreapList<u32> = TreapList::new();
        assert_eq!(tree.size(), 0);
    }

    #[test]
    fn test_is_empty() {
        let tree: TreapList<u32> = TreapList::new();
        assert!(tree.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        assert_eq!(tree.get(0), Some(&1));
    }

    #[test]
    fn test_remove() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        let ret = tree.remove(0);
        assert_eq!(tree.get(0), None);
        assert_eq!(ret, 1);
    }

    #[test]
    fn test_get_mut() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        {
            let value = tree.get_mut(0);
            *value.unwrap() = 3;
        }
        assert_eq!(tree.get(0), Some(&3));
    }

    #[test]
    fn test_push_front() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        tree.push_front(2);
        assert_eq!(tree.get(0), Some(&2));
    }

    #[test]
    fn test_push_back() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        tree.push_back(2);
        assert_eq!(tree.get(1), Some(&2));
    }

    #[test]
    fn test_pop_front() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        tree.insert(1, 2);
        assert_eq!(tree.pop_front(), 1);
    }

    #[test]
    fn test_pop_back() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        tree.insert(1, 2);
        assert_eq!(tree.pop_back(), 2);
    }

    #[test]
    fn test_add() {
        let mut n = TreapList::new();
        n.insert(0, 1);
        n.insert(0, 2);
        n.insert(1, 3);

        let mut m = TreapList::new();
        m.insert(0, 4);
        m.insert(0, 5);
        m.insert(1, 6);

        let res = n + m;

        assert_eq!(
            res.iter().collect::<Vec<&u32>>(),
            vec![&2, &3, &1, &5, &6, &4],
        );
        assert_eq!(res.size(), 6);
    }

    #[test]
    fn test_into_iter() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        tree.insert(0, 2);
        tree.insert(1, 3);

        assert_eq!(
            tree.into_iter().collect::<Vec<u32>>(),
            vec![2, 3, 1],
        );
    }

    #[test]
    fn test_iter() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        tree.insert(0, 2);
        tree.insert(1, 3);

        assert_eq!(
            tree.iter().collect::<Vec<&u32>>(),
            vec![&2, &3, &1],
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut tree = TreapList::new();
        tree.insert(0, 1);
        tree.insert(0, 2);
        tree.insert(1, 3);

        for value in &mut tree {
            *value += 1;
        }

        assert_eq!(
            tree.iter().collect::<Vec<&u32>>(),
            vec![&3, &4, &2],
        );
    }
}
