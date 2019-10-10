use crate::treap::implicit_tree;
use crate::treap::node::ImplicitNode;
use rand::Rng;
use rand::XorShiftRng;
use std::ops::{Add, Index, IndexMut};

/// A list implemented using an implicit treap.
///
/// A treap is a tree that satisfies both the binary search tree property and a heap property. Each
/// node has a key, a value, and a priority. The key of any node is greater than all keys in its
/// left subtree and less than all keys occuring in its right subtree. The priority of a node is
/// greater than the priority of all nodes in its subtrees. By randomly generating priorities, the
/// expected height of the tree is proportional to the logarithm of the number of keys.
///
/// An implicit treap is a treap where the key of a node is implicitly determined by the size of
/// its left subtree. This property allows the list to get, remove, and insert at an arbitrary index
/// in `O(log N)` time.
///
/// # Examples
///
/// ```
/// use extended_collections::treap::TreapList;
///
/// let mut list = TreapList::new();
/// list.insert(0, 1);
/// list.push_back(2);
/// list.push_front(3);
///
/// assert_eq!(list.get(0), Some(&3));
/// assert_eq!(list.get(3), None);
/// assert_eq!(list.len(), 3);
///
/// *list.get_mut(0).unwrap() += 1;
/// assert_eq!(list.pop_front(), 4);
/// assert_eq!(list.pop_back(), 2);
/// ```
pub struct TreapList<T> {
    tree: implicit_tree::Tree<T>,
    rng: XorShiftRng,
}

impl<T> TreapList<T> {
    /// Constructs a new, empty `TreapList<T>`.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let list: TreapList<u32> = TreapList::new();
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
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.insert(0, 1);
    /// list.insert(0, 2);
    /// assert_eq!(list.get(0), Some(&2));
    /// assert_eq!(list.get(1), Some(&1));
    /// ```
    pub fn insert(&mut self, index: usize, value: T) {
        let TreapList {
            ref mut tree,
            ref mut rng,
        } = self;
        implicit_tree::insert(tree, index + 1, ImplicitNode::new(value, rng.next_u32()));
    }

    /// Removes a value at a particular index from the list. Returns the value at the index.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.insert(0, 1);
    /// assert_eq!(list.remove(0), 1);
    /// ```
    pub fn remove(&mut self, index: usize) -> T {
        implicit_tree::remove(&mut self.tree, index + 1)
    }

    /// Inserts a value at the front of the list.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.push_front(1);
    /// list.push_front(2);
    /// assert_eq!(list.get(0), Some(&2));
    /// ```
    pub fn push_front(&mut self, value: T) {
        self.insert(0, value);
    }

    /// Inserts a value at the back of the list.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.push_back(1);
    /// list.push_back(2);
    /// assert_eq!(list.get(0), Some(&1));
    /// ```
    pub fn push_back(&mut self, value: T) {
        let index = self.len();
        self.insert(index, value);
    }

    /// Removes a value at the front of the list.
    ///
    /// # Panics
    ///
    /// Panics if list is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.push_back(1);
    /// list.push_back(2);
    /// assert_eq!(list.pop_front(), 1);
    /// ```
    pub fn pop_front(&mut self) -> T {
        self.remove(0)
    }

    /// Removes a value at the back of the list.
    ///
    /// # Panics
    ///
    /// Panics if list is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.push_back(1);
    /// list.push_back(2);
    /// assert_eq!(list.pop_back(), 2);
    /// ```
    pub fn pop_back(&mut self) -> T {
        let index = self.len() - 1;
        self.remove(index)
    }

    /// Returns an immutable reference to the value at a particular index. Returns `None` if the
    /// index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.insert(0, 1);
    /// assert_eq!(list.get(0), Some(&1));
    /// assert_eq!(list.get(1), None);
    /// ```
    pub fn get(&self, index: usize) -> Option<&T> {
        implicit_tree::get(&self.tree, index + 1)
    }

    /// Returns a mutable reference to the value at a particular index. Returns `None` if the
    /// index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.insert(0, 1);
    /// *list.get_mut(0).unwrap() = 2;
    /// assert_eq!(list.get(0), Some(&2));
    /// ```
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        implicit_tree::get_mut(&mut self.tree, index + 1)
    }

    /// Returns the number of elements in the list.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.insert(0, 1);
    /// assert_eq!(list.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        implicit_tree::len(&self.tree)
    }

    /// Returns `true` if the list is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let list: TreapList<u32> = TreapList::new();
    /// assert!(list.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.tree.is_none()
    }

    /// Clears the list, removing all values.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.insert(0, 1);
    /// list.insert(1, 2);
    /// list.clear();
    /// assert_eq!(list.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.tree = None;
    }

    /// Returns an iterator over the list.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.insert(0, 1);
    /// list.insert(1, 2);
    ///
    /// let mut iterator = list.iter();
    /// assert_eq!(iterator.next(), Some(&1));
    /// assert_eq!(iterator.next(), Some(&2));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> TreapListIter<'_, T> {
        TreapListIter {
            current: &self.tree,
            stack: Vec::new(),
        }
    }

    /// Returns a mutable iterator over the list.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::treap::TreapList;
    ///
    /// let mut list = TreapList::new();
    /// list.insert(0, 1);
    /// list.insert(1, 2);
    ///
    /// for value in &mut list {
    ///     *value += 1;
    /// }
    ///
    /// let mut iterator = list.iter();
    /// assert_eq!(iterator.next(), Some(&2));
    /// assert_eq!(iterator.next(), Some(&3));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter_mut(&mut self) -> TreapListIterMut<'_, T> {
        TreapListIterMut {
            current: self.tree.as_mut().map(|node| &mut **node),
            stack: Vec::new(),
        }
    }
}

impl<T> IntoIterator for TreapList<T> {
    type IntoIter = TreapListIntoIter<T>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            current: self.tree,
            stack: Vec::new(),
        }
    }
}

impl<'a, T> IntoIterator for &'a TreapList<T>
where
    T: 'a,
{
    type IntoIter = TreapListIter<'a, T>;
    type Item = &'a T;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut TreapList<T>
where
    T: 'a,
{
    type IntoIter = TreapListIterMut<'a, T>;
    type Item = &'a mut T;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// An owning iterator for `TreapList<T>`.
///
/// This iterator traverses the elements of the list and yields owned entries.
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

/// An iterator for `TreapList<T>`.
///
/// This iterator traverses the elements of the list in-order and yields immutable references.
pub struct TreapListIter<'a, T> {
    current: &'a implicit_tree::Tree<T>,
    stack: Vec<&'a ImplicitNode<T>>,
}

impl<'a, T> Iterator for TreapListIter<'a, T>
where
    T: 'a,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ref node) = self.current {
            self.current = &node.left;
            self.stack.push(node);
        }
        self.stack.pop().map(|node| {
            let ImplicitNode {
                ref value,
                ref right,
                ..
            } = node;
            self.current = right;
            value
        })
    }
}

type BorrowedTreeMut<'a, T> = Option<&'a mut ImplicitNode<T>>;

/// A mutable iterator for `TreapList<T>`.
///
/// This iterator traverses the elements of the list in-order and yields mutable references.
pub struct TreapListIterMut<'a, T> {
    current: Option<&'a mut ImplicitNode<T>>,
    stack: Vec<Option<(&'a mut T, BorrowedTreeMut<'a, T>)>>,
}

impl<'a, T> Iterator for TreapListIterMut<'a, T>
where
    T: 'a,
{
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        let TreapListIterMut { current, stack } = self;
        while current.is_some() {
            stack.push(current.take().map(|node| {
                *current = node.left.as_mut().map(|node| &mut **node);
                (&mut node.value, node.right.as_mut().map(|node| &mut **node))
            }));
        }
        stack.pop().and_then(|pair_opt| match pair_opt {
            Some(pair) => {
                let (value, right) = pair;
                *current = right;
                Some(value)
            }
            None => None,
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

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("Error: index out of bounds.")
    }
}

impl<T> IndexMut<usize> for TreapList<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).expect("Error: index out of bounds.")
    }
}

#[cfg(test)]
mod tests {
    use super::TreapList;

    #[test]
    fn test_len_empty() {
        let list: TreapList<u32> = TreapList::new();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let list: TreapList<u32> = TreapList::new();
        assert!(list.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        assert_eq!(list.get(0), Some(&1));
    }

    #[test]
    fn test_remove() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        let ret = list.remove(0);
        assert_eq!(list.get(0), None);
        assert_eq!(ret, 1);
    }

    #[test]
    fn test_get_mut() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        {
            let value = list.get_mut(0);
            *value.unwrap() = 3;
        }
        assert_eq!(list.get(0), Some(&3));
    }

    #[test]
    fn test_push_front() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        list.push_front(2);
        assert_eq!(list.get(0), Some(&2));
    }

    #[test]
    fn test_push_back() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        list.push_back(2);
        assert_eq!(list.get(1), Some(&2));
    }

    #[test]
    fn test_pop_front() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        list.insert(1, 2);
        assert_eq!(list.pop_front(), 1);
    }

    #[test]
    fn test_pop_back() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        list.insert(1, 2);
        assert_eq!(list.pop_back(), 2);
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
        assert_eq!(res.len(), 6);
    }

    #[test]
    fn test_into_iter() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        list.insert(0, 2);
        list.insert(1, 3);

        assert_eq!(list.into_iter().collect::<Vec<u32>>(), vec![2, 3, 1]);
    }

    #[test]
    fn test_iter() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        list.insert(0, 2);
        list.insert(1, 3);

        assert_eq!(list.iter().collect::<Vec<&u32>>(), vec![&2, &3, &1]);
    }

    #[test]
    fn test_iter_mut() {
        let mut list = TreapList::new();
        list.insert(0, 1);
        list.insert(0, 2);
        list.insert(1, 3);

        for value in &mut list {
            *value += 1;
        }

        assert_eq!(list.iter().collect::<Vec<&u32>>(), vec![&3, &4, &2]);
    }
}
