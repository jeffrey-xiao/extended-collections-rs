use entry::Entry;
use splay_tree::node::Node;
use splay_tree::tree;
use std::borrow::Borrow;
use std::ops::{Index, IndexMut};

/// An ordered map implemented using splay tree.
///
/// An splay tree is a self-adjusting binary tree with an additional property that recently accessed
/// items are quick to access again. After each operation, the item that was accessed is "splayed"
/// to the root of the tree.
///
/// # Examples
///
/// ```
/// use extended_collections::splay_tree::SplayMap;
///
/// let mut map = SplayMap::new();
/// map.insert(0, 1);
/// map.insert(3, 4);
///
/// assert_eq!(map[&0], 1);
/// assert_eq!(map.get(&1), None);
/// assert_eq!(map.len(), 2);
///
/// assert_eq!(map.min(), Some(&0));
/// assert_eq!(map.ceil(&2), Some(&3));
///
/// map[&0] = 2;
/// assert_eq!(map.remove(&0), Some((0, 2)));
/// assert_eq!(map.remove(&1), None);
/// ```
pub struct SplayMap<T, U> {
    tree: tree::Tree<T, U>,
    len: usize,
}

impl<T, U> SplayMap<T, U> {
    /// Constructs a new, empty `SplayMap<T, U>`.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let map: SplayMap<u32, u32> = SplayMap::new();
    /// ```
    pub fn new() -> Self {
        SplayMap { tree: None, len: 0 }
    }

    /// Inserts a key-value pair into the map. If the key already exists in the map, it will return
    /// and replace the old key-value pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// assert_eq!(map.insert(1, 1), None);
    /// assert_eq!(map.get(&1), Some(&1));
    /// assert_eq!(map.insert(1, 2), Some((1, 1)));
    /// assert_eq!(map.get(&1), Some(&2));
    /// ```
    pub fn insert(&mut self, key: T, value: U) -> Option<(T, U)>
    where
        T: Ord,
    {
        let SplayMap {
            ref mut tree,
            ref mut len,
        } = self;
        let new_node = Node::new(key, value);
        *len += 1;
        tree::insert(tree, new_node).and_then(|entry| {
            let Entry { key, value } = entry;
            *len -= 1;
            Some((key, value))
        })
    }

    /// Removes a key-value pair from the map. If the key exists in the map, it will return the
    /// associated key-value pair. Otherwise it will return `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.remove(&1), Some((1, 1)));
    /// assert_eq!(map.remove(&1), None);
    /// ```
    pub fn remove<V>(&mut self, key: &V) -> Option<(T, U)>
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        let SplayMap {
            ref mut tree,
            ref mut len,
        } = self;
        tree::remove(tree, &key).and_then(|entry| {
            let Entry { key, value } = entry;
            *len -= 1;
            Some((key, value))
        })
    }

    /// Checks if a key exists in the map. Note that `contains_key` does not splay the tree in
    /// order to use a non-mutable reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// assert!(!map.contains_key(&0));
    /// assert!(map.contains_key(&1));
    /// ```
    pub fn contains_key<V>(&self, key: &V) -> bool
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Returns an immutable reference to the value associated with a particular key. It will
    /// return `None` if the key does not exist in the map. Note that `get` does not splay the tree
    /// in order to use a non-mutable reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.get(&0), None);
    /// assert_eq!(map.get(&1), Some(&1));
    /// ```
    pub fn get<V>(&self, key: &V) -> Option<&U>
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        tree::get(&self.tree, key).map(|entry| &entry.value)
    }

    /// Returns a mutable reference to the value associated with a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// *map.get_mut(&1).unwrap() = 2;
    /// assert_eq!(map.get(&1), Some(&2));
    /// ```
    pub fn get_mut<V>(&mut self, key: &V) -> Option<&mut U>
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        tree::get_mut(&mut self.tree, key).map(|entry| &mut entry.value)
    }

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let map: SplayMap<u32, u32> = SplayMap::new();
    /// assert!(map.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears the map, removing all values.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    /// map.clear();
    /// assert_eq!(map.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.tree = None;
        self.len = 0;
    }

    /// Returns a key in the map that is less than or equal to a particular key. Returns `None` if
    /// such a key does not exist. Note that `floor` does not splay the tree in order to use a
    /// non-mutable reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.floor(&0), None);
    /// assert_eq!(map.floor(&2), Some(&1));
    /// ```
    pub fn floor<V>(&self, key: &V) -> Option<&T>
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        tree::floor(&self.tree, key).map(|entry| &entry.key)
    }

    /// Returns a key in the map that is greater than or equal to a particular key. Returns `None`
    /// if such a key does not exist. Note that `ceil` does not splay the tree in order to use a
    /// non-mutable reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.ceil(&0), Some(&1));
    /// assert_eq!(map.ceil(&2), None);
    /// ```
    pub fn ceil<V>(&self, key: &V) -> Option<&T>
    where
        T: Borrow<V>,
        V: Ord + ?Sized,
    {
        tree::ceil(&self.tree, key).map(|entry| &entry.key)
    }

    /// Returns the minimum key of the map. Returns `None` if the map is empty. Node that `min`
    /// does not splay the tree in order to use a non-mutable reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// map.insert(3, 3);
    /// assert_eq!(map.min(), Some(&1));
    /// ```
    pub fn min(&self) -> Option<&T>
    where
        T: Ord,
    {
        tree::min(&self.tree).map(|entry| &entry.key)
    }

    /// Returns the maximum key of the map. Returns `None` if the map is empty. Node that `max`
    /// does not splay the tree in order to use a non-mutable reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// map.insert(3, 3);
    /// assert_eq!(map.max(), Some(&3));
    /// ```
    pub fn max(&self) -> Option<&T>
    where
        T: Ord,
    {
        tree::max(&self.tree).map(|entry| &entry.key)
    }

    /// Returns an iterator over the map. The iterator will yield key-value pairs using in-order
    /// traversal.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    ///
    /// let mut iterator = map.iter();
    /// assert_eq!(iterator.next(), Some((&1, &1)));
    /// assert_eq!(iterator.next(), Some((&2, &2)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> SplayMapIter<T, U> {
        SplayMapIter {
            current: &self.tree,
            stack: Vec::new(),
        }
    }

    /// Returns a mutable iterator over the map. The iterator will yield key-value pairs using
    /// in-order traversal.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::splay_tree::SplayMap;
    ///
    /// let mut map = SplayMap::new();
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    ///
    /// for (key, value) in &mut map {
    ///     *value += 1;
    /// }
    ///
    /// let mut iterator = map.iter_mut();
    /// assert_eq!(iterator.next(), Some((&1, &mut 2)));
    /// assert_eq!(iterator.next(), Some((&2, &mut 3)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter_mut(&mut self) -> SplayMapIterMut<T, U> {
        SplayMapIterMut {
            current: self.tree.as_mut().map(|node| &mut **node),
            stack: Vec::new(),
        }
    }
}

impl<T, U> IntoIterator for SplayMap<T, U> {
    type IntoIter = SplayMapIntoIter<T, U>;
    type Item = (T, U);

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            current: self.tree,
            stack: Vec::new(),
        }
    }
}

impl<'a, T, U> IntoIterator for &'a SplayMap<T, U>
where
    T: 'a,
    U: 'a,
{
    type IntoIter = SplayMapIter<'a, T, U>;
    type Item = (&'a T, &'a U);

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T, U> IntoIterator for &'a mut SplayMap<T, U>
where
    T: 'a,
    U: 'a,
{
    type IntoIter = SplayMapIterMut<'a, T, U>;
    type Item = (&'a T, &'a mut U);

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// An owning iterator for `SplayMap<T, U>`.
///
/// This iterator traverses the elements of the map in-order and yields owned entries.
pub struct SplayMapIntoIter<T, U> {
    current: tree::Tree<T, U>,
    stack: Vec<Node<T, U>>,
}

impl<T, U> Iterator for SplayMapIntoIter<T, U> {
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

/// An iterator for `SplayMap<T, U>`.
///
/// This iterator traverses the elements of the map in-order and yields immutable references.
pub struct SplayMapIter<'a, T, U>
where
    T: 'a,
    U: 'a,
{
    current: &'a tree::Tree<T, U>,
    stack: Vec<&'a Node<T, U>>,
}

impl<'a, T, U> Iterator for SplayMapIter<'a, T, U>
where
    T: 'a,
    U: 'a,
{
    type Item = (&'a T, &'a U);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ref node) = self.current {
            self.current = &node.left;
            self.stack.push(node);
        }
        self.stack.pop().map(|node| {
            let Node {
                entry: Entry { ref key, ref value },
                ref right,
                ..
            } = node;
            self.current = right;
            (key, value)
        })
    }
}

type BorrowedIterEntryMut<'a, T, U> = Option<(&'a mut Entry<T, U>, BorrowedTreeMut<'a, T, U>)>;
type BorrowedTreeMut<'a, T, U> = Option<&'a mut Node<T, U>>;

/// A mutable iterator for `SplayMap<T, U>`.
///
/// This iterator traverses the elements of the map in-order and yields mutable references.
pub struct SplayMapIterMut<'a, T, U>
where
    T: 'a,
    U: 'a,
{
    current: Option<&'a mut Node<T, U>>,
    stack: Vec<BorrowedIterEntryMut<'a, T, U>>,
}

impl<'a, T, U> Iterator for SplayMapIterMut<'a, T, U>
where
    T: 'a,
    U: 'a,
{
    type Item = (&'a T, &'a mut U);

    fn next(&mut self) -> Option<Self::Item> {
        let SplayMapIterMut {
            ref mut current,
            ref mut stack,
        } = self;
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
                    let Entry {
                        ref key,
                        ref mut value,
                    } = entry;
                    *current = right;
                    Some((key, value))
                },
                None => None,
            }
        })
    }
}

impl<T, U> Default for SplayMap<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T, U, V> Index<&'a V> for SplayMap<T, U>
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    type Output = U;

    fn index(&self, key: &V) -> &Self::Output {
        self.get(key).expect("Error: key does not exist.")
    }
}

impl<'a, T, U, V> IndexMut<&'a V> for SplayMap<T, U>
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    fn index_mut(&mut self, key: &V) -> &mut Self::Output {
        self.get_mut(key).expect("Error: key does not exist.")
    }
}

#[cfg(test)]
mod tests {
    use super::SplayMap;

    #[test]
    fn test_len_empty() {
        let map: SplayMap<u32, u32> = SplayMap::new();
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let map: SplayMap<u32, u32> = SplayMap::new();
        assert!(map.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let map: SplayMap<u32, u32> = SplayMap::new();
        assert_eq!(map.min(), None);
        assert_eq!(map.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut map = SplayMap::new();
        assert_eq!(map.insert(1, 1), None);
        assert!(map.contains_key(&1));
        assert_eq!(map.get(&1), Some(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut map = SplayMap::new();
        assert_eq!(map.insert(1, 1), None);
        assert_eq!(map.insert(1, 3), Some((1, 1)));
        assert_eq!(map.get(&1), Some(&3));
    }

    #[test]
    fn test_remove() {
        let mut map = SplayMap::new();
        map.insert(1, 1);
        assert_eq!(map.remove(&1), Some((1, 1)));
        assert!(!map.contains_key(&1));
    }

    #[test]
    fn test_min_max() {
        let mut map = SplayMap::new();
        map.insert(1, 1);
        map.insert(3, 3);
        map.insert(5, 5);

        assert_eq!(map.min(), Some(&1));
        assert_eq!(map.max(), Some(&5));
    }

    #[test]
    fn test_get_mut() {
        let mut map = SplayMap::new();
        map.insert(1, 1);
        {
            let value = map.get_mut(&1);
            *value.unwrap() = 3;
        }
        assert_eq!(map.get(&1), Some(&3));
    }

    #[test]
    fn test_floor_ceil() {
        let mut map = SplayMap::new();
        map.insert(1, 1);
        map.insert(3, 3);
        map.insert(5, 5);

        assert_eq!(map.floor(&0), None);
        assert_eq!(map.floor(&2), Some(&1));
        assert_eq!(map.floor(&4), Some(&3));
        assert_eq!(map.floor(&6), Some(&5));

        assert_eq!(map.ceil(&0), Some(&1));
        assert_eq!(map.ceil(&2), Some(&3));
        assert_eq!(map.ceil(&4), Some(&5));
        assert_eq!(map.ceil(&6), None);
    }

    #[test]
    fn test_into_iter() {
        let mut map = SplayMap::new();
        map.insert(1, 2);
        map.insert(5, 6);
        map.insert(3, 4);

        assert_eq!(
            map.into_iter().collect::<Vec<(u32, u32)>>(),
            vec![(1, 2), (3, 4), (5, 6)],
        );
    }

    #[test]
    fn test_iter() {
        let mut map = SplayMap::new();
        map.insert(1, 2);
        map.insert(5, 6);
        map.insert(3, 4);

        assert_eq!(
            map.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &2), (&3, &4), (&5, &6)],
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut map = SplayMap::new();
        map.insert(1, 2);
        map.insert(5, 6);
        map.insert(3, 4);

        for (_, value) in &mut map {
            *value += 1;
        }

        assert_eq!(
            map.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &3), (&3, &5), (&5, &7)],
        );
    }
}
