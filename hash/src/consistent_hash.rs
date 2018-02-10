use std::hash::Hash;
use std::collections::HashMap;
use std::vec::Vec;
use std::rc::Rc;
use std::mem;
use std::iter::Iterator;

use data_structures::treap::TreapMap;
use util;

struct Node<T: Hash + Eq, U: Hash + Eq> {
    id: Rc<T>,
    points: HashMap<U, u64>,
}

/// A hashing ring implementing using consistent hashing.
///
/// Consistent hashing is based on mapping each node to a pseudorandom value. In this
/// implementation the pseudorandom is a combination of the hash of the node and the hash of the
/// replica number. A point is also represented as a pseudorandom value and it is mapped to the
/// node with the smallest value that is greater than or equal to the point's value. If such a
/// node does not exist, then the point maps to the node with the smallest value.
///
/// # Examples
/// ```
/// use hash::consistent_hash::Ring;
///
/// let mut r = Ring::new();
/// r.insert_node("node-1", 3);
/// r.insert_point("point-1");
/// r.insert_point("point-2");
///
/// assert_eq!(r.size(), 1);
/// assert_eq!(r.get_node(&"point-1"), &"node-1");
///
/// r.remove_point(&"point-2");
/// assert_eq!(r.get_points(&"node-1"), [&"point-1"]);
/// ```
pub struct Ring<T: Hash + Eq, U: Hash + Eq> {
    nodes: TreapMap<u64, Node<T, U>>,
    replicas: HashMap<Rc<T>, usize>,
}

impl<T: Hash + Eq, U: Hash + Eq> Ring<T, U> {
    /// Constructs a new, empty `Ring<T, U>`
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    /// ```
    pub fn new() -> Self {
        Ring {
            nodes: TreapMap::new(),
            replicas: HashMap::new(),
        }
    }

    fn get_next_node(&mut self, hash: &u64) -> Option<(u64, &mut Node<T, U>)> {
        match self.nodes.ceil(hash) {
            Some(&id) => Some((id, self.nodes.get_mut(&id).unwrap())),
            None => match self.nodes.min() {
                Some(&id) => Some((id, self.nodes.get_mut(&id).unwrap())),
                None => None,
            },
        }
    }

    /// Returns the number of nodes in the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// r.insert_node("node-1", 3);
    /// assert_eq!(r.size(), 1);
    /// ```
    pub fn size(&self) -> usize {
        self.replicas.len()
    }

    /// Inserts a node into the ring with a number of replicas.
    ///
    /// Increasing the number of replicas will increase the number of expected points mapped to the
    /// node. For example, a node with three replicas will receive approximately three times more points
    /// than a node with one replica.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// // "node-2" will receive three times more points than "node-1"
    /// r.insert_node("node-1", 1);
    /// r.insert_node("node-2", 3);
    /// ```
    pub fn insert_node(&mut self, id: T, replicas: usize) {
        let id_ref = Rc::new(id);
        self.replicas.insert(Rc::clone(&id_ref), replicas);
        for i in 0..replicas {
            let mut new_node: Node<T, U> = Node {
                id: Rc::clone(&id_ref),
                points: HashMap::new(),
            };
            let new_hash = util::combine_hash(util::gen_hash(&id_ref), util::gen_hash(&i));

            // replaces another node
            if self.nodes.contains(&new_hash) {
                new_node.points = self.nodes.remove(&new_hash).unwrap().1.points;
            }
            // could take some of another node
            else if let Some((hash, &mut Node { ref mut points, .. })) = self.get_next_node(&new_hash) {
                let (old_vec, new_vec) = points.drain().partition(|point| {
                    if new_hash < hash {
                        new_hash < point.1 && point.1 < hash
                    } else {
                        new_hash < point.1 || point.1 < hash
                    }
                });

                new_node.points = new_vec;
                mem::replace(points, old_vec);
            }
            self.nodes.insert(new_hash, new_node);
        }
    }

    /// Removes a node and all its replicas from a ring.
    ///
    /// # Panics
    /// Panics if the ring is empty after removal of a node or if the node does not exist.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// r.insert_node("node-1", 1);
    /// r.insert_node("node-2", 1);
    /// r.remove_node(&"node-1");
    /// ```
    pub fn remove_node(&mut self, id: &T) {
        for i in 0..self.replicas[id] {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            if let Some((_, Node { points, .. })) = self.nodes.remove(&hash) {
                if let Some((_, &mut Node { points: ref mut next_points, .. })) = self.get_next_node(&hash) {
                    for val in points {
                        next_points.insert(val.0, val.1);
                    }
                } else {
                    panic!("Error: empty ring after deletion");
                }
            }
        }
        self.replicas.remove(id);
    }

    /// Returns the points associated with a node and its replicas.
    ///
    /// # Panics
    /// Panics if the node does not exist.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    /// assert_eq!(r.get_points(&"node-1"), [&"point-1"]);
    /// ```
    pub fn get_points(&self, id: &T) -> Vec<&U> {
        let replicas = self.replicas[id];
        let mut ret: Vec<&U> = Vec::new();
        for i in 0..replicas {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            if let Some(node) = self.nodes.get(&hash) {
                for entry in &node.points {
                    ret.push(entry.0);
                }
            }
        }
        ret
    }

    /// Returns the node associated with a point.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    /// assert_eq!(r.get_node(&"point-1"), &"node-1");
    /// ```
    pub fn get_node(&mut self, key: &U) -> &T {
        let hash = util::gen_hash(key);
        if let Some((_, &mut Node { ref id, .. })) = self.get_next_node(&hash) {
            &**id
        } else {
            panic!("Error: empty ring");
        }
    }

    /// Inserts a point into the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r = Ring::new();
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    /// ```
    pub fn insert_point(&mut self, key: U) {
        let hash = util::gen_hash(&key);
        if let Some((_, &mut Node { ref mut points, .. })) = self.get_next_node(&hash) {
            points.insert(key, hash);
        } else {
            panic!("Error: empty ring");
        }
    }

    /// Removes a point from the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r = Ring::new();
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    /// r.remove_point(&"point-1");
    /// ```
    pub fn remove_point(&mut self, key: &U) {
        let hash = util::gen_hash(key);
        if let Some((_, &mut Node { ref mut points, .. })) = self.get_next_node(&hash) {
            points.remove(key);
        } else {
            panic!("Error: empty ring");
        }
    }

    /// Returns an iterator over the ring. The iterator will yield nodes and points in no
    /// particular order.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r = Ring::new();
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    ///
    /// let mut iterator = r.iter();
    /// assert_eq!(iterator.next(), Some((&"node-1", vec![&"point-1"])))
    /// ```
    pub fn iter<'a>(&'a self) -> Box<Iterator<Item = (&'a T, Vec<&'a U>)> + 'a> {
        Box::new(self.replicas.iter().map(move |ref node_entry| {
            let mut points = Vec::new();
            for i in 0..*node_entry.1 {
                let hash = util::combine_hash(util::gen_hash(&*node_entry.0), util::gen_hash(&i));
                for point_entry in &self.nodes.get(&hash).unwrap().points {
                    points.push(point_entry.0);
                }
            }
            (&**node_entry.0, points)
        }))
    }
}

impl<'a, T: Hash + Eq, U: Hash + Eq> IntoIterator for &'a Ring<T, U> {
    type Item = (&'a T, Vec<&'a U>);
    type IntoIter = Box<Iterator<Item = (&'a T, Vec<&'a U>)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: Hash + Eq, U: Hash + Eq> Default for Ring<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Ring;
    use std::hash::{Hash, Hasher};

    #[test]
    fn test_size_empty() {
        let ring: Ring<u32, u32> = Ring::new();
        assert_eq!(ring.size(), 0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_node_empty_ring() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 1);
        ring.remove_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_node_non_existent_node() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.remove_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_get_node_empty_ring() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.get_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_insert_point_empty_ring() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_point(0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_point_empty_ring() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.remove_point(&0);
    }

    #[derive(PartialEq, Eq)]
    pub struct Key(pub u32);
    impl Hash for Key {
        fn hash<H: Hasher>(&self, state: &mut H) {
            state.write_u32(self.0 / 2);
        }
    }

    #[test]
    fn test_insert_node_replace_node() {
        let mut ring: Ring<Key, u32> = Ring::new();
        ring.insert_node(Key(0), 1);
        ring.insert_point(0);
        ring.insert_node(Key(1), 1);
        assert_eq!(ring.get_points(&Key(1)).as_slice(), [&0u32,]);
    }

    #[test]
    fn test_insert_node_share_node() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 1);
        ring.insert_point(0);
        ring.insert_point(1);
        ring.insert_node(1, 1);
        assert_eq!(ring.get_points(&0).as_slice(), [&1u32,]);
        assert_eq!(ring.get_points(&1).as_slice(), [&0u32,]);
    }

    #[test]
    fn test_remove_node() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 1);
        ring.insert_point(0);
        ring.insert_node(1, 1);
        ring.remove_node(&1);
        assert_eq!(ring.get_points(&0), [&0,]);
    }

    #[test]
    fn test_get_node() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 3);
        assert_eq!(ring.get_node(&0), &0);
    }

    #[test]
    fn test_insert_point() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 3);
        ring.insert_point(0);
        assert_eq!(ring.get_points(&0).as_slice(), [&0u32,]);
    }

    #[test]
    fn test_remove_point() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 3);
        ring.insert_point(0);
        ring.remove_point(&0);
        let expected: [&u32; 0] = [];
        assert_eq!(ring.get_points(&0).as_slice(), expected);
    }

    #[test]
    fn test_iter() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 3);
        ring.insert_point(1);
        ring.insert_point(2);
        ring.insert_point(3);
        ring.insert_point(4);
        ring.insert_point(5);
        let mut actual: Vec<(&u32, Vec<&u32>)> = ring.iter().collect();
        actual[0].1.sort();
        assert_eq!(actual[0].0, &0);
        assert_eq!(actual[0].1.as_slice(), [&1, &2, &3, &4, &5]);
    }
}
