use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use std::rc::Rc;
use util;

struct NodeData<U: Hash + Eq> {
    hashes: Vec<u64>,
    points: HashSet<Rc<U>>,
}

/// A hashing ring implementing using rendezvous hashing.
///
/// Rendezvous hashing is based on based on assigning a pseudorandom value to node-point pair.
/// A point is mapped to the node that yields the greatest value associated with the node-point
/// pair.
///
/// # Examples
/// ```
/// use code::hash::rendezvous_hash::Ring;
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
pub struct Ring<T: Hash + Ord, U: Hash + Eq> {
    nodes: HashMap<Rc<T>, NodeData<U>>,
    points: HashMap<Rc<U>, (u64, Rc<T>, u64)>,
}

impl<T: Hash + Ord, U: Hash + Eq> Ring<T, U> {
    /// Constructs a new, empty `Ring<T, U>`
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    /// ```
    pub fn new() -> Self {
        Ring {
            nodes: HashMap::new(),
            points: HashMap::new(),
        }
    }

    /// Returns the number of nodes in the ring.
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// r.insert_node("node-1", 3);
    /// assert_eq!(r.size(), 1);
    /// ```
    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    /// Inserts a node into the ring with a number of replicas.
    ///
    /// Increasing the number of replicas will increase the number of expected points mapped to the
    /// node. For example, a node with three replicas will receive approximately three times more points
    /// than a node with one replica.
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// // "node-2" will receive three times more points than "node-1"
    /// r.insert_node("node-1", 1);
    /// r.insert_node("node-2", 3);
    /// ```
    pub fn insert_node(&mut self, id: T, replicas: usize) {
        let mut new_node: NodeData<U> = NodeData {
            hashes: Vec::new(),
            points: HashSet::new(),
        };

        for i in 0..replicas {
            let hash = util::combine_hash(util::gen_hash(&id), util::gen_hash(&i));
            new_node.hashes.push(hash);
        }

        let id_ref = Rc::new(id);
        for (point, node) in &mut self.points {
            let original_node = node.clone();
            let point_hash = node.2;
            for i in 0..replicas {
                let score = (util::combine_hash(new_node.hashes[i], point_hash), id_ref.clone(), point_hash);
                if *node < score {
                    *node = score;
                }
            }
            if original_node != *node {
                self.nodes.get_mut(&original_node.1).unwrap().points.remove(point);
                new_node.points.insert(point.clone());
            }
        }
        self.nodes.insert(id_ref, new_node);
    }

    /// Removes a node and all its replicas from a ring.
    ///
    /// # Panics
    /// Panics if the ring is empty after removal of a node or if the node does not exist.
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// r.insert_node("node-1", 1);
    /// r.insert_node("node-2", 1);
    /// r.remove_node(&"node-1");
    /// ```
    pub fn remove_node(&mut self, id: &T) {
        let removed_node_data = self.nodes.remove(id).unwrap();
        if self.nodes.is_empty() {
            panic!("Error: empty ring after deletion");
        }
        for point in removed_node_data.points {
            let point_hash = self.points[&point].2;
            let max_score = self.nodes.iter().map(|entry| {
                (
                    entry.1.hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap(),
                    entry.0.clone(),
                    point_hash,
                )
            }).max().unwrap();

            self.nodes.get_mut(&max_score.1).unwrap().points.insert(Rc::clone(&point));
            self.points.insert(Rc::clone(&point), max_score);
        }
    }

    /// Returns the points associated with a node and its replicas.
    ///
    /// # Panics
    /// Panics if the node does not exist.
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    /// assert_eq!(r.get_points(&"node-1"), [&"point-1"]);
    /// ```
    pub fn get_points(&mut self, id: &T) -> Vec<&U> {
        self.nodes[id].points.iter().map(|point| &**point).collect()
    }

    /// Returns the node associated with a point.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r: Ring<&str, &str> = Ring::new();
    ///
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    /// assert_eq!(r.get_node(&"point-1"), &"node-1");
    /// ```
    pub fn get_node(&mut self, key: &U) -> &T {
        let point_hash = util::gen_hash(key);
        &**self.nodes.iter().map(|entry| {
            (
                entry.1.hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap(),
                entry.0,
            )
        }).max().unwrap().1
    }

    /// Inserts a point into the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r = Ring::new();
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    /// ```
    pub fn insert_point(&mut self, key: U) {
        let point_hash = util::gen_hash(&key);
        let key_ref = Rc::new(key);
        let max_score = self.nodes.iter().map(|entry| {
            (
                entry.1.hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap(),
                entry.0.clone(),
                point_hash,
            )
        }).max().unwrap();
        self.nodes.get_mut(&max_score.1).unwrap().points.insert(Rc::clone(&key_ref));
        self.points.insert(Rc::clone(&key_ref), max_score);
    }

    /// Removes a point from the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r = Ring::new();
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    /// r.remove_point(&"point-1");
    /// ```
    pub fn remove_point(&mut self, key: &U) {
        self.nodes.get_mut(&self.points[key].1).unwrap().points.remove(key);
        self.points.remove(key);
    }

    /// Returns an iterator over the ring. The iterator will yield nodes and points in no
    /// particular order.
    ///
    /// # Examples
    /// ```
    /// use code::hash::rendezvous_hash::Ring;
    ///
    /// let mut r = Ring::new();
    /// r.insert_node("node-1", 1);
    /// r.insert_point("point-1");
    ///
    /// let mut iterator = r.iter();
    /// assert_eq!(iterator.next(), Some((&"node-1", vec![&"point-1"])))
    /// ```
    pub fn iter<'a>(&'a self) -> Box<Iterator<Item=(&'a T, Vec<&'a U>)> + 'a> {
        Box::new(self.nodes.iter().map(move |ref node_entry| {
            let &(node_id, node_data) = node_entry;
            let mut points = Vec::new();
            for point in &node_data.points {
                points.push(&**point);
            }
            (&**node_id, points)
        }))
    }
}

impl<'a, T: Hash + Ord, U: Hash + Eq> IntoIterator for &'a Ring<T, U> {
    type Item = (&'a T, Vec<&'a U>);
    type IntoIter = Box<Iterator<Item=(&'a T, Vec<&'a U>)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: Hash + Ord, U: Hash + Eq> Default for Ring<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Ring;

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

    #[test]
    fn test_insert_node() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 1);
        ring.insert_point(0);
        ring.insert_node(1, 1);
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
