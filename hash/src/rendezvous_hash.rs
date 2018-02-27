use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use util;

/// A hashing ring implementing using rendezvous hashing.
///
/// Rendezvous hashing is based on based on assigning a pseudorandom value to node-point pair.
/// A point is mapped to the node that yields the greatest value associated with the node-point
/// pair.
pub struct Ring<'a, T: 'a + Hash + Eq> {
    nodes: HashMap<&'a T, Vec<u64>>,
}

impl<'a, T: 'a + Hash + Ord> Ring<'a, T> {
    pub fn new() -> Self {
        Ring {
            nodes: HashMap::new(),
        }
    }

    pub fn insert_node(&mut self, id: &'a T, replicas: usize) {
        let hashes = (0..replicas)
            .map(|index| util::combine_hash(util::gen_hash(id), util::gen_hash(&index)))
            .collect();
        self.nodes.insert(id, hashes);
    }

    pub fn remove_node(&mut self, id: &T) {
        self.nodes.remove(id);
    }

    pub fn get_node<U: Hash + Eq>(&self, key: &U) -> &'a T {
        let point_hash = util::gen_hash(key);
        &*self.nodes.iter().map(|entry| {
            (
                entry.1.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap(),
                entry.0,
            )
        }).max().unwrap().1
    }

    fn get_hashes(&self, id: &T) -> Vec<u64> {
        self.nodes[id].clone()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// A client that uses `Ring<T>`.
///
/// # Examples
/// ```
/// use hash::rendezvous_hash::Client;
///
/// let mut r = Client::new();
/// r.insert_node(&"node-1", 3);
/// r.insert_point(&"point-1");
/// r.insert_point(&"point-2");
///
/// assert_eq!(r.len(), 1);
/// assert_eq!(r.get_node(&"point-1"), &"node-1");
///
/// r.remove_point(&"point-2");
/// assert_eq!(r.get_points(&"node-1"), [&"point-1"]);
/// ```
pub struct Client<'a, T: 'a + Hash + Ord, U: 'a + Hash + Eq> {
    ring: Ring<'a, T>,
    nodes: HashMap<&'a T, HashSet<&'a U>>,
    points: HashMap<&'a U, (&'a T, u64)>,
}

impl<'a, T: 'a + Hash + Ord, U: 'a + Hash + Eq> Client<'a, T, U> {
    /// Constructs a new, empty `Client<T, U>`
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r: Client<&str, &str> = Client::new();
    /// ```
    pub fn new() -> Self {
        Client {
            ring: Ring::new(),
            nodes: HashMap::new(),
            points: HashMap::new(),
        }
    }

    /// Returns the number of nodes in the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r: Client<&str, &str> = Client::new();
    ///
    /// r.insert_node(&"node-1", 3);
    /// assert_eq!(r.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
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
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r: Client<&str, &str> = Client::new();
    ///
    /// // "node-2" will receive three times more points than "node-1"
    /// r.insert_node(&"node-1", 1);
    /// r.insert_node(&"node-2", 3);
    /// ```
    pub fn insert_node(&mut self, id: &'a T, replicas: usize) {
        self.ring.insert_node(id, replicas);
        let hashes = self.ring.get_hashes(id);

        let mut new_points = HashSet::new();

        for (point, node_entry) in &mut self.points {
            let (ref mut original_node, ref mut original_score) = *node_entry;
            let point_hash = util::gen_hash(point);
            let max_score = hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap();

            if max_score > *original_score {
                // TODO
                self.nodes.get_mut(original_node).unwrap().remove(point);
                new_points.insert(*point);
                *original_score = max_score;
                *original_node = id;
            }
        }

        self.nodes.insert(id, new_points);
    }

    /// Removes a node and all its replicas from a ring.
    ///
    /// # Panics
    /// Panics if the ring is empty after removal of a node or if the node does not exist.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r: Client<&str, &str> = Client::new();
    ///
    /// r.insert_node(&"node-1", 1);
    /// r.insert_node(&"node-2", 1);
    /// r.remove_node(&"node-1");
    /// ```
    pub fn remove_node(&mut self, id: &T) {
        self.ring.remove_node(id);
        if self.ring.is_empty() {
            panic!("Error: empty ring after deletion");
        }
        if let Some(points) = self.nodes.remove(id) {
            for point in points {
                let new_node = self.ring.get_node(point);
                let hashes = self.ring.get_hashes(new_node);
                let point_hash = util::gen_hash(point);
                let max_score = hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap();

                self.nodes.get_mut(new_node).unwrap().insert(point);
                self.points.insert(point, (new_node, max_score));
            }
        }
    }

    /// Returns the points associated with a node and its replicas.
    ///
    /// # Panics
    /// Panics if the node does not exist.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r: Client<&str, &str> = Client::new();
    ///
    /// r.insert_node(&"node-1", 1);
    /// r.insert_point(&"point-1");
    /// assert_eq!(r.get_points(&"node-1"), [&"point-1"]);
    /// ```
    pub fn get_points(&mut self, id: &T) -> Vec<&U> {
        self.nodes[id].iter().map(|point| *point).collect()
    }

    /// Returns the node associated with a point.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r: Client<&str, &str> = Client::new();
    ///
    /// r.insert_node(&"node-1", 1);
    /// r.insert_point(&"point-1");
    /// assert_eq!(r.get_node(&"point-1"), &"node-1");
    /// ```
    pub fn get_node(&mut self, key: &U) -> &T {
        self.ring.get_node(key)
    }

    /// Inserts a point into the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r = Client::new();
    /// r.insert_node(&"node-1", 1);
    /// r.insert_point(&"point-1");
    /// ```
    pub fn insert_point(&mut self, key: &'a U) {
        let node = self.ring.get_node(key);
        let hashes = self.ring.get_hashes(node);
        let point_hash = util::gen_hash(key);
        let max_score = hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap();
        self.nodes.get_mut(node).unwrap().insert(key);
        self.points.insert(key, (node, max_score));
    }

    /// Removes a point from the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r = Client::new();
    /// r.insert_node(&"node-1", 1);
    /// r.insert_point(&"point-1");
    /// r.remove_point(&"point-1");
    /// ```
    pub fn remove_point(&mut self, key: &U) {
        let node = self.ring.get_node(key);
        self.nodes.get_mut(node).unwrap().remove(key);
        self.points.remove(key);
    }

    /// Returns an iterator over the ring. The iterator will yield nodes and points in no
    /// particular order.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous_hash::Client;
    ///
    /// let mut r = Client::new();
    /// r.insert_node(&"node-1", 1);
    /// r.insert_point(&"point-1");
    ///
    /// let mut iterator = r.iter();
    /// assert_eq!(iterator.next(), Some((&"node-1", vec![&"point-1"])))
    /// ```
    pub fn iter(&'a self) -> Box<Iterator<Item = (&'a T, Vec<&'a U>)> + 'a> {
        Box::new(self.nodes.iter().map(move |ref node_entry| {
            let &(node_id, points) = node_entry;
            (&**node_id, points.iter().map(|point| *point).collect())
        }))
    }
}

impl<'a, T: Hash + Ord, U: Hash + Eq> IntoIterator for &'a Client<'a, T, U> {
    type Item = (&'a T, Vec<&'a U>);
    type IntoIter = Box<Iterator<Item = (&'a T, Vec<&'a U>)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: Hash + Ord, U: Hash + Eq> Default for Client<'a, T, U> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Client;

    #[test]
    fn test_size_empty() {
        let ring: Client<u32, u32> = Client::new();
        assert_eq!(ring.len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_node_empty_ring() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.insert_node(&0, 1);
        ring.remove_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_node_non_existent_node() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.remove_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_get_node_empty_ring() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.get_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_insert_point_empty_ring() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.insert_point(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_point_empty_ring() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.remove_point(&0);
    }

    #[test]
    fn test_insert_node() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.insert_node(&0, 1);
        ring.insert_point(&0);
        ring.insert_node(&1, 1);
        assert_eq!(ring.get_points(&1).as_slice(), [&0u32,]);
    }

    #[test]
    fn test_remove_node() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.insert_node(&0, 1);
        ring.insert_point(&0);
        ring.insert_node(&1, 1);
        ring.remove_node(&1);
        assert_eq!(ring.get_points(&0), [&0,]);
    }

    #[test]
    fn test_get_node() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.insert_node(&0, 3);
        assert_eq!(ring.get_node(&0), &0);
    }

    #[test]
    fn test_insert_point() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.insert_node(&0, 3);
        ring.insert_point(&0);
        assert_eq!(ring.get_points(&0).as_slice(), [&0u32,]);
    }

    #[test]
    fn test_remove_point() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.insert_node(&0, 3);
        ring.insert_point(&0);
        ring.remove_point(&0);
        let expected: [&u32; 0] = [];
        assert_eq!(ring.get_points(&0).as_slice(), expected);
    }

    #[test]
    fn test_iter() {
        let mut ring: Client<u32, u32> = Client::new();
        ring.insert_node(&0, 3);
        ring.insert_point(&1);
        ring.insert_point(&2);
        ring.insert_point(&3);
        ring.insert_point(&4);
        ring.insert_point(&5);
        let mut actual: Vec<(&u32, Vec<&u32>)> = ring.iter().collect();
        actual[0].1.sort();
        assert_eq!(actual[0].0, &0);
        assert_eq!(actual[0].1.as_slice(), [&1, &2, &3, &4, &5]);
    }
}
