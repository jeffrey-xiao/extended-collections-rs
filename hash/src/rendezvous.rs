use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use util;

/// A hashing ring implementing using rendezvous hashing.
///
/// Rendezvous hashing is based on based on assigning a pseudorandom value to node-point pair.
/// A point is mapped to the node that yields the greatest value associated with the node-point
/// pair.
///
/// # Examples
/// ```
/// use hash::rendezvous::Ring;
///
/// let mut r = Ring::new();
///
/// r.insert_node(&"node-1", 1);
/// r.insert_node(&"node-2", 3);
///
/// r.remove_node(&"node-1");
///
/// assert_eq!(r.get_node(&"point-1"), &"node-2");
/// assert_eq!(r.len(), 1);
///
/// let mut iterator = r.iter();
/// assert_eq!(iterator.next(), Some((&"node-2", 3)));
/// assert_eq!(iterator.next(), None);
/// ```
pub struct Ring<'a, T: 'a + Hash + Ord> {
    nodes: HashMap<&'a T, Vec<u64>>,
}

impl<'a, T: 'a + Hash + Ord> Ring<'a, T> {
    /// Constructs a new, empty `Ring<T>`
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    /// ```
    pub fn new() -> Self {
        Ring {
            nodes: HashMap::new(),
        }
    }

    /// Inserts a node into the ring with a number of replicas.
    ///
    /// Increasing the number of replicas will increase the number of expected points mapped to the
    /// node. For example, a node with three replicas will receive approximately three times more points
    /// than a node with one replica.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// // "node-2" will receive three times more points than "node-1"
    /// r.insert_node(&"node-1", 1);
    /// r.insert_node(&"node-2", 3);
    /// ```
    pub fn insert_node(&mut self, id: &'a T, replicas: usize) {
        let hashes = (0..replicas)
            .map(|index| util::combine_hash(util::gen_hash(id), util::gen_hash(&index)))
            .collect();
        self.nodes.insert(id, hashes);
    }

    /// Removes a node and all its replicas from the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// r.insert_node(&"node-1", 1);
    /// r.insert_node(&"node-2", 1);
    /// r.remove_node(&"node-2");
    /// ```
    pub fn remove_node(&mut self, id: &T) {
        self.nodes.remove(id);
    }

    /// Returns the node associated with a point.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// r.insert_node(&"node-1", 1);
    /// assert_eq!(r.get_node(&"point-1"), &"node-1");
    /// ```
    pub fn get_node<U: Hash + Eq>(&self, id: &U) -> &'a T {
        let point_hash = util::gen_hash(id);
        self.nodes.iter().map(|entry| {
            (
                entry.1.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap(),
                entry.0,
            )
        }).max().unwrap().1
    }

    fn get_hashes(&self, id: &T) -> Vec<u64> {
        self.nodes[id].clone()
    }

    /// Returns the number of nodes in the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// r.insert_node(&"node-1", 3);
    /// assert_eq!(r.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// assert!(r.is_empty());
    /// r.insert_node(&"node-1", 3);
    /// assert!(!r.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns an iterator over the ring. The iterator will yield nodes and the replica count in
    /// no particular order.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Ring;
    ///
    /// let mut r = Ring::new();
    /// r.insert_node(&"node-1", 1);
    ///
    /// let mut iterator = r.iter();
    /// assert_eq!(iterator.next(), Some((&"node-1", 1)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&'a self) -> Box<Iterator<Item = (&'a T, usize)> + 'a> {
        Box::new(self.nodes.iter().map(|node_entry| {
            let (id, hashes) = node_entry;
            (&**id, hashes.len())
        }))
    }
}

impl<'a, T: Hash + Ord> IntoIterator for &'a Ring<'a, T> {
    type Item = (&'a T, usize);
    type IntoIter = Box<Iterator<Item = (&'a T, usize)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: 'a + Hash + Ord> Default for Ring<'a, T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A client that uses `Ring<T>`.
///
/// # Examples
/// ```
/// use hash::rendezvous::Client;
///
/// let mut c = Client::new();
/// c.insert_node(&"node-1", 3);
/// c.insert_point(&"point-1");
/// c.insert_point(&"point-2");
///
/// assert_eq!(c.len(), 1);
/// assert_eq!(c.get_node(&"point-1"), &"node-1");
///
/// c.remove_point(&"point-2");
/// assert_eq!(c.get_points(&"node-1"), [&"point-1"]);
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
    /// use hash::rendezvous::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    /// ```
    pub fn new() -> Self {
        Client {
            ring: Ring::new(),
            nodes: HashMap::new(),
            points: HashMap::new(),
        }
    }

    /// Inserts a node into the ring with a number of replicas.
    ///
    /// Increasing the number of replicas will increase the number of expected points mapped to the
    /// node. For example, a node with three replicas will receive approximately three times more points
    /// than a node with one replica.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// // "node-2" will receive three times more points than "node-1"
    /// c.insert_node(&"node-1", 1);
    /// c.insert_node(&"node-2", 3);
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
                self.nodes.get_mut(original_node).unwrap().remove(point);
                new_points.insert(*point);
                *original_score = max_score;
                *original_node = id;
            }
        }

        self.nodes.insert(id, new_points);
    }

    /// Removes a node and all its replicas from the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty after removal of a node or if the node does not exist.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// c.insert_node(&"node-1", 1);
    /// c.insert_node(&"node-2", 1);
    /// c.remove_node(&"node-1");
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
    /// use hash::rendezvous::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    /// assert_eq!(c.get_points(&"node-1"), [&"point-1"]);
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
    /// use hash::rendezvous::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    /// assert_eq!(c.get_node(&"point-1"), &"node-1");
    /// ```
    pub fn get_node(&mut self, point: &U) -> &T {
        self.ring.get_node(point)
    }

    /// Inserts a point into the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Client;
    ///
    /// let mut c = Client::new();
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    /// ```
    pub fn insert_point(&mut self, point: &'a U) {
        let node = self.ring.get_node(point);
        let hashes = self.ring.get_hashes(node);
        let point_hash = util::gen_hash(point);
        let max_score = hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap();
        self.nodes.get_mut(node).unwrap().insert(point);
        self.points.insert(point, (node, max_score));
    }

    /// Removes a point from the ring.
    ///
    /// # Panics
    /// Panics if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Client;
    ///
    /// let mut c = Client::new();
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    /// c.remove_point(&"point-1");
    /// ```
    pub fn remove_point(&mut self, point: &U) {
        let node = self.ring.get_node(point);
        self.nodes.get_mut(node).unwrap().remove(point);
        self.points.remove(point);
    }

    /// Returns the number of nodes in the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// c.insert_node(&"node-1", 3);
    /// assert_eq!(c.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// assert!(c.is_empty());
    /// c.insert_node(&"node-1", 3);
    /// assert!(!c.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Returns an iterator over the ring. The iterator will yield nodes and points in no
    /// particular order.
    ///
    /// # Examples
    /// ```
    /// use hash::rendezvous::Client;
    ///
    /// let mut c = Client::new();
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    ///
    /// let mut iterator = c.iter();
    /// assert_eq!(iterator.next(), Some((&"node-1", vec![&"point-1"])));
    /// assert_eq!(iterator.next(), None);
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
        let client: Client<u32, u32> = Client::new();
        assert!(client.is_empty());
        assert_eq!(client.len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_node_empty_client() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_node(&0, 1);
        client.remove_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_node_non_existent_node() {
        let mut client: Client<u32, u32> = Client::new();
        client.remove_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_get_node_empty_client() {
        let mut client: Client<u32, u32> = Client::new();
        client.get_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_insert_point_empty_client() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_point(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_point_empty_client() {
        let mut client: Client<u32, u32> = Client::new();
        client.remove_point(&0);
    }

    #[test]
    fn test_insert_node() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_node(&0, 1);
        client.insert_point(&0);
        client.insert_node(&1, 1);
        assert_eq!(client.get_points(&1).as_slice(), [&0u32,]);
    }

    #[test]
    fn test_remove_node() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_node(&0, 1);
        client.insert_point(&0);
        client.insert_node(&1, 1);
        client.remove_node(&1);
        assert_eq!(client.get_points(&0), [&0,]);
    }

    #[test]
    fn test_get_node() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_node(&0, 3);
        assert_eq!(client.get_node(&0), &0);
    }

    #[test]
    fn test_insert_point() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_node(&0, 3);
        client.insert_point(&0);
        assert_eq!(client.get_points(&0).as_slice(), [&0u32,]);
    }

    #[test]
    fn test_remove_point() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_node(&0, 3);
        client.insert_point(&0);
        client.remove_point(&0);
        let expected: [&u32; 0] = [];
        assert_eq!(client.get_points(&0).as_slice(), expected);
    }

    #[test]
    fn test_iter() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_node(&0, 3);
        client.insert_point(&1);
        client.insert_point(&2);
        client.insert_point(&3);
        client.insert_point(&4);
        client.insert_point(&5);
        let mut actual: Vec<(&u32, Vec<&u32>)> = client.iter().collect();
        actual[0].1.sort();
        assert_eq!(actual[0].0, &0);
        assert_eq!(actual[0].1.as_slice(), [&1, &2, &3, &4, &5]);
    }
}
