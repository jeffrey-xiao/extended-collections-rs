use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use std::mem;
use std::iter::Iterator;

use data_structures::treap::TreapMap;
use util;

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
pub struct Ring<'a, T: 'a + Hash + Eq> {
    nodes: TreapMap<u64, &'a T>,
    replicas: HashMap<&'a T, usize>,
}

impl<'a, T: 'a + Hash + Eq> Ring<'a, T> {
    /// Constructs a new, empty `Ring<T>`
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    /// ```
    pub fn new() -> Self {
        Ring {
            nodes: TreapMap::new(),
            replicas: HashMap::new(),
        }
    }

    fn get_next_node(&mut self, hash: &u64) -> Option<&T> {
        match self.nodes.ceil(hash) {
            Some(&hash) => Some(&*self.nodes[&hash]),
            None => match self.nodes.min() {
                Some(&hash) => Some(&*self.nodes[&hash]),
                None => None,
            },
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
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// // "node-2" will receive three times more points than "node-1"
    /// r.insert_node(&"node-1", 1);
    /// r.insert_node(&"node-2", 3);
    /// ```
    pub fn insert_node(&mut self, id: &'a T, replicas: usize) {
        for i in 0..replicas {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            self.nodes.insert(hash, id);
        }
        self.replicas.insert(id, replicas);
    }

    /// Removes a node and all its replicas from a ring.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// r.insert_node(&"node-1", 1);
    /// r.insert_node(&"node-2", 1);
    /// r.remove_node(&"node-2");
    /// ```
    pub fn remove_node(&mut self, id: &T) {
        for i in 0..self.replicas[id] {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            let should_remove = {
                if let Some(existing_id) = self.nodes.get(&hash) {
                    **existing_id == *id
                } else {
                    false
                }
            };

            if should_remove {
                self.nodes.remove(&hash);
            }
        }
        self.replicas.remove(id);
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
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// r.insert_node(&"node-1", 1);
    /// assert_eq!(r.get_node(&"point-1"), &"node-1");
    /// ```
    pub fn get_node<U: Hash + Eq>(&mut self, key: &U) -> &T {
        let hash = util::gen_hash(key);
        if let Some(node) = self.get_next_node(&hash) {
            &*node
        } else {
            panic!("Error: empty ring");
        }
    }

    fn contains_point(&self, index: u64) -> bool {
        self.nodes.contains_key(&index)
    }

    fn get_replica_count(&self, id: &T) -> usize {
        self.replicas[id]
    }

    /// Returns the number of nodes in the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// r.insert_node(&"node-1", 3);
    /// assert_eq!(r.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    /// Returns `true` if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new();
    ///
    /// assert!(r.is_empty());
    /// r.insert_node(&"node-1", 3);
    /// assert!(!r.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    /// Returns an iterator over the ring. The iterator will yield nodes and the replica count in
    /// no particular order.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Ring;
    ///
    /// let mut r = Ring::new();
    /// r.insert_node(&"node-1", 1);
    ///
    /// let mut iterator = r.iter();
    /// assert_eq!(iterator.next(), Some((&"node-1", 1)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&'a self) -> Box<Iterator<Item = (&'a T, usize)> + 'a> {
        Box::new(self.replicas.iter().map(|replica| {
            let (id, replica_count) = replica;
            (&**id, *replica_count)
        }))
    }
}

impl<'a, T: Hash + Eq> IntoIterator for &'a Ring<'a, T> {
    type Item = (&'a T, usize);
    type IntoIter = Box<Iterator<Item = (&'a T, usize)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: 'a + Hash + Eq> Default for Ring<'a, T> {
    fn default() -> Self {
        Self::new()
    }
}

/// A client that uses `Ring<T>`.
///
/// # Examples
/// ```
/// use hash::consistent_hash::Client;
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
pub struct Client<'a, T: 'a + Hash + Eq, U: 'a + Hash + Eq> {
    ring: Ring<'a, T>,
    data: TreapMap<u64, HashSet<&'a U>>,
}

impl<'a, T: 'a + Hash + Eq, U: Hash + Eq> Client<'a, T, U> {
    /// Constructs a new, empty `Client<T, U>`
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    /// ```
    pub fn new() -> Self {
        Client {
            ring: Ring::new(),
            data: TreapMap::new(),
        }
    }

    fn get_next_node(&mut self, hash: &u64) -> Option<(u64, &mut HashSet<&'a U>)> {
        match self.data.ceil(hash) {
            Some(&hash) => Some((hash, &mut self.data[&hash])),
            None => match self.data.min() {
                Some(&hash) => Some((hash, &mut self.data[&hash])),
                None => None,
            },
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
    /// use hash::consistent_hash::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// // "node-2" will receive three times more points than "node-1"
    /// c.insert_node(&"node-1", 1);
    /// c.insert_node(&"node-2", 3);
    /// ```
    pub fn insert_node(&mut self, id: &'a T, replicas: usize) {
        let new_hashes = (0..replicas)
            .map(|replica| util::combine_hash(util::gen_hash(&id), util::gen_hash(&replica)))
            .collect::<Vec<u64>>();
        self.ring.insert_node(id, replicas);
        for new_hash in new_hashes {
            let mut new_points = HashSet::new();
            // if hash already exists, then no additional work is needed to be done
            if !self.data.contains_key(&new_hash) {
                if let Some((hash, points)) = self.get_next_node(&new_hash) {
                    let (old_set, new_set) = points.drain().partition(|point| {
                        let point_hash = util::gen_hash(point);
                        if new_hash < hash {
                            new_hash < point_hash && point_hash < hash
                        } else {
                            new_hash < point_hash || point_hash < hash
                        }
                    });

                    mem::replace(points, old_set);
                    new_points = new_set;
                }
                self.data.insert(new_hash, new_points);
            }
        }
    }

    /// Removes a node and all its replicas from a ring.
    ///
    /// # Panics
    /// Panics if the ring is empty after removal of a node or if the node does not exist.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// c.insert_node(&"node-1", 1);
    /// c.insert_node(&"node-2", 1);
    /// c.remove_node(&"node-2");
    /// ```
    pub fn remove_node(&mut self, id: &T) {
        let replicas = self.ring.get_replica_count(id);
        self.ring.remove_node(id);
        for i in 0..replicas {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            if !self.ring.contains_point(hash) {
                if let Some((_, mut points)) = self.data.remove(&hash) {
                    if let Some((_, next_points)) = self.get_next_node(&hash) {
                        next_points.extend(points);
                    } else {
                        panic!("Error: empty ring after deletion");
                    }
                }
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
    /// use hash::consistent_hash::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    /// assert_eq!(c.get_points(&"node-1"), [&"point-1"]);
    /// ```
    pub fn get_points(&self, id: &T) -> Vec<&U> {
        let mut ret: Vec<&U> = Vec::new();
        for i in 0..self.ring.get_replica_count(id) {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            if let Some(points) = self.data.get(&hash) {
                ret.extend(points.iter());
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
    /// use hash::consistent_hash::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    /// assert_eq!(c.get_node(&"point-1"), &"node-1");
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
    /// use hash::consistent_hash::Client;
    ///
    /// let mut c = Client::new();
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    /// ```
    pub fn insert_point(&mut self, key: &'a U) {
        let hash = util::gen_hash(key);
        if let Some((_, points)) = self.get_next_node(&hash) {
            points.insert(key);
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
    /// use hash::consistent_hash::Client;
    ///
    /// let mut c = Client::new();
    /// c.insert_node(&"node-1", 1);
    /// c.insert_point(&"point-1");
    /// c.remove_point(&"point-1");
    /// ```
    pub fn remove_point(&mut self, key: &U) {
        let hash = util::gen_hash(&key);
        if let Some((_, points)) = self.get_next_node(&hash) {
            points.remove(key);
        } else {
            panic!("Error: empty ring");
        }
    }

    /// Returns the number of nodes in the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Client;
    ///
    /// let mut c: Client<&str, &str> = Client::new();
    ///
    /// c.insert_node(&"node-1", 3);
    /// assert_eq!(c.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Returns `true` if the ring is empty.
    ///
    /// # Examples
    /// ```
    /// use hash::consistent_hash::Client;
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
    /// use hash::consistent_hash::Client;
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
        Box::new(self.ring.iter().map(move |ref replica| {
            let mut points = Vec::new();
            for i in 0..replica.1 {
                let hash = util::combine_hash(util::gen_hash(&*replica.0), util::gen_hash(&i));
                points.extend(self.data.get(&hash).unwrap())
            }
            (replica.0, points)
        }))
    }
}

impl<'a, T: Hash + Eq, U: Hash + Eq> IntoIterator for &'a Client<'a, T, U> {
    type Item = (&'a T, Vec<&'a U>);
    type IntoIter = Box<Iterator<Item = (&'a T, Vec<&'a U>)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: Hash + Eq, U: Hash + Eq> Default for Client<'a, T, U> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Client;
    use std::hash::{Hash, Hasher};

    #[test]
    fn test_size_empty() {
        let client: Client<u32, u32> = Client::new();
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

    #[derive(PartialEq, Eq)]
    pub struct Key(pub u32);
    impl Hash for Key {
        fn hash<H: Hasher>(&self, state: &mut H) {
            state.write_u32(self.0 / 2);
        }
    }

    #[test]
    fn test_insert_node_replace_node() {
        let mut client: Client<Key, u32> = Client::new();
        client.insert_node(&Key(0), 1);
        client.insert_point(&0);
        client.insert_node(&Key(1), 1);
        assert_eq!(client.get_points(&Key(1)).as_slice(), [&0u32,]);
    }

    #[test]
    fn test_insert_node_share_node() {
        let mut client: Client<u32, u32> = Client::new();
        client.insert_node(&0, 1);
        client.insert_point(&0);
        client.insert_point(&1);
        client.insert_node(&1, 1);
        assert_eq!(client.get_points(&0).as_slice(), [&1u32,]);
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
