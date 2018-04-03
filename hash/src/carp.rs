use std::f64;
use std::hash::Hash;
use util;

/// A node with an associated weight.
///
/// The distribution of points to nodes is proportional to the weights of the nodes. For example, a
/// node with a weight of 3 will receive approximately three times more points than a node with a
/// weight of 1.
pub struct Node<'a, T: 'a + Hash + Ord> {
    id: &'a T,
    hash: u64,
    weight: f64,
    relative_weight: f64,
}

impl<'a, T: 'a + Hash + Ord> Node<'a, T> {
    pub fn new(id: &'a T, weight: f64) -> Self {
        Node {
            id,
            hash: util::gen_hash(id),
            weight,
            relative_weight: 0f64,
        }
    }
}

/// A hashing ring implemented using the Cache Array Routing Protocol.
///
/// The Cache Array Routing Protocol calculates the relative weight for each node in the ring to
/// distribute points according to their weights.
///
/// # Examples
/// ```
/// use hash::carp::{Node, Ring};
///
/// let mut r = Ring::new(vec![
///     Node::new(&"node-1", 1f64),
///     Node::new(&"node-2", 3f64),
/// ]);
///
/// r.remove_node(&"node-1");
///
/// assert_eq!(r.get_node(&"point-1"), &"node-2");
/// assert_eq!(r.len(), 1);
///
/// let mut iterator = r.iter();
/// assert_eq!(iterator.next(), Some((&"node-2", 3f64)));
/// assert_eq!(iterator.next(), None);
/// ```
pub struct Ring<'a, T: 'a + Hash + Ord> {
    nodes: Vec<Node<'a, T>>,
}

impl<'a, T: 'a + Hash + Ord> Ring<'a, T> {
    fn rebalance(&mut self) {
        let mut rolling_product = 1f64;
        let len = self.nodes.len() as f64;
        for i in 0..self.nodes.len() {
            let index = i as f64;
            let mut res;
            if i == 0 {
                res = (len * self.nodes[i].weight).powf(1f64 / len);
            } else {
                res = (len - index) * (self.nodes[i].weight - self.nodes[i - 1].weight) / rolling_product;
                res += self.nodes[i - 1].relative_weight.powf(len - index);
                res = res.powf(1f64 / (len - index));
            }

            rolling_product *= res;
            self.nodes[i].relative_weight = res;
        }
        if let Some(max_relative_weight) = self.nodes.last().map(|node| node.relative_weight) {
            for node in &mut self.nodes {
                node.relative_weight /= max_relative_weight
            }
        }
    }

    /// Constructs a new, empty `Ring<T>`
    ///
    /// # Examples
    /// ```
    /// use hash::carp::Ring;
    ///
    /// let mut r: Ring<&str> = Ring::new(vec![]);
    /// ```
    pub fn new(mut nodes: Vec<Node<'a, T>>) -> Ring<'a, T> {
        nodes.reverse();
        nodes.sort_by_key(|node| node.id);
        nodes.dedup_by_key(|node| node.id);
        nodes.sort_by(|n, m| {
            if (n.weight - m.weight).abs() < f64::EPSILON {
                n.id.cmp(m.id)
            } else {
                n.weight.partial_cmp(&m.weight).unwrap()
            }
        });
        let mut ret = Ring { nodes };
        ret.rebalance();
        ret
    }

    /// Inserts a node into the ring with a particular weight.
    ///
    /// Increasing the weight will increase the number of expected points mapped to the node. For
    /// example, a node with a weight of three will receive approximately three times more points
    /// than a node with a weight of one.
    ///
    /// # Examples
    /// ```
    /// use hash::carp::{Node, Ring};
    ///
    /// let mut r = Ring::new(vec![
    ///     Node::new(&"node-1", 1f64),
    /// ]);
    ///
    /// r.remove_node(&"node-1");
    /// ```
    pub fn insert_node(&mut self, new_node: Node<'a, T>) {
        if let Some(index) = self.nodes.iter().position(|node| node.id == new_node.id) {
            self.nodes[index] = new_node;
        } else {
            self.nodes.push(new_node);
        }
        self.nodes.sort_by(|n, m| {
            if (n.weight - m.weight).abs() < f64::EPSILON {
                n.id.cmp(m.id)
            } else {
                n.weight.partial_cmp(&m.weight).unwrap()
            }
        });
        self.rebalance();
    }

    /// Removes a node from the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::carp::{Node, Ring};
    ///
    /// let mut r = Ring::new(vec![
    ///     Node::new(&"node-1", 1f64),
    ///     Node::new(&"node-2", 3f64),
    /// ]);
    ///
    /// r.remove_node(&"node-2");
    /// ```
    pub fn remove_node(&mut self, id: &T) {
        if let Some(index) = self.nodes.iter().position(|node| node.id == id) {
            self.nodes.remove(index);
            self.rebalance();
        }
    }

    /// Returns the node associated with a point.
    ///
    /// # Examples
    /// ```
    /// use hash::carp::{Node, Ring};
    ///
    /// let mut r = Ring::new(vec![
    ///     Node::new(&"node-1", 1f64),
    /// ]);
    ///
    /// assert_eq!(r.get_node(&"point-1"), &"node-1");
    /// ```
    pub fn get_node<U: Hash + Eq>(&self, point: &U) -> &'a T {
        let point_hash = util::gen_hash(point);
        self.nodes.iter().map(|node| {
            (
                util::combine_hash(node.hash, point_hash) as f64 * node.relative_weight,
                node.id,
            )
        }).max_by(|n, m| {
            if n == m {
                n.1.cmp(m.1)
            } else {
                n.0.partial_cmp(&m.0).unwrap()
            }
        }).unwrap().1
    }

    /// Returns the number of nodes in the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::carp::{Node, Ring};
    ///
    /// let mut r = Ring::new(vec![
    ///     Node::new(&"node-1", 1f64),
    ///     Node::new(&"node-2", 3f64),
    /// ]);
    ///
    /// assert_eq!(r.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Removes a node from the ring.
    ///
    /// # Examples
    /// ```
    /// use hash::carp::{Node, Ring};
    ///
    /// let mut r = Ring::new(vec![
    ///     Node::new(&"node-1", 1f64),
    ///     Node::new(&"node-2", 3f64),
    /// ]);
    ///
    /// assert_eq!(r.len(), 2);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns an iterator over the ring. The iterator will yield nodes and their weights in
    /// sorted by weight, and then by id.
    /// particular order.
    ///
    /// # Examples
    /// ```
    /// use hash::carp::{Node, Ring};
    ///
    /// let mut r = Ring::new(vec![
    ///     Node::new(&"node-1", 1f64),
    ///     Node::new(&"node-2", 3f64),
    /// ]);
    ///
    /// let mut iterator = r.iter();
    /// assert_eq!(iterator.next(), Some((&"node-1", 1f64)));
    /// assert_eq!(iterator.next(), Some((&"node-2", 3f64)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&'a self) -> Box<Iterator<Item = (&'a T, f64)> + 'a> {
        Box::new(self.nodes.iter().map(|node| (&*node.id, node.weight)))
    }
}

impl<'a, T: Hash + Ord> IntoIterator for &'a Ring<'a, T> {
    type Item = (&'a T, f64);
    type IntoIter = Box<Iterator<Item = (&'a T, f64)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{Node, Ring};
    macro_rules! assert_approx_eq {
        ($a:expr, $b:expr) => ({
            let (a, b) = (&$a, &$b);
            assert!((*a - *b).abs() < 1.0e-6, "{} is not approximately equal to {}", *a, *b);
        })
    }


    #[test]
    fn test_size_empty() {
        let ring: Ring<u32> = Ring::new(vec![]);
        assert!(ring.is_empty());
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn test_correct_weights() {
        let ring = Ring::new(vec![
            Node::new(&0, 0.4),
            Node::new(&1, 0.4),
            Node::new(&2, 0.2),
        ]);
        assert_eq!(ring.nodes[0].id, &2);
        assert_eq!(ring.nodes[1].id, &0);
        assert_eq!(ring.nodes[2].id, &1);
        assert_approx_eq!(ring.nodes[0].relative_weight, 0.7745967);
        assert_approx_eq!(ring.nodes[1].relative_weight, 1.0000000);
        assert_approx_eq!(ring.nodes[2].relative_weight, 1.0000000);
    }

    #[test]
    fn test_new_replace() {
        let ring = Ring::new(vec![
            Node::new(&0, 0.5),
            Node::new(&1, 0.1),
            Node::new(&1, 0.5),
        ]);

        assert_eq!(ring.nodes[0].id, &0);
        assert_eq!(ring.nodes[1].id, &1);
        assert_approx_eq!(ring.nodes[0].relative_weight, 1.0000000);
        assert_approx_eq!(ring.nodes[1].relative_weight, 1.0000000);
    }

    #[test]
    fn test_insert_node() {
        let mut ring = Ring::new(vec![
            Node::new(&0, 0.5),
        ]);
        ring.insert_node(Node::new(&1, 0.5));

        assert_eq!(ring.nodes[0].id, &0);
        assert_eq!(ring.nodes[1].id, &1);
        assert_approx_eq!(ring.nodes[0].relative_weight, 1.0000000);
        assert_approx_eq!(ring.nodes[1].relative_weight, 1.0000000);
    }

    #[test]
    fn test_insert_node_replace() {
        let mut ring = Ring::new(vec![
            Node::new(&0, 0.5),
            Node::new(&1, 0.1),
        ]);
        ring.insert_node(Node::new(&1, 0.5));

        assert_eq!(ring.nodes[0].id, &0);
        assert_eq!(ring.nodes[1].id, &1);
        assert_approx_eq!(ring.nodes[0].relative_weight, 1.0000000);
        assert_approx_eq!(ring.nodes[1].relative_weight, 1.0000000);
    }

    #[test]
    fn test_remove_node() {
        let mut ring = Ring::new(vec![
            Node::new(&0, 0.5),
            Node::new(&1, 0.5),
            Node::new(&2, 0.1),
        ]);
        ring.remove_node(&2);

        assert_eq!(ring.nodes[0].id, &0);
        assert_eq!(ring.nodes[1].id, &1);
        assert_approx_eq!(ring.nodes[0].relative_weight, 1.0000000);
        assert_approx_eq!(ring.nodes[1].relative_weight, 1.0000000);
    }

    #[test]
    fn test_get_node() {
        let ring = Ring::new(vec![
            Node::new(&0, 1.0),
        ]);

        assert_eq!(ring.get_node(&0), &0);
        assert_eq!(ring.get_node(&1), &0);
        assert_eq!(ring.get_node(&2), &0);
    }

    #[test]
    fn test_iter() {
        let ring = Ring::new(vec![
            Node::new(&0, 0.4),
            Node::new(&1, 0.4),
            Node::new(&2, 0.2),
        ]);

        let mut iterator = ring.iter();
        let mut node;

        node = iterator.next().unwrap();
        assert_eq!(node.0, &2);
        assert_approx_eq!(node.1, 0.2);

        node = iterator.next().unwrap();
        assert_eq!(node.0, &0);
        assert_approx_eq!(node.1, 0.4);

        node = iterator.next().unwrap();
        assert_eq!(node.0, &1);
        assert_approx_eq!(node.1, 0.4);

        assert_eq!(iterator.next(), None);
    }
}
