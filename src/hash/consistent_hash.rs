use std::hash::Hash;
use std::collections::HashMap;
use std::vec::Vec;
use std::rc::Rc;
use std::mem;
use std::iter::Iterator;

use data_structures::Treap;
use util;

#[derive(Debug)]
struct Node<T: Hash + Eq, U: Hash + Eq> {
    id: Rc<T>,
    points: HashMap<U, u64>,
}

pub struct Ring<T: Hash + Eq, U: Hash + Eq> {
    nodes: Treap<u64, Node<T, U>>,
    replicas: HashMap<Rc<T>, usize>,
}

impl<T: Hash + Eq, U: Hash + Eq> Ring<T, U> {
    pub fn new() -> Self {
        Ring {
            nodes: Treap::new(),
            replicas: HashMap::new(),
        }
    }

    fn get_next_node(&mut self, hash: &u64) -> Option<(u64, &mut Node<T, U>)> {
        match self.nodes.ceil(hash) {
            Some(&id) => Some((id, self.nodes.get_mut(&id).unwrap())),
            None => match self.nodes.min() {
                Some(&id) => Some((id, self.nodes.get_mut(&id).unwrap())),
                None => None,
            }
        }
    }

    pub fn size(&self) -> usize {
        self.replicas.len()
    }

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

    pub fn get_node(&mut self, key: &U) -> &T {
        let hash = util::gen_hash(key);
        if let Some((_, &mut Node { ref id, .. })) = self.get_next_node(&hash) {
            &**id
        } else {
            panic!("Error: empty ring");
        }
    }

    pub fn insert_point(&mut self, key: U) {
        let hash = util::gen_hash(&key);
        if let Some((_, &mut Node { ref mut points, .. })) = self.get_next_node(&hash) {
            points.insert(key, hash);
        } else {
            panic!("Error: empty ring");
        }
    }

    pub fn remove_point(&mut self, key: &U) {
        let hash = util::gen_hash(key);
        if let Some((_, &mut Node { ref mut points, .. })) = self.get_next_node(&hash) {
            points.remove(key);
        } else {
            panic!("Error: empty ring");
        }
    }

    pub fn iter<'a>(&'a self) -> Box<Iterator<Item=(&'a T, Vec<&'a U>)> + 'a> {
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
    type IntoIter = Box<Iterator<Item=(&'a T, Vec<&'a U>)> + 'a>;

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
    fn test_panic_remove_node() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_node(0, 1);
        ring.remove_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_get_node() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.get_node(&0);
    }

    #[test]
    #[should_panic]
    fn test_panic_insert_point() {
        let mut ring: Ring<u32, u32> = Ring::new();
        ring.insert_point(0);
    }

    #[test]
    #[should_panic]
    fn test_panic_remove_point() {
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
