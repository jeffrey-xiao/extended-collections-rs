use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use std::rc::Rc;
use util;

struct NodeData<U: Hash + Eq> {
    hashes: Vec<u64>,
    points: HashSet<Rc<U>>,
}

pub struct Ring<T: Hash + Ord, U: Hash + Eq> {
    nodes: HashMap<Rc<T>, NodeData<U>>,
    points: HashMap<Rc<U>, (u64, Rc<T>, u64)>,
}

impl<T: Hash + Ord, U: Hash + Eq> Ring<T, U> {
    pub fn new() -> Self {
        Ring {
            nodes: HashMap::new(),
            points: HashMap::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

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

    pub fn remove_node(&mut self, id: &T) {
        let removed_node_data = self.nodes.remove(id).unwrap();
        if self.nodes.len() == 0 {
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

    pub fn get_points(&mut self, id: &T) -> Vec<&U> {
        self.nodes[id].points.iter().map(|point| &**point).collect()
    }

    pub fn get_node(&mut self, key: &U) -> &T {
        let point_hash = util::gen_hash(key);
        &**self.nodes.iter().map(|entry| {
            (
                entry.1.hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap(),
                entry.0,
            )
        }).max().unwrap().1
    }

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

    pub fn remove_point(&mut self, key: &U) {
        self.nodes.get_mut(&self.points[key].1).unwrap().points.remove(key);
        self.points.remove(key);
    }

    pub fn iterate(&self) -> Vec<(Rc<T>, &HashSet<Rc<U>>)> {
        self.nodes.iter().map(|entry| (entry.0.clone(), &entry.1.points)).collect()
    }

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
