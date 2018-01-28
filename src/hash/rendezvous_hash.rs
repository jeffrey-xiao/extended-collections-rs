use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use std::rc::Rc;
use util;

struct NodeData<U: Hash + Eq> {
    hashes: Vec<u64>,
    points: HashSet<Rc<U>>,
}

struct Ring<T: Hash + Ord, U: Hash + Eq> {
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

    pub fn get_node(&mut self, key: &U) -> Rc<T> {
        let point_hash = util::gen_hash(key);
        self.nodes.iter().map(|entry| {
            (
                entry.1.hashes.iter().map(|hash| util::combine_hash(*hash, point_hash)).max().unwrap(),
                entry.0.clone(),
            )
        }).max().unwrap().1
    }

    pub fn add_point(&mut self, key: U) {
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

#[test]
fn int_test() {
    let mut ring = Ring::new();
    ring.insert_node(String::from("Client-1"), 3);
    ring.add_point(1);
    ring.insert_node(String::from("Client-2"), 3);
    ring.add_point(2);
    ring.add_point(3);
    ring.add_point(4);
    ring.add_point(5);
    ring.add_point(6);
    ring.add_point(7);
    for i in &ring {
        println!("{:?}", i);
    }
    ring.insert_node(String::from("Client-3"), 3);
    println!("ADDED");
    for i in &ring {
        println!("{:?}", i);
    }

    println!("{:?}", ring.get_node(&3));
    println!("{:?}", ring.get_points(&String::from("Client-2")));
    ring.remove_node(&String::from("Client-3"));
    println!("DELETED");
    for i in &ring {
        println!("{:?}", i);
    }

    ring.remove_node(&String::from("Client-1"));
    println!("DELETED");
    for i in &ring {
        println!("{:?}", i);
    }

    ring.remove_point(&7);

    println!("DELETED POINT");
    for i in &ring {
        println!("{:?}", i);
    }
}

#[test]
fn stats() {
    extern crate rand;
    use std::collections::HashMap;

    let mut ring = Ring::new();
    for i in 0..100 {
        ring.insert_node(format!("Client-{}", i), 3);
    }
    for i in 0..10_000 {
        ring.add_point(i);
    }
    let mut stats = HashMap::new();
    for i in &ring {
        let count = stats.entry(i.0).or_insert(0);
        *count += i.1.len();
    }
    println!("min: {:?}", stats.iter().map(|entry| entry.1).min());
    println!("max: {:?}", stats.iter().map(|entry| entry.1).max());
}
