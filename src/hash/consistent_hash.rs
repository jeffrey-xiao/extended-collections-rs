use data_structures::treap;
use std::hash::Hash;
use std::vec::Vec;
use std::rc::Rc;
use std::mem;
use util;

#[derive(Debug)]
struct Node<T: Hash, U: Hash> {
    id: Rc<T>,
    index: u64,
    points: Vec<(U, u64)>,
}

struct Ring<T: Hash, U: Hash> {
    nodes: treap::Tree<u64, Node<T, U>>,
    replicas: u64,
}

impl<T: Hash, U: Hash> Ring<T, U> {
    pub fn new(replicas: u64) -> Self {
        Ring {
            nodes: treap::Tree::new(),
            replicas: replicas,
        }
    }

    fn get_next_node(&mut self, hash: &u64) -> Option<(u64, &mut Node<T, U>)> {
        match self.nodes.ceil(hash) {
            Some(&id) => Some((id, self.nodes.get(&id).unwrap())),
            None => match self.nodes.min() {
                Some(&id) => Some((id, self.nodes.get(&id).unwrap())),
                None => None,
            }
        }
    }

    pub fn insert_node(&mut self, id: T) {
        let id_ref = Rc::new(id);
        for i in 0..self.replicas {
            let mut new_node = Node {
                id: Rc::clone(&id_ref),
                index: i,
                points: vec![],
            };
            let new_hash = util::combine_hash(util::gen_hash(&id_ref), util::gen_hash(&i));

            // replaces another node
            if self.nodes.contains(&new_hash) {
                new_node.points = self.nodes.delete(&new_hash).unwrap().1.points;
            }
            // could take some of another node
            else if let Some((hash, &mut Node { ref mut points, .. })) = self.get_next_node(&new_hash) {
                let (old_vec, new_vec) = points.drain(..).partition(|point| {
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

    pub fn size(&self) -> usize {
        self.nodes.size()
    }

    pub fn delete_node(&mut self, id: &T) {
        for i in 0..self.replicas {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            if let Some((_, Node { points, .. })) = self.nodes.delete(&hash) {
                if let Some((_, &mut Node { points: ref mut next_point, .. })) = self.get_next_node(&hash) {
                    for val in points {
                        next_point.push(val);
                    }
                } else {
                    panic!("Error: empty ring after deletion");
                }
            }
        }
    }

    pub fn get_points(&self) -> Vec<(&T, &u64, &Vec<(U, u64)>)> {
        let res = self.nodes.traverse();
        res.iter().map(|node| (&*node.1.id, node.0, &node.1.points)).collect()
    }

    pub fn get_point(&mut self, key: &U) -> &T {
        let hash = util::gen_hash(key);
        if let Some((_, &mut Node { ref id, .. })) = self.get_next_node(&hash) {
            id
        } else {
            panic!("Error: empty ring");
        }
    }

    pub fn add_point(&mut self, key: U) {
        let hash = util::gen_hash(&key);
        match self.nodes.ceil(&hash) {
            Some(&id) => self.nodes.get(&id).unwrap().points.push((key, hash)),
            None => match self.nodes.min() {
                Some(&id) => self.nodes.get(&id).unwrap().points.push((key, hash)),
                None => panic!("Error: Empty Ring"),
            }
        }
    }
}

#[test]
fn wtf() {
    let mut ring = Ring::new(3);
    ring.insert_node(String::from("Client-1"));
    ring.add_point(1);
    ring.insert_node(String::from("Client-2"));
    ring.add_point(2);
    ring.add_point(3);
    ring.add_point(4);
    ring.add_point(5);
    ring.add_point(6);
    ring.add_point(7);
    for i in ring.get_points() {
        println!("{:?}", i);
    }
    ring.insert_node(String::from("Client-3"));
    println!("ADDED");
    for i in ring.get_points() {
        println!("{:?}", i);
    }

    ring.delete_node(&String::from("Client-3"));
    println!("DELETED");
    for i in ring.get_points() {
        println!("{:?}", i);
    }

    ring.delete_node(&String::from("Client-1"));
    println!("DELETED");
    for i in ring.get_points() {
        println!("{:?}", i);
    }
}

#[test]
fn edgy() {
    extern crate rand;
    use std::collections::HashMap;

    let mut ring = Ring::new(10);
    for i in 0..100 {
        ring.insert_node(format!("Client-{}", i));
    }
    for i in 0..10_000 {
        ring.add_point(i);
    }
    let mut stats = HashMap::new();
    for i in ring.get_points() {
        let count = stats.entry(i.0).or_insert(0);
        *count += i.2.len();
    }
    println!("min: {:?}", stats.iter().next());
    println!("min: {:?}", stats.iter().next());
    println!("min: {:?}", stats.iter().next());
}
