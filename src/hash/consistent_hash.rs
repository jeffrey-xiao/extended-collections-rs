use data_structures::treap;
use std::hash::Hash;
use std::collections::HashMap;
use std::vec::Vec;
use std::rc::Rc;
use std::mem;
use util;

#[derive(Debug)]
struct Node<T: Hash, U: Hash + Eq> {
    id: Rc<T>,
    index: u64,
    points: HashMap<U, u64>,
}

struct Ring<T: Hash, U: Hash + Eq> {
    nodes: treap::Tree<u64, Node<T, U>>,
    replicas: u64,
}

impl<T: Hash, U: Hash + Eq> Ring<T, U> {
    pub fn new(replicas: u64) -> Self {
        Ring {
            nodes: treap::Tree::new(),
            replicas: replicas,
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
        self.nodes.size()
    }

    pub fn insert_node(&mut self, id: T) {
        let id_ref = Rc::new(id);
        for i in 0..self.replicas {
            let mut new_node: Node<T, U> = Node {
                id: Rc::clone(&id_ref),
                index: i,
                points: HashMap::new(),
            };
            let new_hash = util::combine_hash(util::gen_hash(&id_ref), util::gen_hash(&i));

            // replaces another node
            if self.nodes.contains(&new_hash) {
                new_node.points = self.nodes.delete(&new_hash).unwrap().1.points;
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

    pub fn delete_node(&mut self, id: &T) {
        for i in 0..self.replicas {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            if let Some((_, Node { points, .. })) = self.nodes.delete(&hash) {
                if let Some((_, &mut Node { points: ref mut next_points, .. })) = self.get_next_node(&hash) {
                    for val in points {
                        next_points.insert(val.0, val.1);
                    }
                } else {
                    panic!("Error: empty ring after deletion");
                }
            }
        }
    }

    pub fn get_points(&mut self, id: &T) -> Vec<&U> {
        let mut ret: Vec<&U> = Vec::new();
        for i in 0..self.replicas {
            let hash = util::combine_hash(util::gen_hash(id), util::gen_hash(&i));
            if let Some(node) = self.nodes.get(&hash) {
                for entry in node.points.iter() {
                    ret.push(entry.0);
                }
            }
        }
        ret
    }

    pub fn get_node(&mut self, key: &U) -> &T {
        let hash = util::gen_hash(key);
        if let Some((_, &mut Node { ref id, .. })) = self.get_next_node(&hash) {
            id
        } else {
            panic!("Error: empty ring");
        }
    }

    pub fn add_point(&mut self, key: U) {
        let hash = util::gen_hash(&key);
        if let Some((_, &mut Node { ref mut points, .. })) = self.get_next_node(&hash) {
            points.insert(key, hash);
        } else {
            panic!("Error: empty ring");
        }
    }

    pub fn delete_point(&mut self, key: &U) {
        let hash = util::gen_hash(key);
        if let Some((_, &mut Node { ref mut points, .. })) = self.get_next_node(&hash) {
            points.remove(key);
        } else {
            panic!("Error: empty ring");
        }
    }

    pub fn iterate(&self) -> Vec<(&T, &u64, &HashMap<U, u64>)> {
        let res = self.nodes.traverse();
        res.iter().map(|node| (&*node.1.id, node.0, &node.1.points)).collect()
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
    for i in ring.iterate() {
        println!("{:?}", i);
    }
    ring.insert_node(String::from("Client-3"));
    println!("ADDED");
    for i in ring.iterate() {
        println!("{:?}", i);
    }

    println!("{:?}", ring.get_node(&3));
    println!("{:?}", ring.get_points(&String::from("Client-2")));
    ring.delete_node(&String::from("Client-3"));
    println!("DELETED");
    for i in ring.iterate() {
        println!("{:?}", i);
    }

    ring.delete_node(&String::from("Client-1"));
    println!("DELETED");
    for i in ring.iterate() {
        println!("{:?}", i);
    }

    ring.delete_point(&7);

    println!("DELETED POINT");
    for i in ring.iterate() {
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
    for i in ring.iterate() {
        let count = stats.entry(i.0).or_insert(0);
        *count += i.2.len();
    }
    println!("min: {:?}", stats.iter().next());
    println!("min: {:?}", stats.iter().next());
    println!("min: {:?}", stats.iter().next());
}
