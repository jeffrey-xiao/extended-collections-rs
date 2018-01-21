use util::data_structures::treap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::vec::Vec;

fn generate_hash<T: Hash>(value: &T) -> u64 {
  let mut hasher = DefaultHasher::new();
  value.hash(&mut hasher);
  hasher.finish()
}

fn combine_hash(x: u64, y: u64) -> u64 {
    x ^ y.wrapping_add(0x9e3779b9).wrapping_add(x << 6).wrapping_add(x >> 2)
}

#[derive(Debug, Clone)]
struct Node<T: Hash + Clone> {
    id: T,
    index: u64,
}

impl<T: Hash + Clone> Node<T> {
    pub fn new(id: T, index: u64) -> Self {
        Node {
            id: id,
            index: index,
        }
    }
}

struct Ring<T: Hash + Clone> {
    nodes: treap::Tree<u64, Node<T>>,
    replicas: u64,
}

impl<T: Hash + Clone> Ring<T> {
    pub fn new(replicas: u64) -> Self {
        Ring {
            nodes: treap::Tree::new(),
            replicas: replicas,
        }
    }

    pub fn insert_node(mut self, mut id: T) -> Self {
        for i in 0..self.replicas {
            let node = Node::new(id.clone(), i);
            let hash = combine_hash(generate_hash(&id), generate_hash(&i));
            self.nodes = self.nodes.insert(hash, node);
        }
        self
    }

    pub fn size(&self) -> u32 {
        self.nodes.size()
    }

    pub fn delete_node(mut self, id: &T) -> Self {
        for i in 0..self.replicas {
            let node = Node::new(id.clone(), i);
            let hash = combine_hash(generate_hash(&id), generate_hash(&i));
            self.nodes = self.nodes.delete(hash);
        }
        self
    }

    pub fn get_points(&self) -> Vec<(&T, &u64)> {
        let res = self.nodes.traverse();
        res.iter().map(|node| (&node.1.id, node.0)).collect()
    }
}

#[test]
fn wtf() {
    let mut ring = Ring::new(3);
    let mut a = String::from("ASD");
    ring = ring.insert_node(a);
    a = String::from("BSASB");
    ring = ring.insert_node(a);
    for i in ring.get_points() {
        println!("{:?}", i);
    }
}
