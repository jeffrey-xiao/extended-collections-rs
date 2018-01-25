use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::vec::Vec;
use std::rc::Rc;
use util;

struct NodeData<U: Hash + Eq> {
    hashes: Vec<u64>,
    points: HashSet<Rc<U>>,
}

struct Ring<T: Hash + Eq, U: Hash + Eq> {
    nodes: HashMap<Rc<T>, NodeData<U>>,
    replicas: u32,
    points: HashMap<Rc<U>, (u64, Rc<T>)>,
}

impl<T: Hash + Eq, U: Hash + Eq> Ring<T, U> {
    pub fn new(replicas: u32) -> Self {
        Ring {
            nodes: HashMap::new(),
            replicas: replicas,
            points: HashMap::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub fn insert_node(&mut self, id: T) {
        let id_ref = Rc::new(id);
        let mut new_node: NodeData<U> = NodeData {
            hashes: Vec::new(),
            points: HashSet::new(),
        };
        for i in 0..self.replicas {
            let hash = util::combine_hash(util::gen_hash(&id_ref), util::gen_hash(&i));
            new_node.hashes.push(hash);
        }
    }
}
