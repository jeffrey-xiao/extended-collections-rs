use kademlia::{ROUTING_TABLE_SIZE, REPLICATION_PARAM};
use kademlia::node::{NodeData, Key};
use std::sync::Arc;

#[derive(Clone)]
struct RoutingBucket {
    nodes: Vec<NodeData>,
}

impl RoutingBucket {
    pub fn new() -> Self { RoutingBucket{ nodes: Vec::new() } }

    pub fn update_node(&mut self, node_data: NodeData) {
        if let Some(index) = self.nodes.iter().position(|data| *data == node_data) {
            self.nodes.remove(index);
        }
        self.nodes.push(node_data);
        if self.nodes.len() > REPLICATION_PARAM {
            self.nodes.remove(0);
        }
    }

    pub fn get_nodes(&self, count: usize) -> Vec<NodeData> {
        let mut ret = Vec::new();
        for node_data in &self.nodes {
            if ret.len() < count {
                ret.push(node_data.clone());
            }
        }
        ret
    }
}

#[derive(Clone)]
pub struct RoutingTable {
    buckets: Vec<RoutingBucket>,
    node_data: Arc<NodeData>,
}

impl RoutingTable {
    pub fn new(node_data: Arc<NodeData>) -> Self {
        let mut buckets = Vec::new();
        for _ in 0..ROUTING_TABLE_SIZE {
            buckets.push(RoutingBucket::new());
        }
        RoutingTable{ buckets: buckets, node_data: node_data }
    }

    pub fn update_node(&mut self, node_data: NodeData) {
        let key = self.node_data.id.xor(&node_data.id);
        self.buckets[key.get_distance()].update_node(node_data);
    }

    pub fn get_closest(&mut self, key: &Key, count: usize) -> Vec<NodeData> {
        let key = self.node_data.id.xor(key);
        let mut ret = Vec::new();
        for i in (0..key.get_distance()).rev() {
            let nodes = self.buckets[i].get_nodes(count - ret.len());
            ret.extend(nodes);
        }
        ret
    }
}
