use kademlia::{CONCURRENCY_PARAM, ROUTING_TABLE_SIZE};
use super::NodeData;

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
}

impl RoutingTable {
    pub fn new() -> Self {
        let mut buckets = Vec::new();
        for _ in 0..ROUTING_TABLE_SIZE {
            buckets.push(RoutingBucket::new());
        }
        RoutingTable{ buckets: buckets }
    }

    pub fn update_node(&mut self, distance: usize, node_data: NodeData) {
        self.buckets[distance].update_node(node_data);
    }

    pub fn get_closest(&mut self, distance: usize) -> Vec<NodeData> {
        let mut ret = Vec::new();
        for i in (0..distance).rev() {
            let nodes = self.buckets[i].get_nodes(CONCURRENCY_PARAM - ret.len());
            ret.extend(nodes);
        }
        ret
    }
}
