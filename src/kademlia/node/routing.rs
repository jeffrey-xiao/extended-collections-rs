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

    pub fn get_nodes(&self) -> &[NodeData] {
        self.nodes.as_slice()
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub fn remove_lrs(&mut self) -> Option<NodeData> {
        if self.size() == 0 {
            None
        } else {
            Some(self.nodes.remove(0))
        }
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

    pub fn get_closest(&self, key: &Key, count: usize) -> Vec<NodeData> {
        let key = self.node_data.id.xor(key);
        let mut ret = Vec::new();
        // the distance between target key and keys in [key.get_distance(), ROUTING_TABLE_SIZE]
        // is not necessarily monotonic
        for i in key.get_distance()..ROUTING_TABLE_SIZE {
            ret.extend_from_slice(self.buckets[i].get_nodes());
        }

        if ret.len() < count {
            // the distance between target key and keys in [0, key.get_distance()]
            // is monotonicly decreasing by bucket
            for i in (0..key.get_distance()).rev() {
                ret.extend_from_slice(self.buckets[i].get_nodes());
                if ret.len() >= count {
                    break;
                }
            }
        }

        ret.sort_by_key(|node| node.id.xor(&key).get_distance());
        ret.truncate(count);
        ret
    }

    pub fn remove_lrs(&mut self, key: &Key) -> Option<NodeData> {
        let key = self.node_data.id.xor(key);
        self.buckets[key.get_distance()].remove_lrs()
    }

    pub fn bucket_size(&self, key: &Key) -> usize {
        let key = self.node_data.id.xor(key);
        self.buckets[key.get_distance()].size()
    }
}
