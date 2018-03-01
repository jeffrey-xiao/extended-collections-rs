use std::hash::Hash;

pub struct Node<'a, T: 'a + Hash + Ord> {
    id: &'a T,
    weight: f32,
    relative_weight: f32,
}

pub struct Ring<'a, T: 'a + Hash + Ord> {
    nodes: Vec<Node<'a, T>>,
}

impl<'a, T: 'a + Hash + Ord> Ring<'a, T> {
    fn rebalance(&mut self) {
        let mut rolling_product = 1f32;
        let len = self.nodes.len() as f32;
        for i in 0..self.nodes.len() {
            let index = i as f32;
            let mut res;
            if i == 0 {
                res = (len * self.nodes[i].weight).powf(1f32 / len);
            } else {
                res = (len - index) * (self.nodes[i].weight - self.nodes[i - 1].weight) / rolling_product;
                res += self.nodes[i].relative_weight.powf(len - index);
                res = res.powf(1f32 / (len - index));
            }

            rolling_product *= res;
            self.nodes[i].relative_weight = res;
        }
    }

    pub fn new(mut nodes: Vec<Node<'a, T>>) -> Ring<'a, T> {
        nodes.sort_by_key(|node| node.id);
        nodes.dedup_by_key(|node| node.id);
        nodes.sort_by(|n, m| n.weight.partial_cmp(&m.weight).unwrap());
        let mut ret = Ring { nodes };
        ret.rebalance();
        ret
    }
}
