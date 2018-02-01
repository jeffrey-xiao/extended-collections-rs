extern crate kademlia;

use std::io;
use std::collections::HashMap;

use kademlia::Node;

fn main() {
    let mut node_map = HashMap::new();
    let mut id = 0;
    let n1 = Node::new(&"localhost".to_string(), &"8900".to_string(), None);
    node_map.insert(id, n1.clone());
    id += 1;
    let n2 = Node::new(&"localhost".to_string(), &"8901".to_string(), Some((*n1.node_data).clone()));
    node_map.insert(id, n2);
    id += 1;

    let input = io::stdin();

    loop {
        let mut buffer = String::new();
        if input.read_line(&mut buffer).is_err() {
            break;
        }
        let args: Vec<&str> = buffer.trim_right().split(' ').collect();
        match args[0].as_ref() {
            "new" => {
                let index: u32 = args[2].parse().unwrap();
                let node_data = (*node_map.get(&index).unwrap().node_data).clone();
                let node = Node::new(
                    &"localhost".to_string(),
                    args[1],
                    Some(node_data),
                );
                node_map.insert(id, node);
                id += 1;
            }
            _ => {}
        }
    }
}
