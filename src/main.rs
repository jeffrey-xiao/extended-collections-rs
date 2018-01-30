extern crate code;

use std::io;

use code::kademlia::Node;

fn main() {
    let n1 = Node::new(&"localhost".to_string(), &"8900".to_string(), None);
    let n2 = Node::new(&"localhost".to_string(), &"8901".to_string(), Some((*n1.node_data).clone()));

    let input = io::stdin();
    let mut buffer = String::new();
    input.read_line(&mut buffer).unwrap();
}
