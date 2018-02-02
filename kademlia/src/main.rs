extern crate kademlia;

use std::io;
use std::collections::HashMap;

use kademlia::Node;
use kademlia::node::node_data::Key;
use kademlia::protocol::Message;

fn main() {
    let mut node_map = HashMap::new();
    let mut id = 0;
    for i in 0..10 {
        if i == 0 {
            let n = Node::new(&"localhost".to_string(), &(8900 + i).to_string(), None);
            node_map.insert(id, n.clone());
        } else {
            let n = Node::new(&"localhost".to_string(), &(8900 + i).to_string(), Some((*node_map[&(i - 1)].node_data).clone()));
            node_map.insert(id, n.clone());
        }
        id += 1;
    }

    // for i in 0..8 {
    //     let node_data = node_map[&i].node_data.clone();
    //     node_map[&i].protocol.send_message(&Message::Kill, &node_data);
    // }
    // println!("KILLED NODES -----------------------");
    // let n = Node::new(&"localhost".to_string(), &(8900 + 10).to_string(), Some((*node_map[&(10 - 1)].node_data).clone()));
    // node_map.insert(id, n.clone());
    // id += 1;

    let input = io::stdin();

    loop {
        let mut buffer = String::new();
        if input.read_line(&mut buffer).is_err() {
            break;
        }
        let args: Vec<&str> = buffer.trim_right().split(' ').collect();
        match args[0] {
            "new" => {
                let index: u32 = args[1].parse().unwrap();
                let node_data = (*node_map[&index].node_data).clone();
                let node = Node::new(
                    &"localhost".to_string(),
                    &(8900 + id).to_string(),
                    Some(node_data),
                );
                node_map.insert(id, node);
                id += 1;
            },
            "insert" => {
                let index: u32 = args[1].parse().unwrap();
                let key = Key::rand();
                let value = args[2].to_string();
                node_map.get_mut(&index).unwrap().insert(key, value);
                println!("{:?}", node_map.get_mut(&index).unwrap().get(&key));
            },
            _ => {}
        }
    }
}
