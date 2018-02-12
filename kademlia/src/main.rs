#[macro_use]
extern crate log;
extern crate kademlia;
extern crate simplelog;
extern crate sha3;

use simplelog::{CombinedLogger, TermLogger, Level, LevelFilter, Config};
use std::io;
use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use sha3::{Digest, Sha3_256};

use kademlia::Node;
use kademlia::key::Key;

use std::convert::AsMut;

fn clone_into_array<A: Sized + Default + AsMut<[T]>, T: Clone>(slice: &[T]) -> A {
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).clone_from_slice(slice);
    a
}

fn get_key(key: &str) -> Key {
    let mut hasher = Sha3_256::default();
    hasher.input(key.as_bytes());
    Key(clone_into_array(hasher.result().as_slice()))
}

fn main() {
    let logger_config = Config {
        time: None,
        level: Some(Level::Error),
        target: None,
        location: None,
        time_format: None,
    };
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Warn, logger_config).unwrap(),
            TermLogger::new(LevelFilter::Debug, logger_config).unwrap(),
        ],
    ).unwrap();

    let mut node_map = HashMap::new();
    let mut id = 0;
    for i in 0..10 {
        if i == 0 {
            let n = Node::new(&"localhost".to_string(), &(8900 + i).to_string(), None);
            node_map.insert(id, n.clone());
        } else {
            let n = Node::new(
                &"localhost".to_string(),
                &(8900 + i).to_string(),
                Some((*node_map[&(i - 1)].node_data).clone()),
            );
            node_map.insert(id, n.clone());
        }
        thread::sleep(Duration::from_millis(50));
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
                let key = get_key(args[2]);
                let value = args[3];
                node_map.get_mut(&index).unwrap().insert(key, value);
            },
            "get" => {
                let index: u32 = args[1].parse().unwrap();
                let key = get_key(args[2]);
                info!("{:?}", node_map.get_mut(&index).unwrap().get(&key));
            },
            _ => {},
        }
    }
}
