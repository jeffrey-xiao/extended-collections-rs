pub mod node_data;
mod routing;

use std::net::UdpSocket;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver};
use std::thread;
use std::sync::Arc;
use kademlia::node::node_data::{NodeData, Key};
use kademlia::node::routing::RoutingTable;
use kademlia::protocol::{Protocol, Message, Request, Response, RequestPayload, ResponsePayload};

#[derive(Clone)]
pub struct Node {
    pub node_data: NodeData,
    routing_table: RoutingTable,
    data: HashMap<Key, String>,
    protocol: Protocol,
}

impl Node {
    pub fn new(ip: String, port: String, bootstrap: Option<NodeData>) -> Self {
        let socket = UdpSocket::bind(format!("{}:{}", ip, port))
            .expect("Error: Could not bind to address!");
        let node_data = NodeData {
            addr: socket.local_addr().unwrap().to_string(),
            id: Key::new(),
        };
        let mut routing_table = RoutingTable::new();
        if let Some(bootstrap_data) = bootstrap.clone() {
            routing_table.update_node(
                node_data.id.xor(&bootstrap_data.id).get_distance(),
                bootstrap_data,
            );
        }

        let (request_tx, request_rx) = channel();

        let protocol = Protocol::new(socket, request_tx);
        let mut ret = Node { node_data, routing_table, data: HashMap::new(), protocol: protocol };

        ret.clone().start_request_handler(request_rx);

        if let Some(bootstrap_data) = bootstrap {
            ret.ping(&bootstrap_data);
        }

        ret
    }

    fn start_request_handler(mut self, rx: Receiver<Request>) {
        thread::spawn(move || {
            for request in rx.iter() {
                println!("RECIEVED {:?}", request);
                let receiver_id = self.node_data.id.clone();
                let payload = match request.payload.clone() {
                    RequestPayload::Ping => ResponsePayload::Ping,
                    RequestPayload::Store(key, value) => {
                        self.data.insert(key, value);
                        ResponsePayload::Ping
                    }
                    RequestPayload::FindNode(key) => {
                        ResponsePayload::Nodes(
                            self.routing_table.get_closest(self.node_data.id.xor(&key).get_distance())
                        )
                    },
                    RequestPayload::FindValue(key) => {
                        if let Some(value) = self.data.get(&key) {
                            ResponsePayload::Value(value.clone())
                        } else {
                            ResponsePayload::Nodes(
                                self.routing_table.get_closest(self.node_data.id.xor(&key).get_distance())
                            )
                        }
                    },
                };

                self.protocol.send_message(Message::Response(Response {
                    request: request.clone(),
                    receiver_id,
                    payload,
                }), &request.caller)
            }
        });
    }

    pub fn ping(&self, dest: &NodeData) {
        println!("PINGING");
        self.protocol.send_message(Message::Request(Request {
            request_id: Key::new(),
            caller: self.node_data.clone(),
            payload: RequestPayload::Ping,
        }), dest);
    }
}
