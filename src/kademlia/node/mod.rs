pub mod node_data;
mod routing;

use std::net::UdpSocket;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kademlia::node::node_data::{NodeData, Key};
use kademlia::node::routing::RoutingTable;
use kademlia::protocol::{Protocol, Message, Request, Response, RequestPayload, ResponsePayload};
use kademlia::{REQUEST_TIMEOUT, REPLICATION_PARAM};

#[derive(Clone)]
pub struct Node {
    pub node_data: Arc<NodeData>,
    routing_table: Arc<Mutex<RoutingTable>>,
    data: Arc<Mutex<HashMap<Key, String>>>,
    pending_requests: Arc<Mutex<HashMap<Key, Sender<Option<Response>>>>>,
    protocol: Arc<Protocol>,
}

impl Node {
    pub fn new(ip: String, port: String, bootstrap: Option<NodeData>) -> Self {
        let socket = UdpSocket::bind(format!("{}:{}", ip, port))
            .expect("Error: Could not bind to address!");
        let node_data = Arc::new(NodeData {
            addr: socket.local_addr().unwrap().to_string(),
            id: Key::new(),
        });
        let mut routing_table = RoutingTable::new(Arc::clone(&node_data));
        if let Some(bootstrap_data) = bootstrap.clone() {
            routing_table.update_node(bootstrap_data);
        }

        let (message_tx, message_rx) = channel();

        let protocol = Protocol::new(socket, message_tx);
        let mut ret = Node {
            node_data: node_data,
            routing_table: Arc::new(Mutex::new(routing_table)),
            data: Arc::new(Mutex::new(HashMap::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            protocol: Arc::new(protocol),
        };

        ret.clone().start_message_handler(message_rx);

        if let Some(bootstrap_data) = bootstrap {
            ret.ping(&bootstrap_data);
        }

        ret
    }

    fn start_message_handler(self, rx: Receiver<Message>) {
        thread::spawn(move || {
            for request in rx.iter() {
                let node = self.clone();
                thread::spawn(move || {
                    match request {
                        Message::Request(request) => node.handle_request(request),
                        Message::Response(response) => node.handle_response(response),
                    }
                });
            }
        });
    }

    fn handle_request(self, request: Request) {
        println!("RECIEVED {:?}", request);
        let receiver = (*self.node_data).clone();
        let payload = match request.payload.clone() {
            RequestPayload::Ping => ResponsePayload::Ping,
            RequestPayload::Store(key, value) => {
                self.data.lock().unwrap().insert(key, value);
                ResponsePayload::Ping
            }
            RequestPayload::FindNode(key) => {
                ResponsePayload::Nodes(
                    self.routing_table.lock().unwrap().get_closest(&key, REPLICATION_PARAM)
                )
            },
            RequestPayload::FindValue(key) => {
                if let Some(value) = self.data.lock().unwrap().get(&key) {
                    ResponsePayload::Value(value.clone())
                } else {
                    ResponsePayload::Nodes(
                        self.routing_table.lock().unwrap().get_closest(&key, REPLICATION_PARAM)
                    )
                }
            },
        };

        self.protocol.send_message(Message::Response(Response {
            request: request.clone(),
            receiver: receiver,
            payload,
        }), &request.caller)
    }

    fn handle_response(self, response: Response) {
        let pending_requests = self.pending_requests.lock().unwrap();
        let Response { ref request, .. } = response.clone();
        println!("{:?}", response);
        if let Some(sender) = pending_requests.get(&request.id) {
            println!("RECIEVED RESPONSE BACK");
            sender.send(Some(response)).unwrap();
        } else {
            println!("Warning: Original request not found; irrelevant response or expired request.");
        }
    }

    pub fn send_request(&mut self, dest: &NodeData, payload: RequestPayload) -> Receiver<Option<Response>> {
        let (response_tx, response_rx) = channel();
        let mut pending_requests = self.pending_requests.lock().unwrap();
        let mut token = Key::new();

        while pending_requests.contains_key(&token) {
            token = Key::new();
        }
        pending_requests.insert(token.clone(), response_tx.clone());
        drop(pending_requests);

        self.protocol.send_message(Message::Request(Request {
            id: token.clone(),
            caller: (*self.node_data).clone(),
            payload: payload,
        }), dest);

        let node = self.clone();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(REQUEST_TIMEOUT));
            if let Ok(_) = response_tx.send(None) {
                println!("REQUEST TIMEOUT");
                let mut pending_requests = node.pending_requests.lock().unwrap();
                pending_requests.remove(&token);
            }
        });
        response_rx
    }

    pub fn ping(&mut self, dest: &NodeData) {
        println!("PINGING");
        let response = self.send_request(dest, RequestPayload::Ping).recv().unwrap();
        println!("GOT PING BACK");
    }
}
