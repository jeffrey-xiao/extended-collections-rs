pub mod node_data;
mod routing;

use std::cmp;
use std::net::UdpSocket;
use std::collections::{HashMap, BinaryHeap, HashSet};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ::{REQUEST_TIMEOUT, REPLICATION_PARAM, CONCURRENCY_PARAM, ROUTING_TABLE_SIZE, KEY_LENGTH};
use node::node_data::{NodeDataDistancePair, NodeData, Key};
use node::routing::RoutingTable;
use protocol::{Protocol, Message, Request, Response, RequestPayload, ResponsePayload};

#[derive(Clone)]
pub struct Node {
    pub node_data: Arc<NodeData>,
    routing_table: Arc<Mutex<RoutingTable>>,
    data: Arc<Mutex<HashMap<Key, String>>>,
    pending_requests: Arc<Mutex<HashMap<Key, Sender<Response>>>>,
    protocol: Arc<Protocol>,
}

impl Node {
    pub fn new(ip: &str, port: &str, bootstrap: Option<NodeData>) -> Self {
        let socket = UdpSocket::bind(format!("{}:{}", ip, port))
            .expect("Error: Could not bind to address!");
        let node_data = Arc::new(NodeData {
            addr: socket.local_addr().unwrap().to_string(),
            id: Key::rand(),
        });
        let mut routing_table = RoutingTable::new(Arc::clone(&node_data));
        let (message_tx, message_rx) = channel();
        let protocol = Protocol::new(socket, message_tx);

        // directly use update_node as update_routing_table is async
        if let Some(bootstrap_data) = bootstrap.clone() {
            routing_table.update_node(bootstrap_data);
        }

        let mut ret = Node {
            node_data: node_data,
            routing_table: Arc::new(Mutex::new(routing_table)),
            data: Arc::new(Mutex::new(HashMap::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            protocol: Arc::new(protocol),
        };

        ret.clone().start_message_handler(message_rx);

        let target_key = ret.node_data.id.clone();
        ret.lookup_nodes(&target_key);

        ret
    }

    fn start_message_handler(self, rx: Receiver<Message>) {
        thread::spawn(move || {
            for request in rx.iter() {
                let node = self.clone();
                match request {
                    Message::Request(request) => node.handle_request(request),
                    Message::Response(response) => node.handle_response(response),
                }
            }
        });
    }

    fn update_routing_table(&mut self, node_data: NodeData) {
        println!("{:?} ADDING {:?}", self.node_data.addr, node_data);
        let mut node = self.clone();
        thread::spawn(move || {
            let mut lrs_node_opt = None;
            {
                let mut routing_table = node.routing_table.lock().unwrap();
                if !routing_table.update_node(node_data.clone()) {
                    lrs_node_opt = routing_table.remove_lrs(&node_data.id);
                }
            }

            // Ping the lrs node and move to front of bucket if active
            if let Some(lrs_node) = lrs_node_opt {
                node.rpc_ping(&lrs_node);
                let mut routing_table = node.routing_table.lock().unwrap();
                routing_table.update_node(node_data);
            }
        });
    }

    fn handle_request(self, request: Request) {
        println!("{:?} RECEIVING REQUEST {:?}", self.node_data.addr, request.payload);
        self.clone().update_routing_table(request.sender.clone());
        thread::spawn(move || {
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

            self.protocol.send_message(&Message::Response(Response {
                request: request.clone(),
                receiver: receiver,
                payload,
            }), &request.sender)
        });
    }

    fn handle_response(self, response: Response) {
        self.clone().update_routing_table(response.receiver.clone());
        thread::spawn(move || {
            let pending_requests = self.pending_requests.lock().unwrap();
            let Response { ref request, .. } = response.clone();
            if let Some(sender) = pending_requests.get(&request.id) {
                println!("{:?} RECEIVING RESPONSE {:?}", self.node_data.addr, response.payload);
                sender.send(response).unwrap();
            } else {
                println!("Warning: Original request not found; irrelevant response or expired request.");
            }
        });
    }

    fn send_request(&mut self, dest: &NodeData, payload: RequestPayload) -> Option<Response> {
        println!("{:?} SENDING REQUEST {:?}", self.node_data.addr, payload);
        let (response_tx, response_rx) = channel();
        let mut pending_requests = self.pending_requests.lock().unwrap();
        let mut token = Key::rand();

        while pending_requests.contains_key(&token) {
            token = Key::rand();
        }
        pending_requests.insert(token.clone(), response_tx);
        drop(pending_requests);

        self.protocol.send_message(&Message::Request(Request {
            id: token.clone(),
            sender: (*self.node_data).clone(),
                payload: payload,
        }), dest);

        let node = self.clone();

        match response_rx.recv_timeout(Duration::from_millis(REQUEST_TIMEOUT)) {
            Ok(response) => {
                let mut pending_requests = self.pending_requests.lock().unwrap();
                pending_requests.remove(&token);
                Some(response)
            },
            Err(_) => {
                println!("Warning: Request timed out after waiting for {} milliseconds", REQUEST_TIMEOUT);
                let mut pending_requests = self.pending_requests.lock().unwrap();
                pending_requests.remove(&token);
                None
            },
        }
    }

    fn rpc_ping(&mut self, dest: &NodeData) -> Option<Response> {
        self.send_request(dest, RequestPayload::Ping)
    }

    fn rpc_find_node(&mut self, dest: &NodeData, key: &Key) -> Option<Response> {
        self.send_request(dest, RequestPayload::FindNode(key.clone()))
    }

    fn lookup_nodes(&mut self, key: &Key) {
        let routing_table = self.routing_table.lock().unwrap();
        let closest_nodes = routing_table.get_closest(key, CONCURRENCY_PARAM);
        drop(routing_table);

        let mut closest_distance = Key::new([255u8; KEY_LENGTH]);
        for node_data in &closest_nodes {
            closest_distance = cmp::min(closest_distance, key.xor(&node_data.id))
        }

        // initialize found nodes and priority queue
        let mut found_nodes: HashSet<NodeData> = HashSet::from(
            closest_nodes.clone().into_iter().collect()
        );
        found_nodes.insert((*self.node_data).clone());
        let mut queue: BinaryHeap<NodeDataDistancePair> = BinaryHeap::from(
            closest_nodes
                .into_iter()
                .map(|node_data| {
                    NodeDataDistancePair(
                        node_data.clone(),
                        node_data.id.xor(key).get_distance()
                    )
                })
                .collect::<Vec<NodeDataDistancePair>>()
        );

        let (tx, rx) = channel();

        let mut concurrent_thread_count = 0;
        for _ in 0..CONCURRENCY_PARAM {
            if !queue.is_empty() {
                let mut node = self.clone();
                let next_node_data = queue.pop().unwrap().0;
                let target_key = key.clone();
                let sender = tx.clone();
                thread::spawn(move || {
                    sender.send(node.rpc_find_node(&next_node_data, &target_key)).unwrap();
                });
                concurrent_thread_count += 1;
            }
        }

        let is_terminated = Arc::new(Mutex::new(false));
        while concurrent_thread_count > 0 {
            let response_opt = rx.recv().unwrap();
            *is_terminated.lock().unwrap() = true;

            if let Some(Response{ payload: ResponsePayload::Nodes(nodes), .. }) = response_opt {
                for node_data in nodes {
                    let curr_distance = node_data.id.xor(key);
                    println!("CURR DISTANCE IS {:?}", curr_distance);

                    if !found_nodes.contains(&node_data) {
                        println!("GOT {:?}", node_data);
                        if curr_distance < closest_distance {
                            closest_distance = curr_distance;
                            *is_terminated.lock().unwrap() = false;
                        }

                        found_nodes.insert(node_data.clone());
                        queue.push(NodeDataDistancePair(
                            node_data.clone(),
                            node_data.id.xor(key).get_distance()
                        ));
                    }
                }
            }

            if !*is_terminated.lock().unwrap() {
                let mut node = self.clone();
                let next_node_data = queue.pop().unwrap().0;
                let target_key = key.clone();
                let sender = tx.clone();
                thread::spawn(move || {
                    sender.send(node.rpc_find_node(&next_node_data, &target_key)).unwrap();
                });
            } else {
                concurrent_thread_count -= 1;
            }
        }
    }
}
