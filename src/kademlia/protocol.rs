use std::net::UdpSocket;
use std::str;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread;
use serde_json;

use kademlia::node::node_data::{NodeData, Key};
use kademlia::MESSAGE_LENGTH;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub id: Key,
    pub sender: NodeData,
    pub payload: RequestPayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestPayload {
    Ping,
    Store(Key, String),
    FindNode(Key),
    FindValue(Key),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub request: Request,
    pub receiver: NodeData,
    pub payload: ResponsePayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResponsePayload {
    Nodes(Vec<NodeData>),
    Value(String),
    Ping,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Request),
    Response(Response),
}

#[derive(Clone)]
pub struct Protocol {
    socket: Arc<UdpSocket>,
}

impl Protocol {
    pub fn new(socket: UdpSocket, tx: Sender<Message>) -> Protocol {
        let protocol = Protocol { socket: Arc::new(socket) };
        let ret = protocol.clone();
        thread::spawn(move || {
            let mut buffer = [0u8; MESSAGE_LENGTH];
            loop {
                let (len, src_addr) = protocol.socket.recv_from(&mut buffer).unwrap();
                let buffer_string = String::from(str::from_utf8(&buffer[..len]).unwrap());
                let message = serde_json::from_str::<Message>(&buffer_string).unwrap();

                tx.send(message).unwrap();
            }
        });
        ret
    }

    pub fn send_message(&self, message: &Message, node_data: &NodeData) {
        let buffer_string = serde_json::to_string(&message).unwrap();
        let &NodeData { ref addr, .. } = node_data;
        self.socket.send_to(buffer_string.as_bytes(), addr).unwrap();
    }
}
