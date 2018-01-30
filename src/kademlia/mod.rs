mod protocol;
mod node;
pub use self::node::Node;

const KEY_LENGTH: usize = 20;
const MESSAGE_LENGTH: usize = 8196;
const ROUTING_TABLE_SIZE: usize = KEY_LENGTH * 8;

const REPLICATION_PARAM: usize = 20;
const CONCURRENCY_PARAM: usize = 3;

const REQUEST_TIMEOUT: u64 = 10000;
