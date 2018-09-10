use entry::Entry;
use splay_tree::tree;

pub struct Node<T, U> {
    pub entry: Entry<T, U>,
    pub left: tree::Tree<T, U>,
    pub right: tree::Tree<T, U>,
}

impl<T, U> Node<T, U> {
    pub fn new(key: T, value: U) -> Self {
        Node {
            entry: Entry { key, value },
            left: None,
            right: None,
        }
    }
}
