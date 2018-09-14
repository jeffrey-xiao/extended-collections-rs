use entry::Entry;
use treap::{implicit_tree, tree};

/// A struct representing an internal node of a treap.
pub struct Node<T, U> {
    pub entry: Entry<T, U>,
    pub priority: u32,
    pub len: usize,
    pub left: tree::Tree<T, U>,
    pub right: tree::Tree<T, U>,
}

/// A struct representing an internal node of an implicit treap.
pub struct ImplicitNode<T> {
    pub value: T,
    pub priority: u32,
    pub len: usize,
    pub left: implicit_tree::Tree<T>,
    pub right: implicit_tree::Tree<T>,
}

impl<T, U> Node<T, U> {
    pub fn new(key: T, value: U, priority: u32) -> Self {
        Node {
            entry: Entry { key, value },
            priority,
            len: 1,
            left: None,
            right: None,
        }
    }

    pub fn update(&mut self) {
        let Node {
            ref mut len,
            ref left,
            ref right,
            ..
        } = self;
        *len = 1;
        if let Some(ref left_node) = left {
            *len += left_node.len;
        }
        if let Some(ref right_node) = right {
            *len += right_node.len;
        }
    }
}

impl<T> ImplicitNode<T> {
    pub fn new(value: T, priority: u32) -> Self {
        ImplicitNode {
            value,
            priority,
            len: 1,
            left: None,
            right: None,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn update(&mut self) {
        let ImplicitNode {
            ref mut len,
            ref left,
            ref right,
            ..
        } = self;
        *len = 1;
        if let Some(ref left_node) = left {
            *len += left_node.len;
        }
        if let Some(ref right_node) = right {
            *len += right_node.len;
        }
    }

    pub fn get_implicit_key(&self) -> usize {
        match self.left {
            Some(ref left_node) => left_node.len() + 1,
            None => 1,
        }
    }
}
