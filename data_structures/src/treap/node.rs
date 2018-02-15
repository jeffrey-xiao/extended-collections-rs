use treap::{implicit_tree, tree};
use treap::entry::Entry;

/// A struct representing an internal node of a treap.
pub struct Node<T: Ord, U> {
    pub entry: Entry<T, U>,
    pub priority: u32,
    pub left: tree::Tree<T, U>,
    pub right: tree::Tree<T, U>,
}

/// A struct representing an internal node of an implicit treap.
pub struct ImplicitNode<T> {
    pub value: T,
    pub priority: u32,
    pub size: usize,
    pub left: implicit_tree::Tree<T>,
    pub right: implicit_tree::Tree<T>,
}

impl<T> ImplicitNode<T> {
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn update(&mut self) {
        let ImplicitNode { ref mut size, ref left, ref right, .. } = *self;
        *size = 1;
        if let Some(ref left_node) = *left {
            *size += left_node.size();
        }
        if let Some(ref right_node) = *right {
            *size += right_node.size();
        }
    }

    pub fn get_implicit_key(&self) -> usize {
        match self.left {
            Some(ref left_node) => left_node.size() + 1,
            None => 1
        }
    }
}
