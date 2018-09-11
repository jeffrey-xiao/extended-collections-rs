use entry::Entry;
use splay_tree::tree;
use std::mem;

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

    pub fn rotate_left(&mut self) {
        let mut child = self.right.take().expect("Expected right child node to be `Some`.");
        self.right = child.left.take();
        mem::swap(&mut *child, self);
        self.left = Some(child);
    }

    pub fn rotate_right(&mut self) {
        let mut child = self.left.take().expect("Expected left child node to be `Some`.");
        self.left = child.right.take();
        mem::swap(&mut *child, self);
        self.right = Some(child);
    }
}
