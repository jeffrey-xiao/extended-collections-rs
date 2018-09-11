use avl_tree::tree;
use entry::Entry;
use std::cmp;
use std::mem;

/// A struct representing an internal node of an avl tree.
pub struct Node<T, U> {
    pub entry: Entry<T, U>,
    pub height: usize,
    pub left: tree::Tree<T, U>,
    pub right: tree::Tree<T, U>,
}

impl<T, U> Node<T, U> {
    pub fn new(key: T, value: U) -> Self {
        Node {
            entry: Entry { key, value },
            height: 1,
            left: None,
            right: None,
        }
    }

    pub fn update(&mut self) {
        let Node { ref mut height, ref left, ref right, .. } = self;
        *height = cmp::max(tree::height(left), tree::height(right)) + 1;
    }

    pub fn balance(&self) -> i32 {
        (tree::height(&self.left) as i32) - (tree::height(&self.right) as i32)
    }

    pub fn rotate_left(&mut self) {
        let mut child = self.right.take().expect("Expected right child node to be `Some`.");
        self.right = child.left.take();
        mem::swap(&mut *child, self);
        child.update();
        self.left = Some(child);
        self.update();
    }

    pub fn rotate_right(&mut self) {
        let mut child = self.left.take().expect("Expected left child node to be `Some`.");
        self.left = child.right.take();
        mem::swap(&mut *child, self);
        child.update();
        self.right = Some(child);
        self.update();
    }
}
