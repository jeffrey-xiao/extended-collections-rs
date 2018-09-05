use std::cmp;
use entry::Entry;
use avl_tree::tree;

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

}
