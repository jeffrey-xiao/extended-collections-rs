use std::mem;
use treap::tree;
use treap::entry::Entry;

/// A struct representing an internal node of a treap.
pub struct Node<T: Entry> {
    pub entry: T,
    pub priority: u32,
    pub left: tree::Tree<T>,
    pub right: tree::Tree<T>,
}

impl<T: Entry> Node<T> {
    #[inline]
    pub fn is_heap_property_violated(&self, child: &tree::Tree<T>) -> bool {
        match *child {
            None => false,
            Some(ref child_node) => self.priority < child_node.priority,
        }
    }

    #[inline]
    pub fn rotate_left(&mut self) {
        let right = self.right.take();
        if let Some(mut old_node) = right {
            mem::swap(self, &mut old_node);
            old_node.right = self.left.take();
            self.left = Some(old_node);
        }
    }

    #[inline]
    pub fn rotate_right(&mut self) {
        let left = self.left.take();
        if let Some(mut old_node) = left {
            mem::swap(self, &mut old_node);
            old_node.left = self.right.take();
            self.right = Some(old_node);
        }
    }
}
