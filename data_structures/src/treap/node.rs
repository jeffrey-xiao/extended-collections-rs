use treap::tree;
use treap::entry::Entry;

/// A struct representing an internal node of a treap.
pub struct Node<T: Ord, U> {
    pub entry: Entry<T, U>,
    pub priority: u32,
    pub left: tree::Tree<T, U>,
    pub right: tree::Tree<T, U>,
}
