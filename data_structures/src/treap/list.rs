use rand::Rng;
use rand::XorShiftRng;
use treap::implicit_tree;
use treap::node::ImplicitNode;

/// A list implemented by an implicit treap.
///
/// A treap is a tree that satisfies both the binary search tree property and a heap property. Each
/// node has a key, a value, and a priority. The key of any node is greater than all keys in its
/// left subtree and less than all keys occuring in its right subtree. The priority of a node is
/// greater than the priority of all nodes in its subtrees. By randomly generating priorities, the
/// expected height of the tree is proportional to the logarithm of the number of keys.
///
/// An implicit treap is a treap where the key of a node is implicitly determined by the size of
/// its left subtree. This property allows the list get, remove, and insert at an arbitrary index
/// in O(log N) time.
///
/// # Examples
///
pub struct TreapList<T> {
    tree: implicit_tree::Tree<T>,
    rng: XorShiftRng,
}

impl<T> TreapList<T> {
    pub fn new() -> Self {
        TreapList {
            tree: None,
            rng: XorShiftRng::new_unseeded(),
        }
    }

    pub fn insert(&mut self, index: usize, value: T) {
        assert!(index <= self.size());
        let TreapList { ref mut tree, ref mut rng } = *self;
        implicit_tree::insert(tree, index, ImplicitNode {
            value,
            priority: rng.next_u32(),
            size: 1,
            left: None,
            right: None,
        })
    }

    pub fn remove(&mut self, index: usize) -> T {
        assert!(index < self.size());
        let TreapList { ref mut tree, .. } = *self;
        implicit_tree::remove(tree, index)
    }

    pub fn push_front(&mut self, value: T) {
        self.insert(0, value);
    }

    pub fn push_back(&mut self, value: T) {
        let index = self.size();
        self.insert(index, value);
    }

    pub fn pop_front(&mut self) -> T {
        self.remove(0)
    }

    pub fn pop_back(&mut self) -> T {
        let index = self.size() - 1;
        self.remove(index)
    }

    pub fn get<'a>(&'a self, index: usize) -> &'a T {
        let TreapList { ref tree, .. } = *self;
        implicit_tree::get(tree, index)
    }

    pub fn get_mut<'a>(&'a mut self, index: usize) -> &'a mut T {
        let TreapList { ref mut tree, .. } = *self;
        implicit_tree::get_mut(tree, index)
    }

    pub fn size(&self) -> usize {
        let TreapList { ref tree, .. } = *self;
        implicit_tree::size(tree)
    }
}
