use radix::tree::Tree;
use std::mem;

pub type Key = Vec<u8>;

#[derive(Debug)]
pub struct Node<T> {
    pub key: Key,
    pub value: Option<T>,
    pub next: Tree<T>,
    pub child: Tree<T>,
}

impl<T> Node<T> {
    pub fn new(key: Key, value: Option<T>) -> Self {
        Self {
            key,
            value: value,
            next: None,
            child: None,
        }
    }

    pub fn contains(&self, byte: u8) -> bool {
        self.get(byte).is_some()
    }

    pub fn get(&self, byte: u8) -> &Tree<T> {
        fn get_inner<T>(tree: &Tree<T>, byte: u8) -> &Tree<T> {
            match *tree {
                Some(ref node) if node.key[0] != byte => get_inner(&node.next, byte),
                _ => tree,
            }
        }
        get_inner(&self.child, byte)
    }

    pub fn get_mut(&mut self, byte: u8) -> &mut Tree<T> {
        fn get_mut_inner<T>(tree: &mut Tree<T>, byte: u8) -> &mut Tree<T> {
            match *tree {
                Some(ref mut node) if node.key[0] != byte => get_mut_inner(&mut node.next, byte),
                _ => tree,
            }
        }
        get_mut_inner(&mut self.child, byte)
    }

    pub fn insert_child(&mut self, child: Node<T>) {
        fn insert_inner<T>(tree: &mut Tree<T>, mut new_node: Box<Node<T>>) {
            match *tree {
                Some(ref mut node) => {
                    if node.key[0] > new_node.key[0] {
                        mem::swap(node, &mut new_node);
                        node.next = Some(new_node);
                    } else {
                        insert_inner(&mut node.next, new_node);
                    }
                },
                None => *tree = Some(new_node),
            }
        }
        insert_inner(&mut self.child, Box::new(child));
    }

    pub fn merge(&mut self) {
        if let Some(mut child_node) = self.child.take() {
            if self.value.is_none() && child_node.next.is_none() {
                self.key.extend(child_node.key.iter());
                self.value = child_node.value.take();
                self.child = child_node.child.take();
            } else {
                self.child = Some(child_node);
            }
        }
    }

    pub fn min(&self) -> &Tree<T> {
        &self.child
    }

    pub fn max(&self) -> &Tree<T> {
        let mut curr_tree = &self.child;
        while let Some(ref curr_node) = *curr_tree {
            if (*curr_node).next.is_none() {
                return curr_tree;
            }
            curr_tree = &curr_node.next;
        }
        &None
    }
}
