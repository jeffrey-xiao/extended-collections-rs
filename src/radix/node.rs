use crate::radix::tree::Tree;
use std::mem;

pub struct Node<T> {
    pub key: Vec<u8>,
    pub value: Option<T>,
    pub next: Tree<T>,
    pub child: Tree<T>,
}

impl<T> Node<T> {
    pub fn new(key: Vec<u8>, value: Option<T>) -> Self {
        Self {
            key,
            value,
            next: None,
            child: None,
        }
    }

    pub fn contains(&self, byte: u8) -> bool {
        self.get(byte).is_some()
    }

    pub fn get(&self, byte: u8) -> &Tree<T> {
        let mut curr = &self.child;
        loop {
            match curr {
                Some(ref node) if node.key[0] != byte => curr = &node.next,
                tree => return tree,
            };
        }
    }

    pub fn get_mut(&mut self, byte: u8) -> &mut Tree<T> {
        let mut curr = &mut self.child;
        loop {
            let matches = match curr {
                Some(ref mut node) => node.key[0] == byte,
                None => false,
            };
            if matches {
                return curr;
            } else {
                match { curr } {
                    Some(ref mut node) => curr = &mut node.next,
                    tree => return tree,
                }
            }
        }
    }

    pub fn split(&mut self, split_index: usize) {
        let split_key = self.key.split_off(split_index);
        let mut split = Node::new(split_key, None);
        mem::swap(&mut self.value, &mut split.value);
        mem::swap(&mut self.child, &mut split.child);
        self.insert_child(split);
    }

    pub fn insert_child(&mut self, child: Node<T>) {
        fn insert_inner<T>(tree: &mut Tree<T>, mut new_node: Box<Node<T>>) {
            match tree {
                Some(ref mut node) => {
                    if node.key[0] > new_node.key[0] {
                        mem::swap(node, &mut new_node);
                        node.next = Some(new_node);
                    } else {
                        insert_inner(&mut node.next, new_node);
                    }
                }
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

    pub fn is_leaf(&self) -> bool {
        self.child.is_none()
    }

    pub fn get_replacement_tree(&mut self) -> Tree<T> {
        self.next.take()
    }

    pub fn push_all_children(&self, curr_key: Vec<u8>, keys: &mut Vec<Vec<u8>>) {
        fn push_all_children_inner<T>(
            tree: &Tree<T>,
            mut curr_key: Vec<u8>,
            keys: &mut Vec<Vec<u8>>,
        ) {
            if let Some(ref node) = tree {
                let len = curr_key.len();

                curr_key.extend(node.key.iter());
                if node.value.is_some() {
                    keys.push(curr_key.clone());
                }
                push_all_children_inner(&node.child, curr_key.clone(), keys);

                curr_key.split_off(len);
                push_all_children_inner(&node.next, curr_key, keys);
            }
        }
        push_all_children_inner(&self.child, curr_key, keys);
    }

    pub fn min(&self) -> &Tree<T> {
        &self.child
    }

    pub fn max(&self) -> &Tree<T> {
        let mut curr_tree = &self.child;
        while let Some(ref curr_node) = curr_tree {
            if (*curr_node).next.is_none() {
                return curr_tree;
            }
            curr_tree = &curr_node.next;
        }
        &None
    }
}
