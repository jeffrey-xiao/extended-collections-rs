use radix::node::{Key, Node};
use std::cmp::Ordering;
use std::mem;

pub type Tree<T> = Option<Box<Node<T>>>;

pub fn insert<T>(tree: &mut Tree<T>, mut key: Key, value: T) -> Option<T> {
    let node = match *tree {
        Some(ref mut node) => node,
        _ => unreachable!(),
    };
    let split_index = node.key.iter().zip(key.iter()).position(|pair| pair.0 != pair.1);
    match split_index {
        Some(split_index) => {
            let mut split_key = node.key.split_off(split_index);
            mem::swap(&mut split_key, &mut node.key);
            let mut split = mem::replace(&mut **node, Node::new(split_key, None));
            let mut child = Node::new(key.split_off(split_index), Some(value));

            node.next = split.next.take();
            node.insert_child(split);
            node.insert_child(child);
            None
        },
        None => match node.key.len().cmp(&key.len()) {
            Ordering::Less => {
                key = key.split_off(node.key.len());
                let byte = key[0];
                if node.contains(byte) {
                    insert(node.get_mut(byte), key, value)
                } else {
                    node.insert_child(Node::new(key, Some(value)));
                    None
                }
            },
            Ordering::Greater => {
                let mut split_key = node.key.split_off(key.len());
                mem::swap(&mut split_key, &mut node.key);
                let mut split = mem::replace(&mut **node, Node::new(split_key, None));
                node.next = split.next.take();
                node.value = Some(value);
                node.insert_child(split);
                None
            },
            Ordering::Equal => mem::replace(&mut node.value, Some(value)),
        }
    }
}

pub fn remove<T>(tree: &mut Tree<T>, key: &Key, mut index: usize) -> Option<(Key, T)> {
    let mut next_tree = None;
    let ret;
    {
        let node = match *tree {
            Some(ref mut node) => node,
            None => return None,
        };
        let split_index = node.key.iter().zip(key[index..].iter()).position(|pair| pair.0 != pair.1);
        match split_index {
            Some(_) => return None,
            None => match node.key.len().cmp(&(key.len() - index)) {
                Ordering::Less => {
                    index += node.key.len();
                    let byte = key[index];
                    ret = remove(node.get_mut(byte), key, index);
                    node.merge();
                    if node.value.is_none() && node.child.is_none() {
                        next_tree = Some(node.next.take());
                    }
                },
                Ordering::Greater => return None,
                Ordering::Equal => {
                    ret = node.value.take().map(|value| (key.clone(), value));
                    node.merge();
                    if node.value.is_none() && node.child.is_none() {
                        next_tree = Some(node.next.take());
                    }
                }
            }
        }
    }
    if let Some(next_tree) = next_tree {
        *tree = next_tree;
    }
    ret
}

pub fn get<'a, T>(tree: &'a Tree<T>, key: &Key, mut index: usize) -> Option<&'a T> {
    let node = match *tree {
        Some(ref node) => node,
        None => return None,
    };
    let split_index = node.key.iter().zip(key[index..].iter()).position(|pair| pair.0 != pair.1);
    match split_index {
        Some(_) => None,
        None => match node.key.len().cmp(&(key.len() - index)) {
            Ordering::Less => {
                index += node.key.len();
                get(node.get(key[index]), key, index)
            },
            Ordering::Greater => None,
            Ordering::Equal => node.value.as_ref(),
        }
    }
}

pub fn get_mut<'a, T>(tree: &'a mut Tree<T>, key: &Key, mut index: usize) -> Option<&'a mut T> {
    let node = match *tree {
        Some(ref mut node) => node,
        None => return None,
    };
    let split_index = node.key.iter().zip(key[index..].iter()).position(|pair| pair.0 != pair.1);
    match split_index {
        Some(_) => None,
        None => match node.key.len().cmp(&(key.len() - index)) {
            Ordering::Less => {
                index += node.key.len();
                get_mut(node.get_mut(key[index]), key, index)
            },
            Ordering::Greater => None,
            Ordering::Equal => node.value.as_mut(),
        }
    }
}

pub fn min<T>(tree: &Tree<T>, mut curr_key: Key) -> Option<Key> {
    let node = match *tree {
        Some(ref node) => node,
        None => return None,
    };

    curr_key.extend_from_slice(node.key.as_slice());

    if node.value.is_some() {
       Some(curr_key)
    } else {
        min(node.min(), curr_key)
    }
}

pub fn max<T>(tree: &Tree<T>, mut curr_key: Key) -> Option<Key> {
    let node = match *tree {
        Some(ref node) => node,
        None => return None,
    };

    curr_key.extend_from_slice(node.key.as_slice());

    if node.value.is_some() && node.child.is_none() {
        Some(curr_key)
    } else {
        max(node.max(), curr_key)
    }
}
