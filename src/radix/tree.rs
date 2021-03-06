use crate::radix::node::Node;
use std::cmp::Ordering;
use std::mem;

pub type Tree<T> = Option<Box<Node<T>>>;

pub fn insert<T>(tree: &mut Tree<T>, mut key: &[u8], value: T) -> Option<T> {
    let node = tree.as_mut().expect("Expected non-empty tree.");
    let split_index = node
        .key
        .iter()
        .zip(key.iter())
        .position(|pair| pair.0 != pair.1);
    match split_index {
        Some(split_index) => {
            node.split(split_index);
            key = key.split_at(split_index).1;
            let child = Node::new(key.to_vec(), Some(value));
            node.insert_child(child);
            None
        }
        None => match node.key.len().cmp(&key.len()) {
            Ordering::Less => {
                key = key.split_at(node.key.len()).1;
                let byte = key[0];
                if node.contains(byte) {
                    insert(node.get_mut(byte), key, value)
                } else {
                    node.insert_child(Node::new(key.to_vec(), Some(value)));
                    None
                }
            }
            Ordering::Greater => {
                node.split(key.len());
                node.value = Some(value);
                None
            }
            Ordering::Equal => mem::replace(&mut node.value, Some(value)).map(|value| value),
        },
    }
}

pub fn remove<T>(tree: &mut Tree<T>, key: &[u8], mut index: usize) -> Option<(Vec<u8>, T)> {
    let mut next_tree = None;
    let ret;
    {
        let node = match tree {
            Some(ref mut node) => node,
            None => return None,
        };
        let split_index = node
            .key
            .iter()
            .zip(key[index..].iter())
            .position(|pair| pair.0 != pair.1);
        match split_index {
            Some(_) => return None,
            None => match node.key.len().cmp(&(key.len() - index)) {
                Ordering::Less => {
                    index += node.key.len();
                    let byte = key[index];
                    ret = remove(node.get_mut(byte), key, index);
                    node.merge();
                    if node.value.is_none() && node.is_leaf() {
                        next_tree = Some(node.get_replacement_tree());
                    }
                }
                Ordering::Greater => return None,
                Ordering::Equal => {
                    ret = node.value.take().map(|value| (key.to_vec(), value));
                    node.merge();
                    if node.value.is_none() && node.is_leaf() {
                        next_tree = Some(node.get_replacement_tree());
                    }
                }
            },
        }
    }
    if let Some(next_tree) = next_tree {
        *tree = next_tree;
    }
    ret
}

pub fn get<'a, T>(tree: &'a Tree<T>, key: &[u8], mut index: usize) -> Option<&'a T> {
    let node = match tree {
        Some(ref node) => node,
        None => return None,
    };
    let split_index = node
        .key
        .iter()
        .zip(key[index..].iter())
        .position(|pair| pair.0 != pair.1);
    match split_index {
        Some(_) => None,
        None => match node.key.len().cmp(&(key.len() - index)) {
            Ordering::Less => {
                index += node.key.len();
                get(node.get(key[index]), key, index)
            }
            Ordering::Greater => None,
            Ordering::Equal => node.value.as_ref(),
        },
    }
}

pub fn get_mut<'a, T>(tree: &'a mut Tree<T>, key: &[u8], mut index: usize) -> Option<&'a mut T> {
    let node = match tree {
        Some(ref mut node) => node,
        None => return None,
    };
    let split_index = node
        .key
        .iter()
        .zip(key[index..].iter())
        .position(|pair| pair.0 != pair.1);
    match split_index {
        Some(_) => None,
        None => match node.key.len().cmp(&(key.len() - index)) {
            Ordering::Less => {
                index += node.key.len();
                get_mut(node.get_mut(key[index]), key, index)
            }
            Ordering::Greater => None,
            Ordering::Equal => node.value.as_mut(),
        },
    }
}

pub fn get_longest_prefix<T>(
    tree: &Tree<T>,
    key: &[u8],
    mut index: usize,
    mut curr_key: Vec<u8>,
    keys: &mut Vec<Vec<u8>>,
) {
    let node = match tree {
        Some(ref node) => node,
        None => return,
    };
    curr_key.extend(node.key.iter());
    let split_index = node
        .key
        .iter()
        .zip(key[index..].iter())
        .position(|pair| pair.0 != pair.1);
    match split_index {
        Some(_) => {
            if node.value.is_some() {
                keys.push(curr_key.clone());
            }
            node.push_all_children(curr_key, keys);
        }
        None => match node.key.len().cmp(&(key.len() - index)) {
            Ordering::Less => {
                index += node.key.len();
                let next_child = node.get(key[index]);
                match next_child {
                    Some(_) => get_longest_prefix(next_child, key, index, curr_key, keys),
                    None => {
                        if node.value.is_some() {
                            keys.push(curr_key)
                        }
                    }
                }
            }
            _ => {
                if node.value.is_some() {
                    keys.push(curr_key.clone());
                }
                node.push_all_children(curr_key, keys);
            }
        },
    }
}

pub fn min<T>(tree: &Tree<T>, mut curr_key: Vec<u8>) -> Option<Vec<u8>> {
    let node = match tree {
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

pub fn max<T>(tree: &Tree<T>, mut curr_key: Vec<u8>) -> Option<Vec<u8>> {
    let node = match tree {
        Some(ref node) => node,
        None => return None,
    };

    curr_key.extend_from_slice(node.key.as_slice());

    if node.value.is_some() && node.is_leaf() {
        Some(curr_key)
    } else {
        max(node.max(), curr_key)
    }
}
