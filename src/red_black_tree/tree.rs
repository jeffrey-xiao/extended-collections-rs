use entry::Entry;
use red_black_tree::node::{Color, Node};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::mem;

pub type Tree<T, U> = Option<Box<Node<T, U>>>;

pub fn is_red<T, U>(tree: &Tree<T, U>) -> bool {
    match tree {
        None => false,
        Some(ref node) => node.color == Color::Red,
    }
}

// precondition: there exists a minimum node in the tree
fn remove_min<T, U>(tree: &mut Tree<T, U>) -> Box<Node<T, U>> {
    if let Some(ref mut node) = tree {
        if node.left.is_some() {
            let should_shift = {
                if let Some(ref child) = node.left {
                    child.color != Color::Red && !is_red(&child.left)
                } else {
                    false
                }
            };
            if should_shift {
                node.shift_left();
            }

            let ret = remove_min(&mut node.left);
            node.balance();
            return ret;
        }
    }

    let mut node = tree.take().expect("Expected a non-empty tree.");
    *tree = node.right.take();
    node
}

fn combine_subtrees<T, U>(
    left_tree: Tree<T, U>,
    mut right_tree: Tree<T, U>,
    color: Color,
) -> Tree<T, U> {
    let mut new_root = remove_min(&mut right_tree);
    new_root.left = left_tree;
    new_root.right = right_tree;
    new_root.color = color;
    Some(new_root)
}

pub fn fix_root<T, U>(tree: &mut Tree<T, U>) {
    if let Some(ref mut node) = tree {
        if !is_red(&node.left) && !is_red(&node.right) {
            node.color = Color::Red;
        }
    }
}

pub fn insert<T, U>(tree: &mut Tree<T, U>, new_node: Node<T, U>) -> Option<Entry<T, U>>
where
    T: Ord,
{
    let ret = match tree {
        Some(ref mut node) => {
            match new_node.entry.key.cmp(&node.entry.key) {
                Ordering::Less => insert(&mut node.left, new_node),
                Ordering::Greater => insert(&mut node.right, new_node),
                Ordering::Equal => {
                    let Node { ref mut entry, .. } = &mut **node;
                    Some(mem::replace(entry, new_node.entry))
                },
            }
        },
        None => {
            *tree = Some(Box::new(new_node));
            return None;
        },
    };

    let node = tree.as_mut().expect("Expected non-empty tree.");

    if is_red(&node.right) && !is_red(&node.left) {
        node.rotate_left();
    }

    let should_rotate = {
        if let Some(ref child) = node.left {
            child.color == Color::Red && is_red(&child.left)
        } else {
            false
        }
    };
    if should_rotate {
        node.rotate_right();
    }

    if is_red(&node.left) && is_red(&node.right) {
        node.flip_colors();
    }

    ret
}

pub fn remove<T, U, V>(tree: &mut Tree<T, U>, key: &V) -> Option<Entry<T, U>>
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    let ret = match tree.take() {
        Some(mut node) => {
            if key < node.entry.key.borrow() {
                let should_shift = {
                    if let Some(ref child) = node.left {
                        child.color != Color::Red && !is_red(&child.left)
                    } else {
                        false
                    }
                };
                if should_shift {
                    node.shift_left();
                }

                let ret = remove(&mut node.left, key);
                *tree = Some(node);
                ret
            } else {
                if is_red(&node.left) {
                    node.rotate_right();
                }

                if key == node.entry.key.borrow() && node.right.is_none() {
                    assert!(node.left.is_none());
                    return Some(node.entry);
                }

                let should_shift = {
                    if let Some(ref child) = node.right {
                        child.color != Color::Red && !is_red(&child.left)
                    } else {
                        false
                    }
                };
                if should_shift {
                    node.shift_right();
                }

                if key == node.entry.key.borrow() {
                    let unboxed_node = *node;
                    let Node {
                        entry,
                        left,
                        right,
                        color,
                    } = unboxed_node;
                    *tree = combine_subtrees(left, right, color);
                    Some(entry)
                } else {
                    let ret = remove(&mut node.right, key);
                    *tree = Some(node);
                    ret
                }
            }
        },
        None => return None,
    };

    let node = tree.as_mut().expect("Expected non-empty tree.");
    node.balance();

    ret
}

pub fn get<'a, T, U, V>(tree: &'a Tree<T, U>, key: &V) -> Option<&'a Entry<T, U>>
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    tree.as_ref().and_then(|node| {
        match key.cmp(node.entry.key.borrow()) {
            Ordering::Less => get(&node.left, key),
            Ordering::Greater => get(&node.right, key),
            Ordering::Equal => Some(&node.entry),
        }
    })
}

pub fn get_mut<'a, T, U, V>(tree: &'a mut Tree<T, U>, key: &V) -> Option<&'a mut Entry<T, U>>
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    tree.as_mut().and_then(|node| {
        match key.cmp(node.entry.key.borrow()) {
            Ordering::Less => get_mut(&mut node.left, key),
            Ordering::Greater => get_mut(&mut node.right, key),
            Ordering::Equal => Some(&mut node.entry),
        }
    })
}

pub fn ceil<'a, T, U, V>(tree: &'a Tree<T, U>, key: &V) -> Option<&'a Entry<T, U>>
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    tree.as_ref().and_then(|node| {
        match key.cmp(node.entry.key.borrow()) {
            Ordering::Greater => ceil(&node.right, key),
            Ordering::Less => {
                match ceil(&node.left, key) {
                    None => Some(&node.entry),
                    res => res,
                }
            },
            Ordering::Equal => Some(&node.entry),
        }
    })
}

pub fn floor<'a, T, U, V>(tree: &'a Tree<T, U>, key: &V) -> Option<&'a Entry<T, U>>
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    tree.as_ref().and_then(|node| {
        match key.cmp(node.entry.key.borrow()) {
            Ordering::Less => floor(&node.left, key),
            Ordering::Greater => {
                match floor(&node.right, key) {
                    None => Some(&node.entry),
                    res => res,
                }
            },
            Ordering::Equal => Some(&node.entry),
        }
    })
}

pub fn min<T, U>(tree: &Tree<T, U>) -> Option<&Entry<T, U>>
where
    T: Ord,
{
    tree.as_ref().and_then(|node| {
        let mut curr = node;
        while let Some(ref left_node) = curr.left {
            curr = left_node;
        }
        Some(&curr.entry)
    })
}

pub fn max<T, U>(tree: &Tree<T, U>) -> Option<&Entry<T, U>>
where
    T: Ord,
{
    tree.as_ref().and_then(|node| {
        let mut curr = node;
        while let Some(ref right_node) = curr.right {
            curr = right_node;
        }
        Some(&curr.entry)
    })
}
