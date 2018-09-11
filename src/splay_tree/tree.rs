use entry::Entry;
use splay_tree::node::Node;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::mem;

pub type Tree<T, U> = Option<Box<Node<T, U>>>;

fn splay<T, U, V>(node: &mut Box<Node<T, U>>, key: &V)
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    let mut left_subtree: Tree<T, U> = None;
    let mut right_subtree: Tree<T, U> = None;
    {
        let mut left = &mut right_subtree;
        let mut right = &mut left_subtree;
        loop {
            match key.cmp(node.entry.key.borrow()) {
                Ordering::Less => {
                    let should_rotate = match &mut node.left {
                        Some(ref mut child) => key.cmp(child.entry.key.borrow()) == Ordering::Less,
                        None => break,
                    };
                    if should_rotate {
                        node.rotate_right();
                    }

                    let child = match node.left.take() {
                        Some(child) => child,
                        None => break,
                    };
                    *right= Some(mem::replace(node, child));
                    right= &mut { right }.as_mut().expect("Expected non-empty left child").left;
                },
                Ordering::Greater => {
                    let should_rotate = match &mut node.right {
                        Some(ref mut child) => key.cmp(child.entry.key.borrow()) == Ordering::Greater,
                        None => break,
                    };
                    if should_rotate {
                        node.rotate_left();
                    }

                    let child = match node.right.take() {
                        Some(child) => child,
                        None => break,
                    };
                    *left = Some(mem::replace(node, child));
                    left = &mut { left }.as_mut().expect("Expected non-empty right child").right;
                },
                Ordering::Equal => break,
            }
        }

        mem::swap(left, &mut node.left);
        mem::swap(right, &mut node.right);
    }

    node.left = right_subtree;
    node.right = left_subtree;
}

pub fn insert<T, U>(tree: &mut Tree<T, U>, mut new_node: Node<T, U>) -> Option<Entry<T, U>>
where
    T: Ord,
{
    match tree {
        Some(ref mut node) => {
            splay(node, &new_node.entry.key);
            match new_node.entry.key.cmp(&node.entry.key) {
                Ordering::Less => {
                    new_node.left = node.left.take();
                    mem::swap(&mut **node, &mut new_node);
                    node.right = Some(Box::new(new_node));
                    return None;
                },
                Ordering::Greater => {
                    new_node.right = node.right.take();
                    mem::swap(&mut **node, &mut new_node);
                    node.left = Some(Box::new(new_node));
                    return None;
                },
                Ordering::Equal => {
                    let ret = mem::replace(&mut node.entry, new_node.entry);
                    Some(ret)
                },
            }
        },
        None => {
            *tree = Some(Box::new(new_node));
            return None;
        }
    }
}

pub fn remove<T, U, V>(tree: &mut Tree<T, U>, key: &V) -> Option<Entry<T, U>>
where
    T: Borrow<V>,
    V: Ord + ?Sized,
{
    match tree {
        Some(ref mut node) => {
            splay(node, key);
            if key != node.entry.key.borrow() {
                return None;
            }
        },
        None => return None,
    };

    let unboxed_node = *tree.take().expect("Expected non-empty tree.");
    let Node { left, right, entry } = unboxed_node;
    *tree = match left {
        Some(mut left_child) => {
            splay(&mut left_child, key);
            left_child.right = right;
            Some(left_child)
        },
        None => right,
    };
    Some(entry)
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
    if let Some(ref mut node) = tree {
        splay(node, key);
        if node.entry.key.borrow() == key {
            return Some(&mut node.entry);
        }
    }
    None
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
