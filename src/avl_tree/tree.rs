use avl_tree::node::Node;
use entry::Entry;
use std::cmp::Ordering;
use std::mem;

pub type Tree<T, U> = Option<Box<Node<T, U>>>;

pub fn height<T, U>(tree: &Tree<T, U>) -> usize {
    match tree {
        None => 0,
        Some(ref node) => (**node).height,
    }
}

fn rotate_left<T, U>(mut node: Box<Node<T, U>>) -> Box<Node<T, U>> {
    let mut child = match node.right.take() {
        Some(child) => child,
        None => unreachable!(),
    };
    node.right = child.left.take();
    node.update();
    child.left = Some(node);
    child.update();
    child
}

fn rotate_right<T, U>(mut node: Box<Node<T, U>>) -> Box<Node<T, U>> {
    let mut child = match node.left.take() {
        Some(child) => child,
        None => unreachable!(),
    };
    node.left = child.right.take();
    node.update();
    child.right = Some(node);
    child.update();
    child
}

fn balance<T, U>(tree: &mut Tree<T, U>) {
    let mut node = match tree.take() {
        Some(node) => node,
        None => return,
    };

    node.update();

    if node.balance() > 1 {
        if let Some(child) = node.left.take() {
            if child.balance() < 0 {
                node.left = Some(rotate_left(child));
            } else {
                node.left = Some(child);
            }
        }
        node = rotate_right(node);
    } else if node.balance() < -1 {
        if let Some(child) = node.right.take() {
            if child.balance() > 0 {
                node.right = Some(rotate_right(child));
            } else {
                node.right = Some(child);
            }
        }
        node = rotate_left(node);
    }

    *tree = Some(node);
}

// precondition: there exists a minimum node in the tree
fn remove_min<T, U>(tree: &mut Tree<T, U>) -> Box<Node<T, U>> {
    if let Some(ref mut node) = tree {
        if node.left.is_some() {
            return remove_min(&mut node.left);
        }
    }

    match tree.take() {
        Some(mut node) => {
            *tree = node.right.take();
            node
        },
        _ => unreachable!(),
    }
}

fn combine_subtrees<T, U>(left_tree: Tree<T, U>, mut right_tree: Tree<T, U>) -> Tree<T, U> {
    let mut new_root = remove_min(&mut right_tree);
    new_root.left = left_tree;
    new_root.right = right_tree;
    Some(new_root)
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
                    return Some(mem::replace(entry, new_node.entry));
                },
            }
        },
        None => {
            *tree = Some(Box::new(new_node));
            return None;
        }
    };

    balance(tree);
    ret
}

pub fn remove<T, U>(tree: &mut Tree<T, U>, key: &T) -> Option<Entry<T, U>>
where
    T: Ord,
{
    let ret = match tree.take() {
        Some(mut node) => match key.cmp(&node.entry.key) {
            Ordering::Less => {
                let ret = remove(&mut node.left, key);
                *tree = Some(node);
                ret
            },
            Ordering::Greater => {
                let ret = remove(&mut node.right, key);
                *tree = Some(node);
                ret
            },
            Ordering::Equal => {
                let unboxed_node = *node;
                let Node { entry, left, right, .. } = unboxed_node;
                match (left, right) {
                    (None, right) => *tree = right,
                    (left, None) => *tree = left,
                    (left, right) => *tree = combine_subtrees(left, right),
                }
                Some(entry)
            },
        },
        None => return None,
    };

    balance(tree);
    ret
}

pub fn get<'a, T, U>(tree: &'a Tree<T, U>, key: &T) -> Option<&'a Entry<T, U>>
where
    T: Ord,
{
    tree.as_ref().and_then(|node| {
        match key.cmp(&node.entry.key) {
            Ordering::Less => get(&node.left, key),
            Ordering::Greater => get(&node.right, key),
            Ordering::Equal => Some(&node.entry),
        }
    })
}

pub fn get_mut<'a, T, U>(tree: &'a mut Tree<T, U>, key: &T) -> Option<&'a mut Entry<T, U>>
where
    T: Ord,
{
    tree.as_mut().and_then(|node| {
        match key.cmp(&node.entry.key) {
            Ordering::Less => get_mut(&mut node.left, key),
            Ordering::Greater => get_mut(&mut node.right, key),
            Ordering::Equal => Some(&mut node.entry),
        }
    })
}

pub fn ceil<'a, T, U>(tree: &'a Tree<T, U>, key: &T) -> Option<&'a Entry<T, U>>
where
    T: Ord,
{
    tree.as_ref().and_then(|node| {
        match key.cmp(&node.entry.key) {
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

pub fn floor<'a, T, U>(tree: &'a Tree<T, U>, key: &T) -> Option<&'a Entry<T, U>>
where
    T: Ord,
{
    tree.as_ref().and_then(|node| {
        match key.cmp(&node.entry.key) {
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
