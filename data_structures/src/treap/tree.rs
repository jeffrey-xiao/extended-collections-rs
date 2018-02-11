use std::cmp::Ordering;
use std::mem;
use treap::node::Node;
use treap::entry::{Entry};

pub type Tree<T> = Option<Box<Node<T>>>;

enum RemovalCases {
    RemoveNode,
    RotateLeft,
    RotateRight,
}

pub fn merge<T: Entry>(l_tree: &mut Tree<T>, r_tree: Tree<T>) {
    match (l_tree.take(), r_tree) {
        (Some(mut l_node), Some(mut r_node)) => {
            if l_node.priority > r_node.priority {
                merge(&mut l_node.right, Some(r_node));
                *l_tree = Some(l_node);
            } else {
                let mut new_tree = Some(l_node);
                merge(&mut new_tree, r_node.left.take());
                r_node.left = new_tree;
                *l_tree = Some(r_node);
            }
        },
        (new_tree, None) | (None, new_tree) => *l_tree = new_tree,
    }
}

pub fn split<T: Entry>(tree: &mut Tree<T>, entry: &T::Output) -> (Tree<T>, Tree<T>) {
    match tree.take() {
        Some(mut node) => {
            let mut ret;
            match entry.cmp(node.entry.get_key()) {
                Ordering::Less => {
                    let mut res = split(&mut node.left, entry);
                    *tree = node.left.take();
                    node.left = res.1;
                    ret = (res.0, Some(node));
                },
                Ordering::Greater => {
                    ret = split(&mut node.right, entry);
                    *tree = Some(node);
                },
                Ordering::Equal => {
                    *tree = node.left.take();
                    let right = node.right.take();
                    ret = (Some(node), right);
                },
            }
            ret
        },
        None => (None, None),
    }
}

pub fn insert<T: Entry>(tree: &mut Tree<T>, new_node: Node<T>) -> Option<T> {
    match *tree {
        Some(ref mut node) => {
            let ret;
            match new_node.entry.get_key().cmp(node.entry.get_key()) {
                Ordering::Less => {
                    ret = insert(&mut node.left, new_node);
                    if node.is_heap_property_violated(&node.left) {
                        node.rotate_right();
                    }
                },
                Ordering::Greater => {
                    ret = insert(&mut node.right, new_node);
                    if node.is_heap_property_violated(&node.right) {
                        node.rotate_left();
                    }
                },
                Ordering::Equal => {
                    let &mut Node { ref mut entry, .. } = &mut **node;
                    ret = Some(mem::replace(entry, new_node.entry));
                },
            }
            ret
        }
        None => {
            *tree = Some(Box::new(new_node));
            None
        }
    }
}

fn get_removal_rotation_case<T: Entry>(tree: &Tree<T>) -> Option<RemovalCases> {
    tree.as_ref().and_then(|node| {
        let Node { ref left, ref right, .. } = **node;
        match (left, right) {
            (&Some(ref left_node), &Some(ref right_node)) => {
                if left_node.priority > right_node.priority {
                    Some(RemovalCases::RotateRight)
                } else {
                    Some(RemovalCases::RotateLeft)
                }
            },
            (&Some(_), &None) => Some(RemovalCases::RotateRight),
            (&None, &Some(_)) => Some(RemovalCases::RotateLeft),
            (&None, &None) => Some(RemovalCases::RemoveNode),
        }
    })
}

fn rotate_down<T: Entry>(tree: &mut Tree<T>) -> Option<T> {
    get_removal_rotation_case(tree).and_then(|case| {
        match case {
            RemovalCases::RotateRight => tree.as_mut().and_then(|node| {
                node.rotate_right();
                rotate_down(&mut node.right)
            }),
            RemovalCases::RotateLeft => tree.as_mut().and_then(|node| {
                node.rotate_left();
                rotate_down(&mut node.left)
            }),
            RemovalCases::RemoveNode => tree.take().map(|node| node.entry),
        }
    })
}

pub fn remove<T: Entry>(mut tree: &mut Tree<T>, entry: &T::Output) -> Option<T> {
    tree.as_mut()
        .and_then(|node| Some(entry.cmp(node.entry.get_key())))
        .and_then(|ordering| {
            match ordering {
                Ordering::Less => tree.as_mut().and_then(|node| remove(&mut node.left, entry)),
                Ordering::Greater => tree.as_mut().and_then(|node| remove(&mut node.right, entry)),
                Ordering::Equal => rotate_down(&mut tree),
            }
        })
}

pub fn contains<T: Entry>(tree: &Tree<T>, entry: &T::Output) -> bool {
    match *tree {
        Some(ref node) => {
            match entry.cmp(node.entry.get_key()) {
                Ordering::Less => contains(&node.left, entry),
                Ordering::Greater => contains(&node.right, entry),
                Ordering::Equal => true,
            }
        },
        None => false,
    }
}

pub fn get<'a, T: Entry>(tree: &'a Tree<T>, entry: &T::Output) -> Option<&'a T> {
    tree.as_ref().and_then(|node| {
        match entry.cmp(node.entry.get_key()) {
            Ordering::Less => get(&node.left, entry),
            Ordering::Greater => get(&node.right, entry),
            Ordering::Equal => Some(&node.entry),
        }
    })
}

pub fn get_mut<'a, T: Entry>(tree: &'a mut Tree<T>, entry: &T::Output) -> Option<&'a mut T> {
    tree.as_mut().and_then(|node| {
        match entry.cmp(node.entry.get_key()) {
            Ordering::Less => get_mut(&mut node.left, entry),
            Ordering::Greater => get_mut(&mut node.right, entry),
            Ordering::Equal => Some(&mut node.entry),
        }
    })
}

pub fn ceil<'a, T: Entry>(tree: &'a Tree<T>, entry: &T::Output) -> Option<&'a T> {
    tree.as_ref().and_then(|node| {
        match entry.cmp(node.entry.get_key()) {
            Ordering::Greater => ceil(&node.right, entry),
            Ordering::Less => {
                match ceil(&node.left, entry) {
                    None => Some(&node.entry),
                    res => res
                }
            },
            Ordering::Equal => Some(&node.entry),
        }
    })
}

pub fn floor<'a, T: Entry>(tree: &'a Tree<T>, entry: &T::Output) -> Option<&'a T> {
    tree.as_ref().and_then(|node| {
        match entry.cmp(node.entry.get_key()) {
            Ordering::Less => floor(&node.left, entry),
            Ordering::Greater => {
                match floor(&node.right, entry) {
                    None => Some(&node.entry),
                    res => res
                }
            },
            Ordering::Equal => Some(&node.entry),
        }
    })
}

pub fn min<T: Entry>(tree: &Tree<T>) -> Option<&T> {
    tree.as_ref().and_then(|node| {
        let mut curr = node;
        while let Some(ref left_node) = curr.left {
            curr = left_node;
        }
        Some(&curr.entry)
    })
}

pub fn max<T: Entry>(tree: &Tree<T>) -> Option<&T> {
    tree.as_ref().and_then(|node| {
        let mut curr = node;
        while let Some(ref right_node) = curr.right {
            curr = right_node;
        }
        Some(&curr.entry)
    })
}

pub fn union<T: Entry>(left_tree: Tree<T>, right_tree: Tree<T>, mut swapped: bool) -> (Tree<T>, usize) {
    match (left_tree, right_tree) {
        (Some(mut left_node), Some(mut right_node)) => {
            if left_node.priority < right_node.priority {
                mem::swap(&mut left_node, &mut right_node);
                swapped = !swapped;
            }
            let mut dups = 0;
            {
                let &mut Node {
                    left: ref mut left_subtree,
                    right: ref mut right_subtree,
                    ref mut entry,
                    ..
                } = &mut *left_node;
                let mut right_left_subtree = Some(right_node);
                let (duplicate_opt, right_right_subtree) = split(&mut right_left_subtree, entry.get_key());
                let (new_left_subtree, left_dups) = union(left_subtree.take(), right_left_subtree, swapped);
                let (new_right_subtree, right_dups) = union(right_subtree.take(), right_right_subtree, swapped);
                dups += left_dups + right_dups;
                *left_subtree = new_left_subtree;
                *right_subtree = new_right_subtree;
                if let Some(duplicate_node) = duplicate_opt {
                    if swapped {
                        *entry = duplicate_node.entry;
                    }
                    dups += 1;
                }
            }
            (Some(left_node), dups)
        },
        (None, right_tree) => (right_tree, 0),
        (left_tree, None) => (left_tree, 0),
    }
}

pub fn inter<T: Entry>(left_tree: Tree<T>, right_tree: Tree<T>, mut swapped: bool) -> (Tree<T>, usize) {
    if let (Some(mut left_node), Some(mut right_node)) = (left_tree, right_tree) {
        let mut dups = 0;
        {
            if left_node.priority < right_node.priority {
                mem::swap(&mut left_node, &mut right_node);
                swapped = !swapped;
            }
            let &mut Node {
                left: ref mut left_subtree,
                right: ref mut right_subtree,
                ref mut entry,
                ..
            } = &mut *left_node;
            let mut right_left_subtree = Some(right_node);
            let (duplicate_opt, right_right_subtree) = split(&mut right_left_subtree, entry.get_key());
            let (new_left_subtree, left_dups) = inter(left_subtree.take(), right_left_subtree, swapped);
            let (new_right_subtree, right_dups) = inter(right_subtree.take(), right_right_subtree, swapped);
            dups += left_dups + right_dups;
            *left_subtree = new_left_subtree;
            *right_subtree = new_right_subtree;
            match duplicate_opt {
                Some(duplicate_node) => {
                    if swapped {
                        *entry = duplicate_node.entry;
                    }
                    dups += 1;
                },
                None => {
                    merge(left_subtree, right_subtree.take());
                    return (left_subtree.take(), dups);
                },
            }
        }
        (Some(left_node), dups)
    } else {
        (None, 0)
    }
}

pub fn subtract<T: Entry>(left_tree: Tree<T>, right_tree: Tree<T>, mut swapped: bool) -> (Tree<T>, usize) {
    match (left_tree, right_tree) {
        (Some(mut left_node), Some(mut right_node)) => {
            let mut dups = 0;
            {
                if left_node.priority < right_node.priority {
                    mem::swap(&mut left_node, &mut right_node);
                    swapped = !swapped;
                }
                let &mut Node {
                    left: ref mut left_subtree,
                    right: ref mut right_subtree,
                    ref mut entry,
                    ..
                } = &mut *left_node;
                let mut right_left_subtree = Some(right_node);
                let (duplicate_opt, right_right_subtree) = split(&mut right_left_subtree, entry.get_key());
                let (new_left_subtree, left_dups) = subtract(left_subtree.take(), right_left_subtree, swapped);
                let (new_right_subtree, right_dups) = subtract(right_subtree.take(), right_right_subtree, swapped);
                dups += left_dups + right_dups;
                *left_subtree = new_left_subtree;
                *right_subtree = new_right_subtree;
                if duplicate_opt.is_some() || swapped {
                    merge(left_subtree, right_subtree.take());
                    return (left_subtree.take(), dups + 1);
                }
            }
            (Some(left_node), dups)
        },
        (left_tree, right_tree) => {
            if swapped {
                (right_tree, 0)
            } else {
                (left_tree, 0)
            }
        },
    }
}
