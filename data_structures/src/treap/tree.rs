use std::cmp::Ordering;
use std::mem;
use treap::node::Node;
use treap::entry::{Entry};

pub type Tree<T, U> = Option<Box<Node<T, U>>>;

pub fn merge<T: Ord, U>(l_tree: &mut Tree<T, U>, r_tree: Tree<T, U>) {
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

pub fn split<T: Ord, U>(tree: &mut Tree<T, U>, entry: &T) -> (Tree<T, U>, Tree<T, U>) {
    match tree.take() {
        Some(mut node) => {
            let mut ret;
            match entry.cmp(&node.entry.key) {
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

pub fn insert<T: Ord, U>(tree: &mut Tree<T, U>, mut new_node: Node<T, U>) -> Option<Entry<T, U>> {
    match *tree {
        Some(ref mut node) => {
            if new_node.priority <= node.priority {
                match new_node.entry.key.cmp(&node.entry.key) {
                    Ordering::Less => {
                        return insert(&mut node.left, new_node);
                    },
                    Ordering::Greater => {
                        return insert(&mut node.right, new_node);
                    },
                    Ordering::Equal => {
                        let &mut Node { ref mut entry, .. } = &mut **node;
                        return Some(mem::replace(entry, new_node.entry));
                    },
                }
            }
        }
        None => {
            *tree = Some(Box::new(new_node));
            return None
        }
    }
    new_node.left = tree.take();
    let (dup_opt, right) = split(&mut new_node.left, &new_node.entry.key);
    new_node.right = right;
    *tree = Some(Box::new(new_node));
    dup_opt.map(|node| node.entry)
}

pub fn remove<T: Ord, U>(tree: &mut Tree<T, U>, key: &T) -> Option<Entry<T, U>> {
    let mut new_tree;
    match *tree {
        Some(ref mut node) => {
            match key.cmp(&node.entry.key) {
                Ordering::Less => {
                    return remove(&mut node.left, key);
                },
                Ordering::Greater => {
                    return remove(&mut node.right, key);
                },
                Ordering::Equal => {
                    new_tree = node.left.take();
                    merge(&mut new_tree, node.right.take());
                }
            }
        }
        None => {
            return None;
        }
    }
    mem::replace(tree, new_tree).map(|node| node.entry)
}

pub fn contains<T: Ord, U>(tree: &Tree<T, U>, entry: &T) -> bool {
    match *tree {
        Some(ref node) => {
            match entry.cmp(&node.entry.key) {
                Ordering::Less => contains(&node.left, entry),
                Ordering::Greater => contains(&node.right, entry),
                Ordering::Equal => true,
            }
        },
        None => false,
    }
}

pub fn get<'a, T: Ord, U>(tree: &'a Tree<T, U>, entry: &T) -> Option<&'a Entry<T, U>> {
    tree.as_ref().and_then(|node| {
        match entry.cmp(&node.entry.key) {
            Ordering::Less => get(&node.left, entry),
            Ordering::Greater => get(&node.right, entry),
            Ordering::Equal => Some(&node.entry),
        }
    })
}

pub fn get_mut<'a, T: Ord, U>(tree: &'a mut Tree<T, U>, entry: &T) -> Option<&'a mut Entry<T, U>> {
    tree.as_mut().and_then(|node| {
        match entry.cmp(&node.entry.key) {
            Ordering::Less => get_mut(&mut node.left, entry),
            Ordering::Greater => get_mut(&mut node.right, entry),
            Ordering::Equal => Some(&mut node.entry),
        }
    })
}

pub fn ceil<'a, T: Ord, U>(tree: &'a Tree<T, U>, entry: &T) -> Option<&'a Entry<T, U>> {
    tree.as_ref().and_then(|node| {
        match entry.cmp(&node.entry.key) {
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

pub fn floor<'a, T: Ord, U>(tree: &'a Tree<T, U>, entry: &T) -> Option<&'a Entry<T, U>> {
    tree.as_ref().and_then(|node| {
        match entry.cmp(&node.entry.key) {
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

pub fn min<T: Ord, U>(tree: &Tree<T, U>) -> Option<&Entry<T, U>> {
    tree.as_ref().and_then(|node| {
        let mut curr = node;
        while let Some(ref left_node) = curr.left {
            curr = left_node;
        }
        Some(&curr.entry)
    })
}

pub fn max<T: Ord, U>(tree: &Tree<T, U>) -> Option<&Entry<T, U>> {
    tree.as_ref().and_then(|node| {
        let mut curr = node;
        while let Some(ref right_node) = curr.right {
            curr = right_node;
        }
        Some(&curr.entry)
    })
}

pub fn union<T: Ord, U>(left_tree: Tree<T, U>, right_tree: Tree<T, U>, mut swapped: bool) -> (Tree<T, U>, usize) {
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
                let (duplicate_opt, right_right_subtree) = split(&mut right_left_subtree, &entry.key);
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

pub fn inter<T: Ord, U>(left_tree: Tree<T, U>, right_tree: Tree<T, U>, mut swapped: bool) -> (Tree<T, U>, usize) {
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
            let (duplicate_opt, right_right_subtree) = split(&mut right_left_subtree, &entry.key);
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

pub fn subtract<T: Ord, U>(left_tree: Tree<T, U>, right_tree: Tree<T, U>, mut swapped: bool) -> (Tree<T, U>, usize) {
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
                let (duplicate_opt, right_right_subtree) = split(&mut right_left_subtree, &entry.key);
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
