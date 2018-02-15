use std::cmp::Ordering;
use std::mem;
use treap::node::ImplicitNode;

pub type Tree<T> = Option<Box<ImplicitNode<T>>>;

pub fn merge<T>(l_tree: &mut Tree<T>, r_tree: Tree<T>) {
    match (l_tree.take(), r_tree) {
        (Some(mut l_node), Some(mut r_node)) => {
            if l_node.priority > r_node.priority {
                merge(&mut l_node.right, Some(r_node));
                l_node.update();
                *l_tree = Some(l_node);
            } else {
                let mut new_tree = Some(l_node);
                merge(&mut new_tree, r_node.left.take());
                r_node.left = new_tree;
                r_node.update();
                *l_tree = Some(r_node);
            }
        },
        (new_tree, None) | (None, new_tree) => *l_tree = new_tree,
    }
}

pub fn split<T>(tree: &mut Tree<T>, index: usize, left_inclusive: bool) -> Tree<T> {
    match tree.take() {
        Some(mut node) => {
            let cmp = index.cmp(&node.get_implicit_key());
            let ret;
            if cmp == Ordering::Less || (cmp == Ordering::Equal && left_inclusive) {
                let mut res = split(&mut node.left, index, left_inclusive);
                *tree = node.left.take();
                node.left = res;
                node.update();
                ret = Some(node);
            } else {
                let next_index = index - node.get_implicit_key();
                ret = split(&mut node.right, next_index, left_inclusive);
                node.update();
                *tree = Some(node);
            }
            ret
        },
        None => None,
    }
}

pub fn insert<T>(tree: &mut Tree<T>, index: usize, new_node: ImplicitNode<T>)  {
    let right = split(tree, index, true);
    merge(tree, Some(Box::new(new_node)));
    merge(tree, right);
}

pub fn remove<T>(tree: &mut Tree<T>, index: usize) -> T {
    let new_tree = {
        let node = tree.as_mut().unwrap();
        let key = node.get_implicit_key();
        let &mut ImplicitNode { ref mut left, ref mut right, .. } = &mut **node;
        match index.cmp(&key) {
            Ordering::Less => {
                return remove(left, index);
            },
            Ordering::Greater => {
                return remove(right, index);
            },
            Ordering::Equal => {
                merge(left, right.take());
                left.take()
            }
        }
    };
    mem::replace(tree, new_tree).unwrap().value
}

pub fn get<'a, T>(tree: &'a Tree<T>, index: usize) -> &'a T {
    let node = tree.as_ref().unwrap();
    let key = node.get_implicit_key();
    match index.cmp(&key) {
        Ordering::Less => get(&node.left, index),
        Ordering::Greater => get(&node.right, index - key),
        Ordering::Equal => &node.value,
    }
}

pub fn get_mut<'a, T>(tree: &'a mut Tree<T>, index: usize) -> &'a mut T {
    let node = tree.as_mut().unwrap();
    let key = node.get_implicit_key();
    match index.cmp(&key) {
        Ordering::Less => get_mut(&mut node.left, index),
        Ordering::Greater => get_mut(&mut node.right, index - key),
        Ordering::Equal => &mut node.value,
    }
}

pub fn size<T>(tree: &Tree<T>) -> usize {
    if let Some(ref node) = *tree {
        node.size()
    } else {
        0
    }
}
