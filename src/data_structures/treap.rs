extern crate rand;
use self::rand::Rng;
use std::vec::Vec;
use util;

struct Node <T: PartialOrd + Clone, U> {
    key: T,
    value: U,
    priority: u32,
    size: usize,
    left: Tree<T, U>,
    right: Tree<T, U>,
}

type Tree<T, U> = Option<Box<Node<T, U>>>;

pub struct Treap<T: PartialOrd + Clone, U>(Tree<T, U>);

impl<T: PartialOrd + Clone, U> Treap<T, U> {
    pub fn new() -> Self { Treap(None) }

    fn update(tree: &mut Tree<T, U>) {
        let tree_opt = tree.take();
        if let Some(node) = tree_opt {
            let Node {key, value, priority, left, right, .. } = util::unbox(node);
            let mut size = 1;
            if let Some(ref l_node) = left {
                size += l_node.size;
            }
            if let Some(ref r_node) = right {
                size += r_node.size;
            }
            *tree = Some(Box::new(Node { key, value, priority, size, left, right }));
        }
    }

    fn merge(l_tree: &mut Tree<T, U>, mut r_tree: Tree<T, U>) {
        let r_tree_opt = r_tree.take();

        if let Some(r_node) = r_tree_opt {
            let mut l_tree_opt = l_tree.take();

            if l_tree_opt.is_none() {
                *l_tree = Some(r_node);
            } else {
                let mut left_merge = false;
                {
                    let l_node_opt_ref = l_tree_opt.as_ref();
                    if l_node_opt_ref.unwrap().priority > r_node.priority {
                        left_merge = true;
                    }
                }
                if left_merge {
                    let mut l_node = l_tree_opt.unwrap();
                    Self::merge(&mut l_node.right, Some(r_node));
                    *l_tree = Some(l_node);
                    Self::update(l_tree);
                } else {
                    let Node { key, value, size, priority, left, right } = util::unbox(r_node);
                    Self::merge(&mut l_tree_opt, left);
                    let new_left = Some(l_tree_opt.unwrap());
                    *l_tree = Some(Box::new(Node { key, value, size, priority, left: new_left, right }));
                    Self::update(l_tree);
                }
            }
        }
    }

    fn split(tree: &mut Tree<T, U>, k: &T) -> (Tree<T, U>, Tree<T, U>) {
        let tree_opt = tree.take();
        match tree_opt {
            Some(mut node) => {
                let mut ret = (None, None);
                if node.key < *k {
                    let res = Self::split(&mut node.right, k);
                    if res.0.is_some() {
                        ret.0 = res.0;
                    }
                    ret.1 = res.1;
                    *tree = Some(node);
                } else if node.key > *k {
                    let Node { key, value, priority, size, right, left: mut new_tree } = util::unbox(node);
                    let res = Self::split(&mut new_tree, k);
                    if res.0.is_some() {
                        ret.0 = res.0;
                    }
                    *tree = new_tree;
                    ret.1 = Some(Box::new(Node { key, value, priority, size, left: res.1, right }))
                } else {
                    let Node { key, value, priority, left, right, .. } = util::unbox(node);
                    *tree = left;
                    ret = (
                        Some(Box::new(Node { key, value, priority, size: 1, left: None, right: None})),
                        right,
                    );
                }
                Self::update(tree);
                Self::update(&mut ret.1);
                ret
            },
            None => (None, None),
        }
    }

    pub fn insert(&mut self, key: T, value: U) -> Option<(T, U)> {
        let mut rng = rand::thread_rng();
        let &mut Treap(ref mut tree) = self;

        let (old_node_opt, r_tree) = Self::split(tree, &key);

        let new_node = Some(Box::new(Node {
            key: key,
            value: value,
            priority: rng.gen::<u32>(),
            size: 1,
            left: None,
            right: None,
        }));
        Self::merge(tree, new_node);
        Self::merge(tree, r_tree);
        match old_node_opt {
            Some(old_node) => {
                let Node {key, value, .. } = util::unbox(old_node);
                Some((key, value))
            }
            None => None,
        }
    }

    pub fn remove(&mut self, key: &T) -> Option<(T, U)> {
        let &mut Treap(ref mut tree) = self;
        let (old_node_opt, r_tree) = Self::split(tree, key);
        Self::merge(tree, r_tree);
        match old_node_opt {
            Some(old_node) => {
                let Node {key, value, .. } = util::unbox(old_node);
                Some((key, value))
            }
            None => None,
        }
    }

    fn tree_traverse<'a>(tree: &'a Tree<T, U>, v: &mut Vec<(&'a T, &'a U)>) {
        if let Some(ref node) = *tree {
            if node.left.is_some() {
                Self::tree_traverse(&node.left, v);
            }
            v.push((&node.key, &node.value));
            if node.right.is_some() {
                Self::tree_traverse(&node.right, v);
            }
        }
    }

    pub fn traverse(&self) -> Vec<(&T, &U)> {
        let &Treap(ref tree) = self;
        let mut ret = Vec::new();
        Self::tree_traverse(tree, &mut ret);
        ret
    }

    fn tree_contains(tree: &Tree<T, U>, k: &T) -> bool {
        match *tree {
            Some(ref node) => {
                if k == &node.key {
                    true
                } else if k < &node.key {
                    Self::tree_contains(&node.left, k)
                } else {
                    Self::tree_contains(&node.right, k)
                }
            },
            None => false,
        }
    }

    pub fn contains(&self, k: &T) -> bool {
         let &Treap(ref n) = self;
         Self::tree_contains(n, k)
    }

    fn tree_get<'a>(tree: &'a Tree<T, U>, k: &T) -> Option<&'a U> {
        match *tree {
            Some(ref node) => {
                if k == &node.key {
                    Some(&node.value)
                } else if k < &node.key {
                    Self::tree_get(&node.left, k)
                } else {
                    Self::tree_get(&node.right, k)
                }
            }
            None => None,
        }
    }

    pub fn get(&self, k: &T) -> Option<&U> {
        let &Treap(ref tree) = self;
        Self::tree_get(tree, k)
    }

    fn tree_get_mut<'a>(tree: &'a mut Tree<T, U>, k: &T) -> Option<&'a mut U> {
        match *tree {
            Some(ref mut node) => {
                if k == &node.key {
                    Some(&mut node.value)
                } else if k < &node.key {
                    Self::tree_get_mut(&mut node.left, k)
                } else {
                    Self::tree_get_mut(&mut node.right, k)
                }
            }
            None => None,
        }
    }

    pub fn get_mut(&mut self, k: &T) -> Option<&mut U> {
        let &mut Treap(ref mut tree) = self;
        Self::tree_get_mut(tree, k)
    }

    pub fn size(&self) -> usize {
        let &Treap(ref tree) = self;
        match *tree {
            Some(ref node) => node.size,
            None => 0,
        }
    }

    fn tree_ceil<'a>(tree: &'a Tree<T, U>, k: &T) -> Option<&'a T> {
        match *tree {
            Some(ref node) => {
                if &node.key == k {
                    Some(&node.key)
                } else if &node.key < k {
                    Self::tree_ceil(&node.right, k)
                } else {
                    let res = Self::tree_ceil(&node.left, k);
                    if res.is_some() {
                        res
                    } else {
                        Some(&node.key)
                    }
                }
            },
            None => None,
        }
    }

    pub fn ceil(&self, k: &T) -> Option<&T> {
        let &Treap(ref tree) = self;
        Self::tree_ceil(tree, k)
    }

    fn tree_floor<'a>(tree: &'a Tree<T, U>, k: &T) -> Option<&'a T> {
        match *tree {
            Some(ref node) => {
                if &node.key == k {
                    Some(&node.key)
                } else if &node.key > k {
                    Self::tree_floor(&node.left, k)
                } else {
                    let res = Self::tree_floor(&node.right, k);
                    if res.is_some() {
                        res
                    } else {
                        Some(&node.key)
                    }
                }
            },
            None => None,
        }
    }

    pub fn floor(&self, k: &T) -> Option<&T> {
        let &Treap(ref tree) = self;
        Self::tree_floor(tree, k)
    }

    fn tree_min(tree: &Tree<T, U>) -> Option<&T> {
        match *tree {
            Some(ref node) => {
                if node.left.is_some() {
                    Self::tree_min(&node.left)
                } else {
                    Some(&node.key)
                }
            },
            None => None,
        }
    }

    pub fn min(&self) -> Option<&T> {
        let &Treap(ref tree) = self;
        Self::tree_min(tree)
    }

    fn tree_max(tree: &Tree<T, U>) -> Option<&T> {
        match *tree {
            Some(ref node) => {
                if node.right.is_some() {
                    Self::tree_max(&node.right)
                } else {
                    Some(&node.key)
                }
            },
            None => None,
        }
    }

    pub fn max(&self) -> Option<&T> {
        let &Treap(ref tree) = self;
        Self::tree_max(tree)
    }
}


#[cfg(test)]
mod tests {
    use super::Treap;

    #[test]
    fn test_size_empty() {
        let tree: Treap<u32, u32> = Treap::new();
        assert_eq!(tree.size(), 0);
    }

    #[test]
    fn test_min_max_empty() {
        let tree: Treap<u32, u32> = Treap::new();
        assert_eq!(tree.min(), None);
        assert_eq!(tree.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        assert!(tree.contains(&1));
        assert_eq!(tree.get(&1), Some(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut tree = Treap::new();
        let ret_1 = tree.insert(1, 1);
        let ret_2 = tree.insert(1, 3);
        assert_eq!(tree.get(&1), Some(&3));
        assert_eq!(ret_1, None);
        assert_eq!(ret_2, Some((1, 1)));
    }

    #[test]
    fn test_remove() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        let ret = tree.remove(&1);
        assert!(!tree.contains(&1));
        assert_eq!(ret, Some((1, 1)));
    }

    #[test]
    fn test_min_max() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        tree.insert(3, 3);
        tree.insert(5, 5);

        assert_eq!(tree.min(), Some(&1));
        assert_eq!(tree.max(), Some(&5));
    }

    #[test]
    fn test_get_mut() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        {
            let value = tree.get_mut(&1);
            *value.unwrap() = 3;
        }
        assert_eq!(tree.get(&1), Some(&3));
    }

    #[test]
    fn test_floor_ceil() {
        let mut tree = Treap::new();
        tree.insert(1, 1);
        tree.insert(3, 3);
        tree.insert(5, 5);

        assert_eq!(tree.floor(&0), None);
        assert_eq!(tree.floor(&2), Some(&1));
        assert_eq!(tree.floor(&4), Some(&3));
        assert_eq!(tree.floor(&6), Some(&5));

        assert_eq!(tree.ceil(&0), Some(&1));
        assert_eq!(tree.ceil(&2), Some(&3));
        assert_eq!(tree.ceil(&4), Some(&5));
        assert_eq!(tree.ceil(&6), None);
    }
}
