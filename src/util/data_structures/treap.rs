extern crate rand;
use self::rand::Rng;
use std::vec::Vec;

fn unbox<T>(value: Box<T>) -> T {
    *value
}

struct Node <T: PartialOrd + Clone, U> {
    key: T,
    value: U,
    priority: u32,
    size: usize,
    left: Option<Box<Node<T, U>>>,
    right: Option<Box<Node<T, U>>>,

}

pub struct Tree<T: PartialOrd + Clone, U>(Option<Box<Node<T, U>>>);

impl<T: PartialOrd + Clone, U> Tree<T, U> {
    pub fn new() -> Self { Tree(None) }

    fn update(tree: &mut Option<Box<Node<T, U>>>) {
        let tree_opt = tree.take();
        if let Some(node) = tree_opt {
            let Node {key, value, priority, mut size, left, right } = unbox(node);
            size = 1;
            if let &Some(ref l_node) = &left {
                size += l_node.size;
            }
            if let &Some(ref r_node) = &right {
                size += r_node.size;
            }
            *tree = Some(Box::new(Node { key, value, priority, size, left, right }));
        }
    }

    fn merge(l_tree: &mut Option<Box<Node<T, U>>>, mut r_tree: Option<Box<Node<T, U>>>) {
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
                    let Node { key, value, size, priority, left, right } = unbox(r_node);
                    Self::merge(&mut l_tree_opt, left);
                    let new_left = Some(l_tree_opt.unwrap());
                    *l_tree = Some(Box::new(Node { key, value, size, priority, left: new_left, right }));
                    Self::update(l_tree);
                }
            }
        }
    }

    fn split(tree: &mut Option<Box<Node<T, U>>>, k: &T) -> (Option<Box<Node<T, U>>>, Option<Box<Node<T, U>>>) {
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
                    let Node { key, value, priority, size, right, left: mut new_tree } = unbox(node);
                    let res = Self::split(&mut new_tree, k);
                    if res.0.is_some() {
                        ret.0 = res.0;
                    }
                    *tree = new_tree;
                    ret.1 = Some(Box::new(Node { key, value, priority, size, left: res.1, right }))
                } else {
                    let Node { key, value, priority, size, left, right } = unbox(node);
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
        let &mut Tree(ref mut tree) = self;

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
                let Node {key, value, .. } = unbox(old_node);
                Some((key, value))
            }
            None => None,
        }
    }

    pub fn delete(&mut self, key: &T) -> Option<(T, U)> {
        let &mut Tree(ref mut tree) = self;
        let (old_node_opt, r_tree) = Self::split(tree, &key);
        Self::merge(tree, r_tree);
        match old_node_opt {
            Some(old_node) => {
                let Node {key, value, .. } = unbox(old_node);
                Some((key, value))
            }
            None => None,
        }
    }

    fn tree_traverse<'a>(tree: &'a Option<Box<Node<T, U>>>, v: &mut Vec<(&'a T, &'a U)>) {
        if let &Some(ref node) = tree {
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
        let &Tree(ref tree) = self;
        let mut ret = Vec::new();
        Self::tree_traverse(tree, &mut ret);
        ret
    }

    fn tree_contains(tree: &Option<Box<Node<T, U>>>, k: &T) -> bool {
        match tree {
            &Some(ref node) => {
                if k == &node.key {
                    true
                } else if k < &node.key {
                    Self::tree_contains(&node.left, k)
                } else {
                    Self::tree_contains(&node.right, k)
                }
            },
            &None => false,
        }
    }

    pub fn contains(&self, k: &T) -> bool {
         let &Tree(ref n) = self;
         Self::tree_contains(&n, k)
    }

    fn tree_get<'a>(tree: &'a mut Option<Box<Node<T, U>>>, k: &T) -> Option<&'a mut U> {
        match tree {
            &mut Some(ref mut node) => {
                if k == &node.key {
                    Some(&mut node.value)
                } else if k < &node.key {
                    Self::tree_get(&mut node.left, k)
                } else {
                    Self::tree_get(&mut node.right, k)
                }
            }
            &mut None => None,
        }
    }

    pub fn get(&mut self, k: &T) -> Option<&mut U> {
        let &mut Tree(ref mut tree) = self;
        Self::tree_get(tree, k)
    }

    pub fn size(&self) -> usize {
        let &Tree(ref tree) = self;
        match tree {
            &Some(ref node) => node.size,
            &None => 0,
        }
    }

    fn tree_ceil<'a>(tree: &'a Option<Box<Node<T, U>>>, k: &T) -> Option<&'a T> {
        match tree {
            &Some(ref node) => {
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
            &None => None,
        }
    }

    pub fn ceil(&self, k: &T) -> Option<&T> {
        let &Tree(ref tree) = self;
        Self::tree_ceil(tree, k)
    }

    fn tree_floor<'a>(tree: &'a Option<Box<Node<T, U>>>, k: &T) -> Option<&'a T> {
        match tree {
            &Some(ref node) => {
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
            &None => None,
        }
    }

    pub fn floor(&self, k: &T) -> Option<&T> {
        let &Tree(ref tree) = self;
        Self::tree_floor(tree, k)
    }

    fn tree_min(tree: &Option<Box<Node<T, U>>>) -> Option<&T> {
        match tree {
            &Some(ref node) => {
                if node.left.is_some() {
                    Self::tree_min(&node.left)
                } else {
                    Some(&node.key)
                }
            },
            &None => None,
        }
    }

    pub fn min(&self) -> Option<&T> {
        let &Tree(ref tree) = self;
        Self::tree_min(tree)
    }

    fn tree_max(tree: &Option<Box<Node<T, U>>>) -> Option<&T> {
        match tree {
            &Some(ref node) => {
                if node.right.is_some() {
                    Self::tree_max(&node.right)
                } else {
                    Some(&node.key)
                }
            },
            &None => None,
        }
    }

    pub fn max(&self) -> Option<&T> {
        let &Tree(ref tree) = self;
        Self::tree_max(tree)
    }
}

macro_rules! sorted_tests {
    ( $($name: ident: $size:expr,)* ) => {
        $(
            #[test]
            fn $name() {
                let mut rng = rand::thread_rng();
                let mut t = Tree::new();
                let mut expected = Vec::new();
                for _ in 0..$size {
                    let key = rng.gen::<u32>();
                    let val = rng.gen::<u32>();

                    if !t.contains(&key) {
                        t.insert(key, val);
                        expected.push((key, val));
                    }
                }

                let actual = t.traverse();

                expected.sort();
                expected.dedup_by_key(|pair| pair.0);

                assert_eq!(expected.len(), actual.len());
                for i in 0..expected.len() {
                    assert_eq!(&expected[i].0, actual[i].0);
                    assert_eq!(&expected[i].1, actual[i].1);
                }
            }
        )*
    }
}

sorted_tests! {
    test_integration_10: 10,
    test_integration_100: 100,
    test_integration_1000: 1000,
    test_integration_10000: 10000,
    test_integration_100000: 100000,
}
