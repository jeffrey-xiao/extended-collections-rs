extern crate rand;
use self::rand::Rng;
use std::vec::Vec;
use std::fmt::Debug;

fn unbox<T>(value: Box<T>) -> T {
    *value
}

pub struct Node <T: PartialOrd + Clone, U> {
    key: T,
    value: U,
    priority: u32,
    size: u32,
    left: Option<Box<Node<T, U>>>,
    right: Option<Box<Node<T, U>>>,

}

pub struct Tree<T: PartialOrd + Clone, U>(Option<Box<Node<T, U>>>);

impl<T: PartialOrd + Clone + Debug, U: Debug> Tree<T, U> {
    pub fn new() -> Self { Tree(None) }

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
                    Tree::merge(&mut l_node.right, Some(r_node));
                    *l_tree = Some(l_node);
                } else {
                    let Node { key, value, size, priority, left, right } = unbox(r_node);
                    Tree::merge(&mut l_tree_opt, left);
                    let new_left = Some(l_tree_opt.unwrap());
                    *l_tree = Some(Box::new(Node { key, value, size, priority, left: new_left, right }));
                }
            }
        }
    }

    fn split(tree: &mut Option<Box<Node<T, U>>>, k: &T) -> Option<Box<Node<T, U>>> {
        let tree_opt = tree.take();
        if let Some(mut node) = tree_opt {
            if node.key < *k {
                let ret = Tree::split(&mut node.right, k);
                *tree = Some(node);
                ret
            } else if node.key > *k {
                let Node { key, value, priority, size, right, left: mut new_tree } = unbox(node);
                let left = Tree::split(&mut new_tree, k);
                *tree = new_tree;
                Some(Box::new(Node { key, value, priority, size, left, right }))
            } else {
                let Node { left, right, .. } = unbox(node);
                *tree = left;
                right
            }
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: T, value: U) {
        let mut rng = rand::thread_rng();
        let &mut Tree(ref mut n) = self;

        let r = Self::split(n, &key);

        let new_node = Some(Box::new(Node {
            key: key,
            value: value,
            priority: rng.gen::<u32>(),
            size: 1,
            left: None,
            right: None,
        }));
        Self::merge(n, new_node);
        Self::merge(n, r);
    }

    pub fn delete(&mut self, key: &T) {
        let &mut Tree(ref mut n) = self;
        let r = Self::split(n, &key);
        Self::merge(n, r);
    }

    fn node_traverse<'a>(node: &'a Option<Box<Node<T, U>>>, v: &mut Vec<(&'a T, &'a U)>) {
        if let &Some(ref n) = node {
            if n.left.is_some() {
                Self::node_traverse(&n.left, v);
            }
            v.push((&n.key, &n.value));
            if n.right.is_some() {
                Self::node_traverse(&n.right, v);
            }
        }
    }

    pub fn traverse(&self) -> Vec<(&T, &U)> {
        let &Tree(ref n) = self;
        let mut ret = Vec::new();
        Tree::node_traverse(n, &mut ret);
        ret
    }

    fn node_contains(node: &Option<Box<Node<T, U>>>, k: &T) -> bool {
        if let &Some(ref n) = node {
            if k == &n.key {
                true
            } else if k < &n.key {
                Self::node_contains(&n.left, k)
            } else {
                Self::node_contains(&n.right, k)
            }
        } else {
            false
        }
    }

    pub fn contains(&self, k: &T) -> bool {
         let &Tree(ref n) = self;
         Self::node_contains(&n, k)
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
                for i in 0..$size {
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
    test_sorted_elements_10: 10,
    test_sorted_elements_100: 100,
    test_sorted_elements_1000: 1000,
    test_sorted_elements_10000: 10000,
    test_sorted_elements_100000: 100000,
}

