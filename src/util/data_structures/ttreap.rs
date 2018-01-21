extern crate rand;
use self::rand::{Rng, SeedableRng, StdRng};
use std::vec::Vec;
use std::cmp::Ordering;
use std::fmt::Debug;

fn unbox<T>(value: Box<T>) -> T {
    *value
}

#[derive(Debug)]
pub struct Node <T: PartialOrd + Clone + Debug, U: Debug> {
    key: T,
    value: U,
    priority: u32,
    size: u32,
    left: Option<Box<Node<T, U>>>,
    right: Option<Box<Node<T, U>>>,

}

pub struct Tree<T: PartialOrd + Clone + Debug, U: Debug>(Option<Box<Node<T, U>>>);

impl<T: PartialOrd + Clone + Debug, U: Debug> Tree<T, U> {
    pub fn new() -> Self { Tree(None) }

    fn merge(lTree: &mut Option<Box<Node<T, U>>>, mut rTree: Option<Box<Node<T, U>>>) {
        let rNodeOpt = rTree.take();

        if let Some(rNode) = rNodeOpt {
            let mut lNodeOpt = lTree.take();
            if lNodeOpt.is_none() {
                *lTree = Some(rNode);
            } else {
                let mut leftMerge = false;
                {
                    let mut lNodeOptRef = lNodeOpt.as_ref();
                    if lNodeOptRef.unwrap().priority > rNode.priority {
                        leftMerge = true;
                    }
                }
                if leftMerge {
                    let mut lNode = lNodeOpt.unwrap();
                    Tree::merge(&mut lNode.right, Some(rNode));
                    *lTree = Some(lNode);
                } else {
                    let Node { key, value, size, priority, left, right } = unbox(rNode);
                    Tree::merge(&mut lNodeOpt, left);
                    let mut lNode = lNodeOpt.unwrap();
                    *lTree = Some(Box::new(Node {
                        key: key,
                        value: value,
                        size: size,
                        priority: priority,
                        left: Some(lNode),
                        right: right,
                    }));
                }
            }
        }
    }

    fn split(n: &mut Option<Box<Node<T, U>>>, k: &T) -> Option<Box<Node<T, U>>> {
        let nodeOpt = n.take();
        if nodeOpt.is_none() {
            None
        } else {
            let mut node = nodeOpt.unwrap();
            if node.key < *k {
                let ret = Tree::split(&mut node.right, k);
                *n = Some(node);
                ret
            } else if node.key > *k {
                let Node { key, value, priority, size, right, mut left } = unbox(node);
                let leftChild = Tree::split(&mut left, k);
                *n = left;
                Some(Box::new(Node {
                    key: key,
                    value: value,
                    priority: priority,
                    size: size,
                    right: right,
                    left: leftChild,
                }))
            } else {
                let Node { left: left, right: right, .. } = unbox(node);
                *n = left;
                right
            }
        }
    }

    pub fn insert(&mut self, key: T, value: U) {
        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        let &mut Tree(ref mut n) = self;

        let r = Tree::split(n, &key);

        let new_node = Some(Box::new(Node {
            key: key,
            value: value,
            priority: rng.gen::<u32>(),
            size: 1,
            left: None,
            right: None,
        }));
        Tree::merge(n, new_node);
        Tree::merge(n, r);
    }

    pub fn delete(&mut self, key: &T) {
        let &mut Tree(ref mut n) = self;
        let r = Tree::split(n, &key);
        Tree::merge(n, r);
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

    // pub fn node_contains()

    // pub fn contains(&self, k: &T) -> bool {
    //     let &mut Tree(ref mut n) = self;

    // }
}
#[test]
fn test_1000() {
    let mut rng = rand::thread_rng();
    let mut t = Tree::new();
    let mut expected = Vec::new();
    for i in 0..5 {
        let key = rng.gen::<u32>();
        let val = rng.gen::<u32>();

        t.insert(key, val);
        expected.push((key, val));
    }

    let actual = t.traverse();

    for i in 0..actual.len() {
        println!("{:?}", actual[i]);
    }

    // expected.sort();
    // expected.dedup_by_key(|pair| pair.0);

    // assert_eq!(expected.len(), actual.len());
    // for i in 0..expected.len() {
    //     assert_eq!(&expected[i].0, actual[i].0);
    //     assert_eq!(&expected[i].1, actual[i].1);
    // }
}
