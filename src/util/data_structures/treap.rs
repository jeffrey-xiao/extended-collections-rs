extern crate rand;
use self::rand::Rng;
use std::vec::Vec;

pub enum Tree<T: PartialOrd + Clone> {
    Prop(
        T,
        u32,
        Box<Tree<T>>,
        Box<Tree<T>>,
    ),
    Empty,
}

impl<T: PartialOrd + Clone> Tree<T> {
    pub fn new() -> Self { Tree::Empty }

    fn node_size(node: &Self) -> u32 {
        use self::Tree::*;
        return match *node {
            Empty => 0,
            Prop(_, _, ref left, ref right) => 1 + Self::size(left) + Self::size(right),
        }
    }

    fn node_traverse<'a>(node: &'a Self, v: &mut Vec<&'a T>)  {
        use self::Tree::*;
        match *node {
            Prop(ref key, _, ref left, ref right) => {
                Self::node_traverse(&*left, v);
                v.push(key);
                Self::node_traverse(&*right, v);
            }
            _ => {}
        }
    }

    fn merge(left: Self, right: Self) -> Self {
        use self::Tree::*;
        match (left, right) {
            (Empty, Empty) => Empty,
            (n, Empty) => n,
            (Empty, n) => n,
            (
                Prop(lkey, lpriority, ll, lr),
                Prop(rkey, rpriority, rl, rr),
            ) => {
                if lpriority > rpriority {
                    let right = Prop(rkey, rpriority, rl, rr);
                    Prop(lkey, lpriority, ll, Box::new(Self::merge(*lr, right)))
                } else {
                    let left = Prop(lkey, rpriority, ll, lr);
                    Prop(rkey, rpriority, Box::new(Self::merge(left, *rl)), rr)
                }
            }
        }
    }

    fn split(self, k: &T) -> (Self, Self) {
        use self::Tree::*;
        match self {
            Empty => (Empty, Empty),
            Prop(key, priority, left, right) => {
                if *k > key {
                    let ret = right.split(k);
                    (
                        Prop(key, priority, left, Box::new(ret.0)),
                        ret.1,
                    )
                } else if *k < key {
                    let ret = left.split(k);
                    (
                        ret.0,
                        Prop(key, priority, Box::new(ret.1), right),
                    )
                } else {
                    (*left, *right)
                }
            }
        }
    }

    pub fn insert(mut self, key: T) -> Self {
        use self::Tree::*;
        let mut rng = rand::thread_rng();

        let (l, r) = self.split(&key);
        let new_node = Prop(key, rng.gen::<u32>(), Box::new(Empty), Box::new(Empty));
        Tree::merge(Tree::merge(l, new_node), r)
    }

    pub fn delete(mut self, key: T) -> Self {
        let (l, r) = self.split(&key);
        Tree::merge(l, r)
    }

    pub fn traverse(&self) -> Vec<&T> {
        let mut ret = Vec::new();
        Tree::node_traverse(&self, &mut ret);
        ret
    }

    pub fn size(&self) -> u32 {
        Tree::node_size(&self)
    }

    pub fn contains(&self, k: &T) -> bool {
        match self {
            &Tree::Empty => false,
            &Tree::Prop(ref key, _, ref left, ref right) => {
                if *key == *k {
                    true
                } else if *key < *k {
                    right.contains(k)
                } else {
                    left.contains(k)
                }
            }
        }
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
                    let val = rng.gen::<u32>();
                    t = t.insert(val);
                    expected.push(val);
                }

                let actual = t.traverse();

                expected.sort();
                expected.dedup();

                assert_eq!(expected.len(), actual.len());
                for i in 0..expected.len() {
                    assert_eq!(&expected[i], actual[i]);
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
    test_sorted_elements_1000000: 1000000,
}
