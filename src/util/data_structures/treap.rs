extern crate rand;
use self::rand::Rng;
use std::vec::Vec;
use std::cmp::Ordering;

pub struct Prop <T: PartialOrd + Clone, U> {
    key: T,
    value: U,
    priority: u32,
    size: u32,
    left: Box<Tree<T, U>>,
    right: Box<Tree<T, U>>,

}

pub enum Tree<T: PartialOrd + Clone, U> {
    Prop(Prop<T, U>),
    Empty,
}

impl<T: PartialOrd + Clone, U> Tree<T, U> {
    pub fn new() -> Self { Tree::Empty }

    fn node_size(node: &Self) -> u32 {
        use self::Tree::*;
        return match *node {
            Empty => 0,
            Prop(ref prop) => 1 + Self::size(&prop.left) + Self::size(&prop.right),
        }
    }

    fn node_traverse<'a>(node: &'a Self, v: &mut Vec<(&'a T, &'a U)>)  {
        use self::Tree::*;
        match *node {
            Prop(ref prop) => {
                Self::node_traverse(&*prop.left, v);
                let entry = (&prop.key, &prop.value);
                v.push(entry);
                Self::node_traverse(&*prop.right, v);
            }
            _ => {}
        }
    }

    fn merge(left: Self, right: Self) -> Self {
        match (left, right) {
            (Tree::Empty, Tree::Empty) => Tree::Empty,
            (n, Tree::Empty) => n,
            (Tree::Empty, n) => n,
            (Tree::Prop(l), Tree::Prop(r)) => {
                if l.priority > r.priority {
                    let foo = Tree::Prop(Prop{ .. r });
                    Tree::Prop(Prop {
                        right: Box::new(Self::merge(*l.right, foo)),
                        .. l
                    })
                } else {
                    let foo = Tree::Prop(Prop{ .. l });
                    Tree::Prop(Prop {
                        left: Box::new(Self::merge(foo, *r.left)),
                        .. r
                    })
                }
            }
        }
    }

    fn split(self, k: &T) -> (Self, Self) {
        match self {
            Tree::Empty => (Tree::Empty, Tree::Empty),
            Tree::Prop(p) => {
                if *k > p.key {
                    let ret = p.right.split(k);
                    (
                        Tree::Prop(Prop { right: Box::new(ret.0), .. p }),
                        ret.1,
                    )
                } else if *k < p.key {
                    let ret = p.left.split(k);
                    (
                        ret.0,
                        Tree::Prop(Prop {
                            left: Box::new(ret.1),
                            .. p
                        }),
                    )
                } else {
                    (*p.left, *p.right)
                }
            }
        }
    }

    pub fn insert(mut self, key: T, value: U) -> Self {
        let mut rng = rand::thread_rng();

        let (l, r) = self.split(&key);
        let new_node = Tree::Prop(Prop {
            key: key,
            value: value,
            priority: rng.gen::<u32>(),
            size: 1,
            left: Box::new(Tree::Empty),
            right: Box::new(Tree::Empty),
        });
        Tree::merge(Tree::merge(l, new_node), r)
    }

    pub fn delete(mut self, key: T) -> Self {
        let (l, r) = self.split(&key);
        Tree::merge(l, r)
    }

    pub fn traverse(&self) -> Vec<(&T, &U)> {
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
            &Tree::Prop(ref p) => {
                if p.key == *k {
                    true
                } else if p.key < *k {
                    p.right.contains(k)
                } else {
                    p.left.contains(k)
                }
            }
        }
    }

    pub fn max(&self) -> Option<T> {
        match self {
            &Tree::Empty => None,
            &Tree::Prop(ref p) => {
                match &*p.right {
                    &Tree::Empty => Some(p.key.clone()),
                    &Tree::Prop(ref q) => p.right.max()
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
                    let key = rng.gen::<u32>();
                    let val = rng.gen::<u32>();

                    if !t.contains(&key) {
                        t = t.insert(key, val);
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
