#![feature(test)]

use rand::Rng;
use std::collections::BTreeMap;
use test::Bencher;

const NUM_OF_OPERATIONS: usize = 100;

#[bench]
fn bench_btreemap_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
        let mut map = BTreeMap::new();
        for _ in 0..NUM_OF_OPERATIONS {
            let key = rng.next_u32();
            let val = rng.next_u32();

            map.insert(key, val);
        }
    });
}

#[bench]
fn bench_btreemap_get(b: &mut Bencher) {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut map = BTreeMap::new();
    let mut values = Vec::new();
    for _ in 0..NUM_OF_OPERATIONS {
        let key = rng.next_u32();
        let val = rng.next_u32();

        map.insert(key, val);
        values.push(key);
    }
    b.iter(|| {
        for key in &values {
            test::black_box(map.get(key));
        }
    });
}

macro_rules! bst_map_benches {
    ($($module_name:ident: $type_name:ident,)*) => {
        $(
            mod $module_name {
                use crate::extended_collections::$module_name::$type_name;
                use rand::Rng;
                use super::NUM_OF_OPERATIONS;
                use test::Bencher;

                #[bench]
                fn bench_insert(b: &mut Bencher) {
                    b.iter(|| {
                        let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
                        let mut map = $type_name::new();
                        for _ in 0..NUM_OF_OPERATIONS {
                            let key = rng.next_u32();
                            let val = rng.next_u32();

                            map.insert(key, val);
                        }
                    });
                }

                #[bench]
                fn bench_get(b: &mut Bencher) {
                    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
                    let mut map = $type_name::new();
                    let mut values = Vec::new();

                    for _ in 0..NUM_OF_OPERATIONS {
                        let key = rng.next_u32();
                        let val = rng.next_u32();

                        map.insert(key, val);
                        values.push(key);
                    }

                    b.iter(|| {
                        for key in &values {
                            super::test::black_box(map.get(key));
                        }
                    });
                }
            }
        )*
    }
}

bst_map_benches!(
    avl_tree: AvlMap,
    red_black_tree: RedBlackMap,
    skiplist: SkipMap,
    splay_tree: SplayMap,
    treap: TreapMap,
);
