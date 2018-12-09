use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;
use std::collections::BTreeMap;

const NUM_OF_OPERATIONS: usize = 100;

fn bench_btreemap_insert(c: &mut Criterion) {
    c.bench_function("bench btreemap insert", |b| {
        b.iter(|| {
            let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
            let mut map = BTreeMap::new();
            for _ in 0..NUM_OF_OPERATIONS {
                let key = rng.next_u32();
                let val = rng.next_u32();

                map.insert(key, val);
            }
        })
    });
}

fn bench_btreemap_get(c: &mut Criterion) {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut map = BTreeMap::new();
    let mut values = Vec::new();
    for _ in 0..NUM_OF_OPERATIONS {
        let key = rng.next_u32();
        let val = rng.next_u32();

        map.insert(key, val);
        values.push(key);
    }

    c.bench_function("bench btreemap get", move |b| {
        b.iter(|| {
            for key in &values {
                black_box(map.get(key));
            }
        })
    });
}

macro_rules! bst_map_benches {
    ($($module_name:ident: $type_name:ident,)*) => {
        $(
            mod $module_name {
                use extended_collections::$module_name::$type_name;
                use rand::Rng;
                use super::NUM_OF_OPERATIONS;
                use criterion::{Criterion, black_box};

                pub fn bench_insert(c: &mut Criterion) {
                    c.bench_function(&format!("bench {} get", stringify!($module_name)), |b| b.iter(|| {
                        let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
                        let mut map = $type_name::new();
                        for _ in 0..NUM_OF_OPERATIONS {
                            let key = rng.next_u32();
                            let val = rng.next_u32();

                            map.insert(key, val);
                        }
                    }));
                }

                pub fn bench_get(c: &mut Criterion) {
                    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
                    let mut map = $type_name::new();
                    let mut values = Vec::new();

                    for _ in 0..NUM_OF_OPERATIONS {
                        let key = rng.next_u32();
                        let val = rng.next_u32();

                        map.insert(key, val);
                        values.push(key);
                    }

                    c.bench_function(&format!("bench {} insert", stringify!($module_name)), move |b| b.iter(|| {
                        for key in &values {
                            black_box(map.get(key));
                        }
                    }));
                }
            }
        )*

        criterion_group!(
            benches,
            bench_btreemap_get,
            bench_btreemap_insert,
            $(
                $module_name::bench_get,
                $module_name::bench_insert,
            )*
        );
    }
}

bst_map_benches!(
    avl_tree: AvlMap,
    red_black_tree: RedBlackMap,
    skiplist: SkipMap,
    splay_tree: SplayMap,
    treap: TreapMap,
);

criterion_main!(benches);
