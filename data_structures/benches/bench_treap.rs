#![feature(test)]
extern crate data_structures;
extern crate rand;
extern crate test;

use test::Bencher;
use self::rand::Rng;
use data_structures::Treap;
use std::collections::BTreeMap;

#[bench]
fn bench_treap_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
        let mut tree = Treap::new();
        for _ in 0..100 {
            let key = rng.gen::<u32>();
            let val = rng.gen::<u32>();

            tree.insert(key, val);
        }
    });
}

#[bench]
fn bench_treap_get(b: &mut Bencher) {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut tree = Treap::new();
    let mut values = Vec::new();

    for _ in 0..100 {
        let key = rng.gen::<u32>();
        let val = rng.gen::<u32>();

        tree.insert(key, val);
        values.push(key);
    }

    b.iter(|| {
        for key in &values {
            test::black_box(tree.get(key));
        }
    });
}

#[bench]
fn bench_btreemap_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
        let mut tree = BTreeMap::new();
        for _ in 0..100 {
            let key = rng.gen::<u32>();
            let val = rng.gen::<u32>();

            tree.insert(key, val);
        }
    });
}

#[bench]
fn bench_btreemap_get(b: &mut Bencher) {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut tree = BTreeMap::new();
    let mut values = Vec::new();
    for _ in 0..100 {
        let key = rng.gen::<u32>();
        let val = rng.gen::<u32>();

        tree.insert(key, val);
        values.push(key);
    }
    b.iter(|| {
        for key in &values {
            test::black_box(tree.get(key));
        }
    });
}
