#![feature(test)]
extern crate test;
extern crate data_structures;
extern crate rand;

use test::Bencher;
use self::rand::Rng;
use data_structures::Treap;
use std::collections::BTreeMap;

#[bench]
fn bench_treap(b: &mut Bencher) {
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
fn bench_btreemap(b: &mut Bencher) {
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
