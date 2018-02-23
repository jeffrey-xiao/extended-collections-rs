#![feature(test)]
extern crate data_structures;
extern crate rand;
extern crate test;

use test::Bencher;
use data_structures::skiplist::SkipMap;
use data_structures::treap::TreapMap;
use self::rand::Rng;
use std::collections::BTreeMap;

#[bench]
fn bench_treapmap_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
        let mut map = TreapMap::new();
        for _ in 0..100 {
            let key = rng.next_u32();
            let val = rng.next_u32();

            map.insert(key, val);
        }
    });
}

#[bench]
fn bench_treapmap_get(b: &mut Bencher) {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut map = TreapMap::new();
    let mut values = Vec::new();

    for _ in 0..100 {
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

#[bench]
fn bench_skipmap_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
        let mut map = SkipMap::new();
        for _ in 0..100 {
            let key = rng.next_u32();
            let val = rng.next_u32();

            map.insert(key, val);
        }
    });
}

#[bench]
fn bench_skipmap_get(b: &mut Bencher) {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut map = SkipMap::new();
    let mut values = Vec::new();

    for _ in 0..100 {
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

#[bench]
fn bench_btreemap_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
        let mut map = BTreeMap::new();
        for _ in 0..100 {
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
    for _ in 0..100 {
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