#![feature(test)]

extern crate extended_collections;
extern crate test;

use extended_collections::arena::Entry;
use extended_collections::arena::TypedArena;
use test::Bencher;

const CHUNK_SIZE: usize = 1024;
const NUM_OF_ALLOCATIONS: usize = 100;

#[bench]
fn bench_arena(b: &mut Bencher) {
    struct Test {
        pub val: i32,
        pub next: Option<Entry>,
    }

    b.iter(|| {
        let mut arena = TypedArena::new(CHUNK_SIZE);
        let mut curr = arena.allocate(Test { val: 0, next: None });
        for _ in 0..NUM_OF_ALLOCATIONS {
            curr = arena.allocate(Test {
                val: 0,
                next: Some(curr),
            });
        }
    });
}

#[bench]
fn bench_box(b: &mut Bencher) {
    struct Test {
        pub val: i32,
        pub next: Option<Box<Test>>,
    }

    b.iter(|| {
        let mut curr = Box::new(Test { val: 0, next: None });
        for _ in 0..NUM_OF_ALLOCATIONS {
            curr = Box::new(Test {
                val: 0,
                next: Some(curr),
            });
        }
    })
}
