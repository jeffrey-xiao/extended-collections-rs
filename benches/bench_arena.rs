#![feature(test)]

extern crate extended_collections;
extern crate test;

use extended_collections::arena::TypedArena;
use extended_collections::arena::Entry;
use test::Bencher;


#[bench]
fn bench_arena(b: &mut Bencher) {
    struct Test {
        pub val: i32,
        pub next: Option<Entry>,
    }

    b.iter(|| {
        let mut arena = TypedArena::new(1024);
        let mut curr = arena.allocate(Test { val: 0, next: None });
        for _ in 0..100 {
            curr = arena.allocate(Test { val: 0, next: Some(curr) });
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
        for _ in 0..100 {
            curr = Box::new(Test { val: 0, next: Some(curr) });
        }
    })
}
