#![feature(test)]

extern crate data_structures;
extern crate test;

use data_structures::arena::TypedArena;
use data_structures::arena::Entry;
use test::Bencher;


#[bench]
fn bench_arena(b: &mut Bencher) {
    struct Test {
        val: i32,
        next: Option<Entry>,
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
        val: i32,
        next: Option<Box<Test>>,
    }

    b.iter(|| {
        let mut curr = Box::new(Test { val: 0, next: None });
        for _ in 0..100 {
            curr = Box::new(Test { val: 0, next: Some(curr) });
        }
    })
}
