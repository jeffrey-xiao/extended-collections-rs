#![feature(test)]

extern crate data_structures;
extern crate test;

use data_structures::pool::TypedMemoryPool;
use data_structures::pool::Entry;
use test::Bencher;


#[bench]
fn bench_pool(b: &mut Bencher) {
    struct Test {
        val: i32,
        next: Option<Entry>,
    }

    b.iter(|| {
        let mut pool = TypedMemoryPool::new(1024);
        let mut curr = pool.allocate(Test { val: 0, next: None });
        for _ in 0..100 {
            curr = pool.allocate(Test { val: 0, next: Some(curr) });
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
