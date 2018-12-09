use criterion::{criterion_group, criterion_main, Criterion};
use extended_collections::arena::Entry;
use extended_collections::arena::TypedArena;

const CHUNK_SIZE: usize = 1024;
const NUM_OF_ALLOCATIONS: usize = 100;

fn bench_arena(c: &mut Criterion) {
    c.bench_function("bench arena", |b| {
        b.iter(|| {
            struct Test {
                pub val: i32,
                pub next: Option<Entry>,
            }

            let mut arena = TypedArena::new(CHUNK_SIZE);
            let mut curr = arena.allocate(Test { val: 0, next: None });
            for _ in 0..NUM_OF_ALLOCATIONS {
                curr = arena.allocate(Test {
                    val: 0,
                    next: Some(curr),
                });
            }
        })
    });
}

fn bench_box(c: &mut Criterion) {
    c.bench_function("bench box", |b| {
        b.iter(|| {
            struct Test {
                pub val: i32,
                pub next: Option<Box<Test>>,
            }

            let mut curr = Box::new(Test { val: 0, next: None });
            for _ in 0..NUM_OF_ALLOCATIONS {
                curr = Box::new(Test {
                    val: 0,
                    next: Some(curr),
                });
            }
        })
    });
}

criterion_group!(benches, bench_arena, bench_box);
criterion_main!(benches);
