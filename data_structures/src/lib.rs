#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

extern crate crossbeam_epoch as epoch;
extern crate rand;

mod entry;
pub mod arena;
pub mod skiplist;
pub mod treap;
pub mod sync;
