#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate rand;

mod treap;
pub use self::treap::Treap;
pub use self::treap::TreapIterator;
