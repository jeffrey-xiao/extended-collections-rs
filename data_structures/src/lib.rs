#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

extern crate rand;

mod treap;
pub use self::treap::map::TreapMap;
pub use self::treap::map::TreapMapIterator;
