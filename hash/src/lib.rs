#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

extern crate data_structures;
extern crate rand;

mod util;
pub mod consistent_hash;
pub mod rendezvous_hash;
