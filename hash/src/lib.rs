#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]
 
extern crate rand;
extern crate data_structures;

mod util;
pub mod consistent_hash;
pub mod rendezvous_hash;
