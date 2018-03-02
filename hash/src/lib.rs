#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

extern crate data_structures;
extern crate rand;

mod util;
pub mod carp;
pub mod consistent;
pub mod rendezvous;
pub mod weighted_rendezvous;
