#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate rand;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

mod util;
pub mod data_structures;
pub mod hash;
pub mod kademlia;
