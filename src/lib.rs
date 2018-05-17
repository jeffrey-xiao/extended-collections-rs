//! # extended-collections-rs
//! [![extended-collections](http://meritbadge.herokuapp.com/extended-collections)](https://crates.io/crates/extended-collections)
//! [![Documentation](https://docs.rs/extended-collections/badge.svg)](https://docs.rs/extended-collections)
//! [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
//! [![Build Status](https://travis-ci.org/jeffrey-xiao/extended-collections-rs.svg?branch=master)](https://travis-ci.org/jeffrey-xiao/extended-collections-rs)
//! [![codecov](https://codecov.io/gh/jeffrey-xiao/extended-collections-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/jeffrey-xiao/extended-collections-rs)
//!
//! `extended-collections` contains various implementations of collections that are not found in the standard library.
//!
//! ## Usage
//! Add this to your `Cargo.toml`:
//! ```toml
//! [dependencies]
//! extended-collections = "*"
//! ```
//! and this to your crate root:
//! ```rust
//! extern crate extended_collections;
//! ```
//!
//! ## References
//!  - [Scalable Bloom Filters](https://dl.acm.org/citation.cfm?id=1224501)
//!  > Almeida, Paulo Sérgio, Carlos Baquero, Nuno Preguiça, and David Hutchison. 2007. “Scalable Bloom Filters.” *Inf. Process. Lett.* 101 (6). Amsterdam, The Netherlands, The Netherlands: Elsevier North-Holland, Inc.: 255–61. doi:[10.1016/j.ipl.2006.10.007](https://doi.org/10.1016/j.ipl.2006.10.007).
//!  - [Advanced Bloom Filter Based Algorithms for Efficient Approximate Data De-Duplication in Streams](https://arxiv.org/abs/1212.3964)
//!  > Bera, Suman K., Sourav Dutta, Ankur Narang, and Souvik Bhattacherjee. 2012. “Advanced Bloom Filter Based Algorithms for Efficient Approximate Data de-Duplication in Streams.” *CoRR* abs/1212.3964. <http://arxiv.org/abs/1212.3964>.
//!  - [Fast set operations using treaps](https://dl.acm.org/citation.cfm?id=277660)
//!  > Blelloch, Guy E., and Margaret Reid-Miller. 1998. “Fast Set Operations Using Treaps.” In *Proceedings of the Tenth Annual Acm Symposium on Parallel Algorithms and Architectures*, 16–26. SPAA ’98. New York, NY, USA: ACM. doi:[10.1145/277651.277660](https://doi.org/10.1145/277651.277660).
//!  - [Cuckoo Filter: Practically Better Than Bloom](https://dl.acm.org/citation.cfm?id=2674994)
//!  > Fan, Bin, Dave G. Andersen, Michael Kaminsky, and Michael D. Mitzenmacher. 2014. “Cuckoo Filter: Practically Better Than Bloom.” In *Proceedings of the 10th Acm International on Conference on Emerging Networking Experiments and Technologies*, 75–88. CoNEXT ’14. New York, NY, USA: ACM. doi:[10.1145/2674005.2674994](https://doi.org/10.1145/2674005.2674994).
//!  - [HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
//!  > Flajolet, Philippe, Éric Fusy, Olivier Gandouet, and Frédéric Meunier. 2007. “Hyperloglog: The Analysis of a Near-Optimal Cardinality Estimation Algorithm.” In *IN Aofa ’07: PROCEEDINGS of the 2007 International Conference on Analysis of Algorithms*.
//!  - [HyperLogLog in practice: algorithmic engineering of a state of the art cardinality estimation algorithm](https://dl.acm.org/citation.cfm?id=2452456)
//!  > Heule, Stefan, Marc Nunkesser, and Alexander Hall. 2013. “HyperLogLog in Practice: Algorithmic Engineering of a State of the Art Cardinality Estimation Algorithm.” In *Proceedings of the 16th International Conference on Extending Database Technology*, 683–92. EDBT ’13. New York, NY, USA: ACM. doi:[10.1145/2452376.2452456](https://doi.org/10.1145/2452376.2452456).
//!  - [Less hashing, same performance: Building a better Bloom filter](https://dl.acm.org/citation.cfm?id=1400125)
//!  > Kirsch, Adam, and Michael Mitzenmacher. 2008. “Less Hashing, Same Performance: Building a Better Bloom Filter.” *Random Struct. Algorithms* 33 (2). New York, NY, USA: John Wiley & Sons, Inc.: 187–218. doi:[10.1002/rsa.v33:2](https://doi.org/10.1002/rsa.v33:2).
//!  - [A Skip List Cookbook.](https://dl.acm.org/citation.cfm?id=93711)
//!  > Pugh, William. 1990a. “A Skip List Cookbook.” College Park, MD, USA: University of Maryland at College Park.
//!  - [Skip Lists: A Probabilistic Alternative to Balanced Trees](https://dl.acm.org/citation.cfm?id=78977)
//!  > Pugh, William. 1990b. “Skip Lists: A Probabilistic Alternative to Balanced Trees.” *Commun. ACM* 33 (6). New York, NY, USA: ACM: 668–76. doi:[10.1145/78973.78977](https://doi.org/10.1145/78973.78977).

#![warn(missing_docs)]

extern crate bincode;
extern crate byteorder;
extern crate crossbeam_epoch as epoch;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate siphasher;

pub mod arena;
pub mod bit_array_vec;
pub mod bit_vec;
pub mod bloom;
pub mod bp_tree;
pub mod cuckoo;
mod entry;
pub mod hyperloglog;
pub mod lsm_tree;
pub mod radix;
pub mod skiplist;
pub mod sync;
pub mod treap;
