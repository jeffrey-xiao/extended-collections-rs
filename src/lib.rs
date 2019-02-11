//! # extended-collections-rs
//!
//! [![extended-collections](http://meritbadge.herokuapp.com/extended-collections)](https://crates.io/crates/extended-collections)
//! [![Documentation](https://docs.rs/extended-collections/badge.svg)](https://docs.rs/extended-collections)
//! [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
//! [![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
//! [![Build Status](https://travis-ci.org/jeffrey-xiao/extended-collections-rs.svg?branch=master)](https://travis-ci.org/jeffrey-xiao/extended-collections-rs)
//! [![codecov](https://codecov.io/gh/jeffrey-xiao/extended-collections-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/jeffrey-xiao/extended-collections-rs)
//!
//! `extended-collections` contains various implementations of collections that are not found in the
//! standard library.
//!
//! ## Usage
//!
//! Add this to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! extended-collections = "*"
//! ```
//!
//! and this to your crate root:
//!
//! ```rust
//! extern crate extended_collections;
//! ```
//!
//! ## Changelog
//!
//! See [CHANGELOG](CHANGELOG.md) for more details.
//!
//! ## References
//!
//! - [Fast set operations using treaps](https://dl.acm.org/citation.cfm?id=277660)
//!   > Blelloch, Guy E., and Margaret Reid-Miller. 1998. “Fast Set Operations Using Treaps.” In *Proceedings of the Tenth Annual Acm Symposium on Parallel Algorithms and Architectures*, 16–26. SPAA ’98. New York, NY, USA: ACM. doi:[10.1145/277651.277660](https://doi.org/10.1145/277651.277660).
//! - [A Skip List Cookbook.](https://dl.acm.org/citation.cfm?id=93711)
//!   > Pugh, William. 1990a. “A Skip List Cookbook.” College Park, MD, USA: University of Maryland at College Park.
//! - [Skip Lists: A Probabilistic Alternative to Balanced Trees](https://dl.acm.org/citation.cfm?id=78977)
//!   > Pugh, William. 1990b. “Skip Lists: A Probabilistic Alternative to Balanced Trees.” *Commun. ACM* 33 (6). New York, NY, USA: ACM: 668–76. doi:[10.1145/78973.78977](https://doi.org/10.1145/78973.78977).
//!
//! ## License
//!
//! `extended-collections-rs` is dual-licensed under the terms of either the MIT License or the
//! Apache License (Version 2.0).
//!
//! See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT) for more details.

#![warn(missing_docs)]

pub mod arena;
pub mod avl_tree;
pub mod bp_tree;
mod entry;
pub mod lsm_tree;
pub mod radix;
pub mod red_black_tree;
pub mod skiplist;
pub mod splay_tree;
pub mod sync;
pub mod treap;
