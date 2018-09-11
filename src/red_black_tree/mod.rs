//! Self-balancing binary search tree that uses a color bit to ensure that the tree remains
//! approximately balanced during insertions and deletions.

mod map;
mod node;
mod set;
mod tree;

pub use self::map::RedBlackMap;
pub use self::set::RedBlackSet;
