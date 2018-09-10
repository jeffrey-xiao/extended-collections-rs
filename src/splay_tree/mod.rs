//! Self-adjusting binary search tree with the additional property that recently accessed elements
//! are quick to access again.

mod map;
mod node;
mod set;
mod tree;

pub use self::map::SplayMap;
pub use self::set::SplaySet;
