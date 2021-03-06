//! Disk-resident N-ary tree.

macro_rules! init_array(
    ($ty:ty, $len:expr, $val:expr) => (
        {
            let mut v: Vec<$ty> = Vec::with_capacity($len);
            for _ in 0..$len {
                v.push($val);
            }
            v.into_boxed_slice()
        }
    )
);

mod map;
mod node;
mod pager;

pub use self::map::BpMap;
pub use self::pager::{Error, Result};
