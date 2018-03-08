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

pub mod node;
pub mod pager;
pub mod tree;

pub use self::tree::Tree;
