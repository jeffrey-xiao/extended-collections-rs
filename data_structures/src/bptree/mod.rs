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

pub mod map;
pub mod node;
pub mod pager;

pub use self::map::BPMap;
