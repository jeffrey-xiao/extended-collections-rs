macro_rules! init_array(
    ($ty:ty, $len:expr, $val:expr) => (
        {
            let mut array: [$ty; $len] = unsafe { mem::uninitialized() };
            for i in array.iter_mut() {
                unsafe { ::std::ptr::write(i, $val); }
            }
            array
        }
    )
);

pub mod node;
pub mod pager;
pub mod tree;

pub use self::tree::Tree;

const INTERNAL_DEGREE: usize = 3;
const LEAF_DEGREE: usize = 3;
