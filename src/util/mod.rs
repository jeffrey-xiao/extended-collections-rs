use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub fn gen_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub fn combine_hash(x: u64, y: u64) -> u64 {
    x ^ y.wrapping_add(0x9e37_79b9).wrapping_add(x << 6).wrapping_add(x >> 2)
}

