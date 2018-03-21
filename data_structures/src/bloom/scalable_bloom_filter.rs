use std::hash::{Hash};
use std::marker::PhantomData;

pub struct ScalableBloomFilter<T: Hash> {
    _marker: PhantomData<T>,
}
