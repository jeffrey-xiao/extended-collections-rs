use bit_vec::BitVec;
use rand::{Rng, XorShiftRng};
use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

/// A space-efficient probabilistic data structure to test for membership in a set.
///
/// At its core, a bloom filter is a bit array, initially all set to zero. `K` hash functions
/// map each element to `K` bits in the bit array. An element definitely does not exist in the
/// bloom filter if any of the `K` bits are unset. An element is possibly in the set if all of the
/// `K` bits are set. This particular implementation of a bloom filter uses two hash functions to
/// simulate `K` hash functions. Additionally, it operates on only one "slice" in order to have
/// predictable memory usage.
///
/// # Examples
/// ```
/// use data_structures::bloom::BloomFilter;
///
/// let mut filter = BloomFilter::new(100, 10);
///
/// assert!(!filter.contains(&"foo"));
/// filter.insert(&"foo");
/// assert!(filter.contains(&"foo"));
///
/// filter.clear();
/// assert!(!filter.contains(&"foo"));
///
/// assert_eq!(filter.len(), 100);
/// assert_eq!(filter.hasher_count(), 7);
/// ```
pub struct BloomFilter<T: Hash> {
    bit_vec: BitVec,
    hashers: [SipHasher; 2],
    hasher_count: usize,
    _marker: PhantomData<T>,
}

impl<T: Hash> BloomFilter<T> {
    fn get_hashers() -> [SipHasher; 2] {
        let mut rng = XorShiftRng::new_unseeded();
        [
            SipHasher::new_with_keys(rng.next_u64(), rng.next_u64()),
            SipHasher::new_with_keys(rng.next_u64(), rng.next_u64()),
        ]
    }

    fn get_hasher_count(bit_count: usize, item_count: usize) -> usize {
        ((bit_count as f64) / (item_count as f64) * 2f64.ln()).ceil() as usize
    }

    /// Constructs a new, empty `BloomFilter<T>` with `bit_count` bits and an estimated max
    /// capacity of `item_count` items.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let filter: BloomFilter<u32> = BloomFilter::new(100, 10);
    /// ```
    pub fn new(bit_count: usize, item_count: usize) -> Self {
        BloomFilter {
            bit_vec: BitVec::new(bit_count),
            hasher_count: Self::get_hasher_count(bit_count, item_count),
            hashers: Self::get_hashers(),
            _marker: PhantomData,
        }
    }

    /// Constructs a new, empty `BloomFilter<T>` with an estimated max capacity of `item_count`
    /// items and a maximum false positive probability of `fpp`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let filter: BloomFilter<u32> = BloomFilter::from_fpp(10, 0.01);
    /// ```
    pub fn from_fpp(item_count: usize, fpp: f64) -> Self {
        let bit_count = (-fpp.log(2.0) * (item_count as f64) / 2f64.ln()).ceil() as usize;
        BloomFilter {
            bit_vec: BitVec::new(bit_count),
            hasher_count: Self::get_hasher_count(bit_count, item_count),
            hashers: Self::get_hashers(),
            _marker: PhantomData,
        }
    }

    fn get_hashes(&self, item: &T) -> [u64; 2] {
        let mut ret = [0; 2];
        for index in 0..2 {
            let sip = &mut self.hashers[index].clone();
            item.hash(sip);
            ret[index] = sip.finish();
        }
        ret
    }

    /// Inserts an element into the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let mut filter = BloomFilter::new(100, 10);
    ///
    /// filter.insert(&"foo");
    /// ```
    pub fn insert(&mut self, item: &T) {
        let hashes = self.get_hashes(item);
        for index in 0..self.hasher_count {
            let mut offset = (index as u64).wrapping_mul(hashes[1]) % 0xffffffffffffffc5;
            offset = hashes[0].wrapping_add(offset);
            offset = offset % self.bit_vec.len() as u64;
            self.bit_vec.set(offset as usize, true);
        }
    }

    /// Checks if an element is possibly in the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let mut filter = BloomFilter::new(100, 10);
    ///
    /// assert!(!filter.contains(&"foo"));
    /// filter.insert(&"foo");
    /// assert!(filter.contains(&"foo"));
    /// ```
    pub fn contains(&mut self, item: &T) -> bool {
        let hashes = self.get_hashes(item);
        (0..self.hasher_count).all(|index| {
            let mut offset = (index as u64).wrapping_mul(hashes[1]) % 0xffffffffffffffc5;
            offset = hashes[0].wrapping_add(offset);
            offset = offset % self.bit_vec.len() as u64;
            self.bit_vec[offset as usize]
        })
    }

    /// Returns the number of bits in the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let filter: BloomFilter<u32> = BloomFilter::new(100, 10);
    ///
    /// assert_eq!(filter.len(), 100);
    /// ```
    pub fn len(&self) -> usize {
        self.bit_vec.len()
    }

    /// Returns the number of hash functions used by the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let filter: BloomFilter<u32> = BloomFilter::new(100, 10);
    ///
    /// assert_eq!(filter.hasher_count(), 7);
    /// ```
    pub fn hasher_count(&self) -> usize {
        self.hasher_count
    }

    /// Clears the bloom filter, removing all elements.
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let mut filter = BloomFilter::new(100, 10);
    ///
    /// filter.insert(&"foo");
    /// filter.clear();
    ///
    /// assert!(!filter.contains(&"foo"));
    pub fn clear(&mut self) {
        self.bit_vec.set_all(false)
    }
}

#[cfg(test)]
mod tests {
    use super::BloomFilter;

    #[test]
    fn test_bloom_filter() {
        let mut filter = BloomFilter::new(100, 10);

        assert!(!filter.contains(&"foo"));
        filter.insert(&"foo");
        assert!(filter.contains(&"foo"));

        filter.clear();
        assert!(!filter.contains(&"foo"));

        assert_eq!(filter.len(), 100);
        assert_eq!(filter.hasher_count(), 7);
    }
}
