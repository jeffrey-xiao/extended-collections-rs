use bit_vec::BitVec;
use rand::{Rng, XorShiftRng};
use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

/// A space-efficient probabilistic data structure to test for membership in a set.
///
/// This particular implementation of a bloom filter uses `K` partitions and `K` hash functions.
/// Each hash function maps to a bit in its respective partition. A partitioned bloom filter is
/// more robust than its traditional counterpart, but the memory usage is varies based on how many
/// hash functions you are using.
///
/// # Examples
/// ```
/// use data_structures::bloom::BloomFilter;
///
/// let mut filter = BloomFilter::new(10, 0.01);
///
/// assert!(!filter.contains(&"foo"));
/// filter.insert(&"foo");
/// assert!(filter.contains(&"foo"));
///
/// filter.clear();
/// assert!(!filter.contains(&"foo"));
///
/// assert_eq!(filter.len(), 96);
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

    fn get_hasher_count(fpp: f64) -> usize {
        (1.0 / fpp).log(2)
    }

    /// Constructs a new, empty `BloomFilter<T>` with an estimated max capacity of `item_count`
    /// items and a maximum false positive probability of `fpp`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let filter: BloomFilter<u32> = BloomFilter::new(10, 0.01);
    /// ```
    pub fn new(item_count: usize, fpp: f64) -> Self {
        let hasher_count = get_hasher_count(fpp);
        let bit_count = (-item_count * fpp.ln() / 2f64.log(2).powi(2) / (hasher_count as f64)).ceil() as usize;
        BloomFilter {
            bit_vec: BitVec::new(bit_count * hasher_count),
            hasher_count,
            hashers: Self::get_hashers(),
            _marker: PhantomData,
        }
    }

    /// Constructs a new, empty `BloomFilter<T>` with `bit_count` bits and an estimated max
    /// capacity of `item_count` items.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let filter: BloomFilter<u32> = BloomFilter::from_item_count(100, 10);
    /// ```
    pub fn from_item_count(bit_count: usize, item_count: usize) -> Self {
        let hasher_count = get_hasher_count
        BloomFilter {
            bit_vec: BitVec::new(bit_count),
            hasher_count: Self::get_hasher_count(bit_count, item_count),
            hashers: Self::get_hashers(),
            _marker: PhantomData,
        }
    }

    /// Constructs a new, empty `BloomFilter<T>` with `bit_count` bits and a maximum false positive
    /// probability of `fpp`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let filter: BloomFilter<u32> = BloomFilter::from_fpp(100, 0.01);
    /// ```
    pub fn from_fpp(bit_count: usize, fpp: f64) -> Self {
        let item_count = (-2f64.ln() * (bit_count as f64) / fpp.log(2.0)).floor() as usize;
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
    /// let mut filter = BloomFilter::new(10, 0.01);
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
    /// let mut filter = BloomFilter::new(10, 0.01);
    ///
    /// assert!(!filter.contains(&"foo"));
    /// filter.insert(&"foo");
    /// assert!(filter.contains(&"foo"));
    /// ```
    pub fn contains(&self, item: &T) -> bool {
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
    /// let filter: BloomFilter<u32> = BloomFilter::from_fpp(100, 0.01);
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
    /// let filter: BloomFilter<u32> = BloomFilter::new(10, 0.01);
    ///
    /// assert_eq!(filter.hasher_count(), 7);
    /// ```
    pub fn hasher_count(&self) -> usize {
        self.hasher_count
    }

    /// Clears the bloom filter, removing all elements.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let mut filter = BloomFilter::new(10, 0.01);
    ///
    /// filter.insert(&"foo");
    /// filter.clear();
    ///
    /// assert!(!filter.contains(&"foo"));
    /// ```
    pub fn clear(&mut self) {
        self.bit_vec.set_all(false)
    }

    /// Returns the number of set bits in the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let mut filter = BloomFilter::from_fpp(100, 0.01);
    /// filter.insert(&"foo");
    ///
    /// assert_eq!(filter.count_ones(), 7);
    /// ```
    pub fn count_ones(&self) -> usize {
        self.bit_vec.count_ones()
    }

    /// Returns the number of unset bits in the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::BloomFilter;
    ///
    /// let mut filter = BloomFilter::from_fpp(100, 0.01);
    /// filter.insert(&"foo");
    ///
    /// assert_eq!(filter.count_zeros(), 93);
    /// ```
    pub fn count_zeros(&self) -> usize {
        self.bit_vec.count_zeros()
    }
}

#[cfg(test)]
mod tests {
    use super::BloomFilter;

    #[test]
    fn test_new() {
        let mut filter = BloomFilter::new(10, 0.01);

        assert!(!filter.contains(&"foo"));
        filter.insert(&"foo");
        assert!(filter.contains(&"foo"));
        assert_eq!(filter.count_ones(), 7);
        assert_eq!(filter.count_zeros(), 89);

        filter.clear();
        assert!(!filter.contains(&"foo"));

        assert_eq!(filter.len(), 96);
        assert_eq!(filter.hasher_count(), 7);
    }

    #[test]
    fn test_from_fpp() {
        let mut filter = BloomFilter::from_fpp(100, 0.01);

        assert!(!filter.contains(&"foo"));
        filter.insert(&"foo");
        assert!(filter.contains(&"foo"));
        assert_eq!(filter.count_ones(), 7);
        assert_eq!(filter.count_zeros(), 93);

        filter.clear();
        assert!(!filter.contains(&"foo"));

        assert_eq!(filter.len(), 100);
        assert_eq!(filter.hasher_count(), 7);
    }

    #[test]
    fn test_from_item_count() {
        let mut filter = BloomFilter::from_item_count(100, 10);

        assert!(!filter.contains(&"foo"));
        filter.insert(&"foo");
        assert!(filter.contains(&"foo"));
        assert_eq!(filter.count_ones(), 7);
        assert_eq!(filter.count_zeros(), 93);

        filter.clear();
        assert!(!filter.contains(&"foo"));

        assert_eq!(filter.len(), 100);
        assert_eq!(filter.hasher_count(), 7);
    }
}
