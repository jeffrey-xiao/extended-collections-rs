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
/// use extended_collections::bloom::PartitionedBloomFilter;
///
/// let mut filter = PartitionedBloomFilter::from_item_count(10, 0.01);
///
/// assert!(!filter.contains(&"foo"));
/// filter.insert(&"foo");
/// assert!(filter.contains(&"foo"));
///
/// filter.clear();
/// assert!(!filter.contains(&"foo"));
///
/// assert_eq!(filter.len(), 98);
/// assert_eq!(filter.bit_count(), 14);
/// assert_eq!(filter.hasher_count(), 7);
/// ```
pub struct PartitionedBloomFilter<T>
where T: Hash
{
    bit_vec: BitVec,
    hashers: [SipHasher; 2],
    bit_count: usize,
    hasher_count: usize,
    _marker: PhantomData<T>,
}

impl<T> PartitionedBloomFilter<T>
where T: Hash
{
    fn get_hashers() -> [SipHasher; 2] {
        let mut rng = XorShiftRng::new_unseeded();
        [
            SipHasher::new_with_keys(rng.next_u64(), rng.next_u64()),
            SipHasher::new_with_keys(rng.next_u64(), rng.next_u64()),
        ]
    }

    fn get_hasher_count(fpp: f64) -> usize {
        (1.0 / fpp).log2().ceil() as usize
    }

    /// Constructs a new, empty `PartitionedBloomFilter<T>` with an estimated max capacity of
    /// `item_count` items and a maximum false positive probability of `fpp`.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let filter: PartitionedBloomFilter<u32> = PartitionedBloomFilter::from_item_count(10, 0.01);
    /// ```
    pub fn from_item_count(item_count: usize, fpp: f64) -> Self {
        let hasher_count = Self::get_hasher_count(fpp);
        let bit_count = (item_count as f64 * fpp.ln() / -2f64.ln().powi(2) / (hasher_count as f64)).ceil() as usize;
        PartitionedBloomFilter {
            bit_vec: BitVec::new(bit_count * hasher_count),
            hashers: Self::get_hashers(),
            bit_count,
            hasher_count,
            _marker: PhantomData,
        }
    }

    /// Constructs a new, empty `PartitionedBloomFilter<T>` with `bit_count` bits per partition and
    /// a maximum false positive probability of `fpp`.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let filter: PartitionedBloomFilter<u32> = PartitionedBloomFilter::from_bit_count(10, 0.01);
    /// ```
    pub fn from_bit_count(bit_count: usize, fpp: f64) -> Self {
        let hasher_count = Self::get_hasher_count(fpp);
        PartitionedBloomFilter {
            bit_vec: BitVec::new(bit_count * hasher_count),
            hashers: Self::get_hashers(),
            bit_count,
            hasher_count,
            _marker: PhantomData,
        }
    }

    fn get_hashes(&self, item: &T) -> [u64; 2] {
        let mut ret = [0; 2];
        for (index, hash) in ret.iter_mut().enumerate() {
            let mut sip = self.hashers[index];
            item.hash(&mut sip);
            *hash = sip.finish();
        }
        ret
    }

    /// Inserts an element into the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::from_item_count(10, 0.01);
    ///
    /// filter.insert(&"foo");
    /// ```
    pub fn insert(&mut self, item: &T) {
        let hashes = self.get_hashes(item);
        for index in 0..self.hasher_count {
            let mut offset = (index as u64).wrapping_mul(hashes[1]) % 0xFFFF_FFFF_FFFF_FFC5;
            offset = hashes[0].wrapping_add(offset);
            offset %= self.bit_count as u64;
            offset += (index * self.bit_count) as u64;
            self.bit_vec.set(offset as usize, true);
        }
    }

    /// Checks if an element is possibly in the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::from_item_count(10, 0.01);
    ///
    /// assert!(!filter.contains(&"foo"));
    /// filter.insert(&"foo");
    /// assert!(filter.contains(&"foo"));
    /// ```
    pub fn contains(&self, item: &T) -> bool {
        let hashes = self.get_hashes(item);
        (0..self.hasher_count).all(|index| {
            let mut offset = (index as u64).wrapping_mul(hashes[1]) % 0xFFFF_FFFF_FFFF_FFC5;
            offset = hashes[0].wrapping_add(offset);
            offset %= self.bit_count as u64;
            offset += (index * self.bit_count) as u64;
            self.bit_vec[offset as usize]
        })
    }

    /// Returns the number of bits in the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let filter: PartitionedBloomFilter<u32> = PartitionedBloomFilter::from_item_count(10, 0.01);
    ///
    /// assert_eq!(filter.len(), 98);
    /// ```
    pub fn len(&self) -> usize {
        self.bit_vec.len()
    }

    /// Returns `true` if the bloom filter is empty.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let filter: PartitionedBloomFilter<u32> = PartitionedBloomFilter::from_item_count(10, 0.01);
    ///
    /// assert!(!filter.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of bits in each partition in the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let filter: PartitionedBloomFilter<u32> = PartitionedBloomFilter::from_item_count(10, 0.01);
    ///
    /// assert_eq!(filter.bit_count(), 14);
    /// ```
    pub fn bit_count(&self) -> usize {
        self.bit_count
    }

    /// Returns the number of hash functions used by the bloom filter.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let filter: PartitionedBloomFilter<u32> = PartitionedBloomFilter::from_item_count(10, 0.01);
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
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::from_item_count(10, 0.01);
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
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::from_item_count(10, 0.01);
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
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::from_item_count(10, 0.01);
    /// filter.insert(&"foo");
    ///
    /// assert_eq!(filter.count_zeros(), 91);
    /// ```
    pub fn count_zeros(&self) -> usize {
        self.bit_vec.count_zeros()
    }

    /// Returns the estimated false positive probability of the bloom filter. This value will
    /// increase as more items are added.
    ///
    /// This is a fairly rough estimate as it takes the overall fill ratio of all
    /// partitions instead of considering each partition individually.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::from_item_count(100, 0.01);
    /// assert!(filter.estimate_fpp() < 1e-6);
    ///
    /// filter.insert(&0);
    /// assert!(filter.estimate_fpp() < 0.01);
    /// ```
    pub fn estimate_fpp(&self) -> f64 {
        let single_fpp = self.bit_vec.count_ones() as f64 / self.bit_vec.len() as f64;
        single_fpp.powi(self.hasher_count as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::PartitionedBloomFilter;

    #[test]
    fn test_from_item_count() {
        let mut filter = PartitionedBloomFilter::from_item_count(10, 0.01);

        assert!(!filter.contains(&"foo"));
        filter.insert(&"foo");
        assert!(filter.contains(&"foo"));
        assert_eq!(filter.count_ones(), 7);
        assert_eq!(filter.count_zeros(), 91);

        filter.clear();
        assert!(!filter.contains(&"foo"));

        assert_eq!(filter.len(), 98);
        assert_eq!(filter.bit_count(), 14);
        assert_eq!(filter.hasher_count(), 7);
    }

    #[test]
    fn test_from_bit_count() {
        let mut filter = PartitionedBloomFilter::from_bit_count(10, 0.01);

        assert!(!filter.contains(&"foo"));
        filter.insert(&"foo");
        assert!(filter.contains(&"foo"));
        assert_eq!(filter.count_ones(), 7);
        assert_eq!(filter.count_zeros(), 63);

        filter.clear();
        assert!(!filter.contains(&"foo"));

        assert_eq!(filter.len(), 70);
        assert_eq!(filter.bit_count(), 10);
        assert_eq!(filter.hasher_count(), 7);
    }

    #[test]
    fn test_estimate_fpp() {
        let mut filter = PartitionedBloomFilter::from_item_count(100, 0.01);
        assert!(filter.estimate_fpp() < 1e-6);

        filter.insert(&0);

        let expected_fpp = (7f64 / 959f64).powi(7);
        assert!((filter.estimate_fpp() - expected_fpp).abs() < 1e-15);
    }
}
