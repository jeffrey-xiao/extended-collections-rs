use bloom::BloomFilter;
use std::hash::Hash;

/// A space-efficient probabilistic data structure to test for membership in a set.
///
/// A scalable bloom filter uses multiple bloom filters to progressively grow as more items are
/// added to the scalable bloom filter. The optimal fill ratio of a bloom filter is 50%, so as
/// soon as the number of ones exceeds 50% of the total bits, then another bloom filter is added to
/// the scalable bloom filter. The new filter will have its size based on the growth ratio, and the
/// number of hash functions based on the tightening ratio. The overall false positive probability
/// percentage of the scalable bloom filter will be `initial_fpp * 1 / (1 - tightening_ratio)`.
///
/// # Examples
/// ```
/// use data_structures::bloom::ScalableBloomFilter;
///
/// let mut filter = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
///
/// assert!(!filter.contains(&"foo"));
/// filter.insert(&"foo");
/// assert!(filter.contains(&"foo"));
///
/// filter.clear();
/// assert!(!filter.contains(&"foo"));
///
/// assert_eq!(filter.len(), 100);
/// assert_eq!(filter.filter_count(), 1);
/// ```
pub struct ScalableBloomFilter<T: Hash> {
    filters: Vec<BloomFilter<T>>,
    approximate_bits_used: usize,
    initial_fpp: f64,
    growth_ratio: f64,
    tightening_ratio: f64,
}

impl<T: Hash> ScalableBloomFilter<T> {
    /// Constructs a new, empty `ScalableBloomFilter<T>` with initially `initial_bit_count` bits
    /// and a initial maximum false positive probability of `fpp`. Every time a new bloom filter is
    /// added, the size will be `growth_ratio` multiplied by the previous size, and the false
    /// positive probability will be `tightening_ratio` multipled by the previous false positive
    /// probability.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::ScalableBloomFilter;
    ///
    /// let filter: ScalableBloomFilter<u32> = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
    /// ```
    pub fn new(initial_bit_count: usize, fpp: f64, growth_ratio: f64, tightening_ratio: f64) -> Self {
        ScalableBloomFilter {
            filters: vec![BloomFilter::from_fpp(initial_bit_count, fpp)],
            approximate_bits_used: 0,
            initial_fpp: fpp,
            growth_ratio,
            tightening_ratio,
        }
    }

    fn try_grow(&mut self) {
        let mut new_filter = None;
        {
            let filter = match self.filters.last() {
                Some(filter) => filter,
                _ => unreachable!(),
            };

            if self.approximate_bits_used * 2 >= filter.len() {
                self.approximate_bits_used = filter.count_ones();
                if self.approximate_bits_used * 2 >= filter.len() {
                    let exponent = self.filters.len() as i32;
                    new_filter = Some(BloomFilter::from_fpp(
                        (filter.len() as f64* self.growth_ratio).ceil() as usize,
                        self.initial_fpp * self.tightening_ratio.powi(exponent),
                    ));
                    self.approximate_bits_used = 0;
                }
            }
        }

        if let Some(new_filter) = new_filter {
            self.filters.push(new_filter);
        }
    }

    /// Inserts an element into the scalable bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::ScalableBloomFilter;
    ///
    /// let mut filter = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
    ///
    /// filter.insert(&"foo");
    /// ```
    pub fn insert(&mut self, item: &T) {
        if !self.filters.iter().rev().skip(1).any(|ref mut filter| filter.contains(item)) {
            let filter = match self.filters.last_mut() {
                Some(filter) => filter,
                _ => unreachable!(),
            };

            filter.insert(item);
            self.approximate_bits_used += filter.hasher_count();
        }
        self.try_grow();
    }

    /// Checks if an element is possibly in the scalable bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::ScalableBloomFilter;
    ///
    /// let mut filter = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
    ///
    /// assert!(!filter.contains(&"foo"));
    /// filter.insert(&"foo");
    /// assert!(filter.contains(&"foo"));
    /// ```
    pub fn contains(&self, item: &T) -> bool {
        self.filters.iter().any(|ref mut filter| filter.contains(item))
    }

    /// Returns the number of bits in the scalable bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::ScalableBloomFilter;
    ///
    /// let filter: ScalableBloomFilter<u32> = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
    ///
    /// assert_eq!(filter.len(), 100);
    /// ```
    pub fn len(&self) -> usize {
        self.filters.iter().map(|filter| filter.len()).sum()
    }

    /// Returns the number of bloom filters used by the scalable bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::ScalableBloomFilter;
    ///
    /// let filter: ScalableBloomFilter<u32> = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
    ///
    /// assert_eq!(filter.filter_count(), 1);
    /// ```
    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }

    /// Clears the scalable bloom filter, removing all elements.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::ScalableBloomFilter;
    ///
    /// let mut filter = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
    ///
    /// filter.insert(&"foo");
    /// filter.clear();
    ///
    /// assert!(!filter.contains(&"foo"));
    /// ```
    pub fn clear(&mut self) {
        let initial_bit_count = match self.filters.first() {
            Some(filter) => filter.len(),
            _ => unreachable!(),
        };

        self.filters = vec![BloomFilter::from_fpp(initial_bit_count, self.initial_fpp)];
        self.approximate_bits_used = 0;
    }

    /// Returns the number of set bits in the scalable bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::ScalableBloomFilter;
    ///
    /// let mut filter = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
    /// filter.insert(&"foo");
    ///
    /// assert_eq!(filter.count_ones(), 7);
    /// ```
    pub fn count_ones(&self) -> usize {
        self.filters.iter().map(|filter| filter.count_ones()).sum()
    }

    /// Returns the number of unset bits in the scalable bloom filter.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bloom::ScalableBloomFilter;
    ///
    /// let mut filter = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
    /// filter.insert(&"foo");
    ///
    /// assert_eq!(filter.count_zeros(), 93);
    /// ```
    pub fn count_zeros(&self) -> usize {
        self.filters.iter().map(|filter| filter.count_zeros()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::ScalableBloomFilter;

    #[test]
    fn test_scalable_bloom_filter() {
        let mut filter = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);

        assert!(!filter.contains(&"foo"));
        filter.insert(&"foo");
        assert!(filter.contains(&"foo"));
        assert_eq!(filter.approximate_bits_used, 7);
        assert_eq!(filter.count_ones(), 7);
        assert_eq!(filter.count_zeros(), 93);

        filter.clear();
        assert!(!filter.contains(&"foo"));
        assert_eq!(filter.approximate_bits_used, 0);

        assert_eq!(filter.len(), 100);
        assert_eq!(filter.filter_count(), 1);
    }

    #[test]
    fn test_grow() {
        let mut filter  = ScalableBloomFilter::new(100, 0.01, 2.0, 0.5);
        
        for i in 0..15 {
            filter.insert(&i);
        }

        assert_eq!(filter.len(), 300);
        assert_eq!(filter.filter_count(), 2);
        assert_eq!(filter.filters[0].hasher_count(), 7);
        assert_eq!(filter.filters[1].hasher_count(), 8);
    }
}
