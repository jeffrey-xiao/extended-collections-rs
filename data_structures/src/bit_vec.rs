use std::mem;
use std::ops::Range;
use std::slice;

/// A growable list of bits implemented using a `Vec<u8>`
///
/// # Examples
///
/// ```
/// use data_structures::bit_vec::BitVec;
///
/// let mut bv = BitVec::from_elem(5, false);
///
/// bv.set(0, true);
/// bv.set(1, true);
/// bv.set(2, true);
/// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![true, true, true, false, false]);
///
/// bv.set_all(true);
/// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![true, true, true, true, true]);
///
/// bv.flip(0);
/// bv.flip_all();
/// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![true, false, false, false, false]);
///
/// bv.push(true);
/// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![true, false, false, false, false, true]);
/// assert_eq!(bv.pop(), Some(true));
///
/// let clone = bv.clone();
/// bv.flip_all();
/// bv.union(&clone);
/// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![true, true, true, true, true]);
/// ```
pub struct BitVec {
    blocks: Vec<u8>,
    len: usize,
}

impl BitVec {
    #[inline]
    fn get_block_bit_count() -> usize {
        mem::size_of::<u8>()
    }

    fn get_block_count(len: usize) -> usize {
        let block_bit_count = Self::get_block_bit_count();
        (len + block_bit_count - 1) / block_bit_count
    }

    fn clear_extra_bits(&mut self) {
        let extra_bits = self.len() & Self::get_block_bit_count();
        if extra_bits > 0 {
            let mask = (1 << extra_bits) - 1;
            let blocks_len = self.blocks.len();
            let block = &mut self.blocks[blocks_len - 1];
            *block &= mask;
        }
    }

    /// Constructs a new `BitVec` with a certain number of bits. All bits are initialized to false.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let bv = BitVec::new(5);
    /// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![false, false, false, false, false]);
    /// ```
    pub fn new(len: usize) -> Self {
        Self {
            blocks: vec![0; Self::get_block_count(len)],
            len,
        }
    }

    /// Constructs a new `BitVec` with a certain number of bits. All bits are initialized to `bit`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let bv = BitVec::from_elem(5, true);
    /// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![true, true, true, true, true]);
    /// ```
    pub fn from_elem(len: usize, bit: bool) -> Self {
        let mut ret = BitVec {
            blocks: vec![if bit { 1 } else { 0 }; Self::get_block_count(len)],
            len,
        };
        ret.clear_extra_bits();
        ret
    }

    /// Constructs a new, empty `BitVec` with a certain capacity.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let bv = BitVec::with_capacity(5);
    /// ```
    pub fn with_capacity(len: usize) -> Self {
        BitVec {
            blocks: Vec::with_capacity(Self::get_block_count(len)),
            len,
        }
    }

    /// Returns the value at index `index`, or `None` if index is out of bounds.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::new(5);
    /// bv.set(1, true);
    ///
    /// assert_eq!(bv.get(0), Some(false));
    /// assert_eq!(bv.get(1), Some(true));
    /// ```
    pub fn get(&self, index: usize) -> Option<bool> {
        if index >= self.len {
            None
        } else {
            let block_index = index / Self::get_block_bit_count();
            let bit_index = index % Self::get_block_bit_count();
            self.blocks.get(block_index).map(|block| {
                ((block >> bit_index) & 1) != 0
            })
        }
    }

    /// Sets the value at index `index` to `bit`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::new(5);
    /// bv.set(1, true);
    ///
    /// assert_eq!(bv.get(0), Some(false));
    /// assert_eq!(bv.get(1), Some(true));
    /// ```
    pub fn set(&mut self, index: usize, bit: bool) {
        assert!(index < self.len);
        let block_index = index / Self::get_block_bit_count();
        let bit_index = index % Self::get_block_bit_count();
        let mask = 1 << bit_index;
        if bit {
            self.blocks[block_index] |= mask;
        } else {
            self.blocks[block_index] &= !mask;
        }
    }

    /// Sets all values in the `BitVec` to `bit`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(5, false);
    /// bv.set_all(true);
    ///
    /// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![true, true, true, true, true]);
    /// ```
    pub fn set_all(&mut self, bit: bool)  {
        let mask = { if bit { !0 } else { 0 } };
        for block in &mut self.blocks {
            *block = mask;
        }
        self.clear_extra_bits();
    }

    /// Flip the value at index `index`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(5, false);
    ///
    /// bv.flip(0);
    /// assert_eq!(bv.get(0), Some(true));
    ///
    /// bv.flip(1);
    /// assert_eq!(bv.get(0), Some(true));
    /// ```
    pub fn flip(&mut self, index: usize) {
        assert!(index < self.len);
        let block_index = index / Self::get_block_bit_count();
        let bit_index = index % Self::get_block_bit_count();
        let mask = 1 << bit_index;
        if (self.blocks[block_index] >> bit_index) & 1 == 0 {
            self.blocks[block_index] |= mask;
        } else {
            self.blocks[block_index] &= !mask;
        }
    }

    /// Flips all values in the `BitVec`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(5, false);
    ///
    /// bv.flip_all();
    /// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![true, true, true, true, true]);
    ///
    /// bv.flip_all();
    /// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![false, false, false, false, false]);
    /// ```
    pub fn flip_all(&mut self) {
        for block in &mut self.blocks {
            *block = !*block;
        }
    }

    fn apply<F: FnMut(u8, u8) -> u8>(&mut self, other: &BitVec, mut op: F) {
        assert_eq!(self.len(), other.len());
        assert_eq!(self.blocks.len(), other.blocks.len());
        for (x, y) in self.blocks_mut().zip(other.blocks()) {
            *x = op(*x, y);
        }
    }

    /// Sets `self` to the union of `self` and `other`.
    ///
    /// # Panics
    /// Panics if the two `BitVec` are of different lengths.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv1 = BitVec::new(4);
    /// bv1.set(0, true);
    /// bv1.set(1, true);
    ///
    /// let mut bv2 = BitVec::new(4);
    /// bv2.set(0, true);
    /// bv2.set(2, true);
    ///
    /// bv1.union(&bv2);
    /// assert_eq!(bv1.iter().collect::<Vec<bool>>(), vec![true, true, true, false]);
    /// ```
    pub fn union(&mut self, other: &Self) {
        self.apply(other, |x, y| x | y)
    }

    /// Sets `self` to the intersection of `self` and `other`.
    ///
    /// # Panics
    /// Panics if the two `BitVec` are of different lengths.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv1 = BitVec::new(4);
    /// bv1.set(0, true);
    /// bv1.set(1, true);
    ///
    /// let mut bv2 = BitVec::new(4);
    /// bv2.set(0, true);
    /// bv2.set(2, true);
    ///
    /// bv1.intersection(&bv2);
    /// assert_eq!(bv1.iter().collect::<Vec<bool>>(), vec![true, false, false, false]);
    /// ```
    pub fn intersection(&mut self, other: &Self) {
        self.apply(other, |x, y| x & y)
    }

    /// Sets `self` to the difference of `self` and `other`.
    ///
    /// # Panics
    /// Panics if the two `BitVec` are of different lengths.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv1 = BitVec::new(4);
    /// bv1.set(0, true);
    /// bv1.set(1, true);
    ///
    /// let mut bv2 = BitVec::new(4);
    /// bv2.set(0, true);
    /// bv2.set(2, true);
    ///
    /// bv1.difference(&bv2);
    /// assert_eq!(bv1.iter().collect::<Vec<bool>>(), vec![false, true, false, false]);
    /// ```
    pub fn difference(&mut self, other: &Self) {
        self.apply(other, |x, y| x & !y)
    }

    /// Sets `self` to the symmetric difference of `self` and `other`.
    ///
    /// # Panics
    /// Panics if the two `BitVec` are of different lengths.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv1 = BitVec::new(4);
    /// bv1.set(0, true);
    /// bv1.set(1, true);
    ///
    /// let mut bv2 = BitVec::new(4);
    /// bv2.set(0, true);
    /// bv2.set(2, true);
    ///
    /// bv1.symmetric_difference(&bv2);
    /// assert_eq!(bv1.iter().collect::<Vec<bool>>(), vec![false, true, true, false]);
    /// ```
    pub fn symmetric_difference(&mut self, other: &Self) {
        self.apply(other, |x, y| (x & !y) | (!x & y))
    }


    /// Truncates a `BitVec`, dropping excess elements.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(5, false);
    ///
    /// bv.truncate(2);
    /// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![false, false]);
    /// ```
    pub fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.len = len;
            self.blocks.truncate(Self::get_block_count(len));
            self.clear_extra_bits();
        }
    }

    /// Reserves capacity for at least `additional` more bits to be inserted in the given
    /// `BitVec`.
    ///
    /// # Examples
    ///
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(5, false);
    /// bv.reserve(10);
    /// assert_eq!(bv.len(), 5);
    /// assert!(bv.capacity() >= 15);
    /// ```
    pub fn reserve(&mut self, additional: usize) {
        let desired_cap = self.len + additional;
        let blocks_len = self.blocks.len();
        if desired_cap > self.capacity() {
            self.blocks.reserve(Self::get_block_count(desired_cap) - blocks_len);
        }
    }

    /// Reserves capacity for exactly `additional` more bits to be inserted in the given
    /// `BitVec`.
    ///
    /// # Examples
    ///
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(5, false);
    /// bv.reserve_exact(10);
    /// assert_eq!(bv.len(), 5);
    /// assert!(bv.capacity() == 15);
    /// ```
    pub fn reserve_exact(&mut self, additional: usize) {
        let desired_cap = self.len + additional;
        let blocks_len = self.blocks.len();
        if desired_cap > self.capacity() {
            self.blocks.reserve_exact(Self::get_block_count(desired_cap) - blocks_len);
        }
    }

    /// Returns and removes the last element of the `BitVec`. Returns `None` if the `BitVec` is
    /// empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(1, false);
    ///
    /// assert_eq!(bv.pop(), Some(false));
    /// assert_eq!(bv.pop(), None);
    /// ```
    pub fn pop(&mut self) -> Option<bool> {
        if self.is_empty() {
            None
        } else {
            let index = self.len - 1;
            let ret = self.get(index);
            self.set(index, false);
            self.len -= 1;
            if self.len % Self::get_block_bit_count() == 0 {
                self.blocks.pop();
            }
            ret
        }
    }

    /// Pushes an element into the `BitVec`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(1, false);
    ///
    /// bv.push(true);
    /// assert_eq!(bv.get(1), Some(true));
    /// ```
    pub fn push(&mut self, bit: bool) {
        if self.len % Self::get_block_bit_count() == 0 {
            self.blocks.push(0);
        }
        let index = self.len;
        self.len += 1;
        self.set(index, bit);
    }

    fn blocks(&self) -> Blocks {
        Blocks { iter: self.blocks.iter() }
    }

    fn blocks_mut(&mut self) -> BlocksMut {
        self.blocks.iter_mut()
    }

    /// Returns an iterator over the elements of the vector in order.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(1, false);
    ///
    /// bv.push(true);
    /// assert_eq!(bv.iter().collect::<Vec<bool>>(), vec![false, true]);
    /// ```
    pub fn iter(&self) -> Iter {
        Iter { bit_vec: self, range: 0..self.len }
    }

    /// Returns `true` if the `BitVec` is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(1, false);
    ///
    /// assert!(!bv.is_empty());
    /// bv.pop();
    /// assert!(bv.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of elements in the `BitVec`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::from_elem(1, false);
    ///
    /// assert_eq!(bv.len(), 1);
    /// bv.pop();
    /// assert_eq!(bv.len(), 0);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }


    /// Returns the capacity of the `BitVec`.
    ///
    /// # Examples
    /// ```
    /// use data_structures::bit_vec::BitVec;
    ///
    /// let mut bv = BitVec::new(0);
    ///
    /// bv.reserve_exact(10);
    /// assert_eq!(bv.capacity(), 10);
    /// ```
    pub fn capacity(&self) -> usize {
        self.blocks.capacity() * Self::get_block_bit_count()
    }
}

impl Clone for BitVec {
    fn clone(&self) -> Self {
        BitVec { blocks: self.blocks.clone(), len: self.len }
    }

    fn clone_from(&mut self, source: &Self) {
        self.len = source.len;
        self.blocks.clone_from(&source.blocks);
    }
}

pub struct Iter<'a> {
    bit_vec: &'a BitVec,
    range: Range<usize>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        self.range.next().map(|i| self.bit_vec.get(i).unwrap())
    }
}

impl<'a> IntoIterator for &'a BitVec {
    type Item = bool;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}


pub struct IntoIter {
    bit_vec: BitVec,
    range: Range<usize>,
}

impl Iterator for IntoIter {
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        self.range.next().map(|i| self.bit_vec.get(i).unwrap())
    }
}

impl IntoIterator for BitVec {
    type Item = bool;
    type IntoIter = IntoIter;

    fn into_iter(self) -> IntoIter {
        let len = self.len;
        IntoIter { bit_vec: self, range: 0..len }
    }
}

pub type BlocksMut<'a> = slice::IterMut<'a, u8>;

pub struct Blocks<'a> {
    iter: slice::Iter<'a, u8>,
}

impl<'a> Iterator for Blocks<'a> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        self.iter.next().cloned()
    }
}
