use bit_vec::BitVec;
use rand::{Rng, XorShiftRng};
use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

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

    pub fn new(bit_count: usize, item_count: usize) -> Self {
        BloomFilter {
            bit_vec: BitVec::new(bit_count),
            hasher_count: Self::get_hasher_count(bit_count, item_count),
            hashers: Self::get_hashers(),
            _marker: PhantomData,
        }
    }

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

    pub fn insert(&mut self, item: &T) {
        let hashes = self.get_hashes(item);
        for index in 0..self.hasher_count {
            let mut offset = (index as u64).wrapping_mul(hashes[1]) % 0xffffffffffffffc5;
            offset = hashes[0].wrapping_add(offset);
            offset = offset % self.bit_vec.len() as u64;
            self.bit_vec.set(offset as usize, true);
        }
    }

    pub fn contains(&mut self, item: &T) -> bool {
        let hashes = self.get_hashes(item);
        for index in 0..self.hasher_count {
            let mut offset = (index as u64).wrapping_mul(hashes[1]) % 0xffffffffffffffc5;
            offset = hashes[0].wrapping_add(offset);
            offset = offset % self.bit_vec.len() as u64;
            if !self.bit_vec[offset as usize] {
                return false;
            }
        }
        true
    }

    pub fn len(&self) -> usize {
        self.bit_vec.len()
    }

    pub fn clear(&mut self) {
        self.bit_vec.set_all(false)
    }

    pub fn hasher_count(&self) -> usize {
        self.hasher_count
    }
}
