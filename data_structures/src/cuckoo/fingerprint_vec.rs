use std::cmp;

#[derive(Clone)]
pub struct FingerprintVec {
    blocks: Vec<u8>,
    fingerprint_bit_count: usize,
    len: usize,
}

impl FingerprintVec {
    pub fn new(fingerprint_bit_count: usize, len: usize) -> Self {
        let bits = fingerprint_bit_count * len;
        FingerprintVec {
            blocks: vec![0; (bits + 7) / 8],
            fingerprint_bit_count,
            len,
        }
    }

    pub fn set(&mut self, index: usize, bytes: Vec<u8>) {
        let mut bits_left = self.fingerprint_bit_count;
        let mut bits_offset = index * self.fingerprint_bit_count;
        let mut byte_offset = 0;

        while bits_left > 0 {
            let curr_bits = cmp::min(bits_left, 8 - bits_offset % 8);
            bits_left -= curr_bits;

            let new_bits = {
                if byte_offset % 8 == 0 {
                    bytes[byte_offset / 8]
                } else if (8 - byte_offset % 8) >= bits_left + curr_bits {
                    (bytes[byte_offset / 8]) >> byte_offset % 8
                } else {
                    (bytes[byte_offset / 8] >> byte_offset % 8) | (bytes[byte_offset / 8 + 1] << (8 - byte_offset % 8))
                }
            };

            self.blocks[bits_offset / 8] &= !(!0 >> (8 - curr_bits) << (bits_offset % 8));
            self.blocks[bits_offset / 8] |= new_bits << (bits_offset % 8);

            byte_offset += curr_bits;
            bits_offset += curr_bits;
        }
    }

    pub fn get(&self, index: usize) -> Vec<u8> {
        let mut ret = vec![0; (self.fingerprint_bit_count + 7) / 8];
        let mut bits_left = self.fingerprint_bit_count;
        let mut bits_offset = index * self.fingerprint_bit_count;
        let mut ret_offset = 0;

        while bits_left > 0 {
            let curr_bits = cmp::min(bits_left, 8);
            bits_left -= curr_bits;

            let old_bits = {
                if bits_offset % 8 == 0 {
                    self.blocks[bits_offset / 8]
                } else if 8 - bits_offset % 8 >= curr_bits {
                    self.blocks[bits_offset / 8] >> bits_offset % 8
                } else {
                    (self.blocks[bits_offset / 8] >> bits_offset % 8) | (self.blocks[bits_offset / 8 + 1] << (8 - bits_offset % 8))
                }
            };

            ret[ret_offset / 8] = old_bits & (!0u8 >> (8 - curr_bits));

            bits_offset += curr_bits;
            ret_offset += curr_bits;
        }
        ret
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn fingerprint_bit_count(&self) -> usize {
        self.fingerprint_bit_count
    }
}

#[cfg(test)]
mod tests {
    use super::FingerprintVec;

    #[test]
    fn test_bit_count_8() {
        let mut fpv = FingerprintVec::new(5, 8);
        for i in 0..8 {
            fpv.set(i, vec![(i as u8)]);
        }

        for i in 0..8 {
            assert_eq!(fpv.get(i), vec![(i as u8)]);
        }
    }

    #[test]
    fn test_bit_count_13() {
        let mut fpv = FingerprintVec::new(13, 8);
        for i in 0..8 {
            fpv.set(i, vec![(i as u8), !0]);
        }

        for i in 0..8 {
            assert_eq!(fpv.get(i), vec![(i as u8), 0b11111]);
        }
    }

    #[test]
    fn test_bit_count_21() {
        let mut fpv = FingerprintVec::new(21, 8);
        for i in 0..8 {
            fpv.set(i, vec![(i as u8), !0, !0]);
        }

        for i in 0..8 {
            assert_eq!(fpv.get(i), vec![(i as u8), !0, 0b11111]);
        }
    }

    #[test]
    fn test_len() {
        let fpv = FingerprintVec::new(8, 10);
        assert_eq!(fpv.len(), 10);
    }

    #[test]
    fn test_fingerprint_bit_count() {
        let fpv = FingerprintVec::new(8, 10);
        assert_eq!(fpv.fingerprint_bit_count(), 8);
    }
}
