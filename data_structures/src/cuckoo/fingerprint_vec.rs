use std::cmp;

#[derive(Clone)]
pub struct FingerprintVec {
    blocks: Vec<u8>,
    fingerprint_bit_count: usize,
    occupied_len: usize,
    len: usize,
}

impl FingerprintVec {
    pub fn new(fingerprint_bit_count: usize, len: usize) -> Self {
        let bits = fingerprint_bit_count * len;
        FingerprintVec {
            blocks: vec![0; (bits + 7) / 8],
            fingerprint_bit_count,
            occupied_len: 0,
            len,
        }
    }

    pub fn set(&mut self, index: usize, bytes: &[u8]) {
        let prev_is_zero = self.get(index).iter().all(|byte| *byte == 0);
        let mut bits_left = self.fingerprint_bit_count;
        let mut bits_offset = index * self.fingerprint_bit_count;
        let mut byte_offset = 0;

        while bits_left > 0 {
            let curr_bits = cmp::min(bits_left, 8 - bits_offset % 8);
            bits_left -= curr_bits;

            let mut new_bits = {
                if byte_offset % 8 == 0 {
                    bytes[byte_offset / 8]
                } else if (8 - byte_offset % 8) >= bits_left + curr_bits {
                    (bytes[byte_offset / 8]) >> (byte_offset % 8)
                } else {
                    (bytes[byte_offset / 8] >> (byte_offset % 8)) | (bytes[byte_offset / 8 + 1] << (8 - byte_offset % 8))
                }
            };

            new_bits = new_bits << (8 - curr_bits) >> (8 - curr_bits);

            self.blocks[bits_offset / 8] &= !(!0 >> (8 - curr_bits) << (bits_offset % 8));
            self.blocks[bits_offset / 8] |= new_bits << (bits_offset % 8);

            byte_offset += curr_bits;
            bits_offset += curr_bits;
        }
        let curr_is_zero = self.get(index).iter().all(|byte| *byte == 0);
        if prev_is_zero != curr_is_zero {
            if curr_is_zero {
                self.occupied_len -= 1;
            } else {
                self.occupied_len += 1;
            }
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
                    self.blocks[bits_offset / 8] >> (bits_offset % 8)
                } else {
                    (self.blocks[bits_offset / 8] >> (bits_offset % 8)) | (self.blocks[bits_offset / 8 + 1] << (8 - bits_offset % 8))
                }
            };

            ret[ret_offset / 8] = old_bits & (!0u8 >> (8 - curr_bits));

            bits_offset += curr_bits;
            ret_offset += curr_bits;
        }
        ret
    }

    pub fn clear(&mut self) {
        self.occupied_len = 0;
        for byte in &mut self.blocks {
            *byte = 0;
        }
    }

    pub fn capacity(&self) -> usize {
        self.len
    }

    pub fn occupied_len(&self) -> usize {
        self.occupied_len
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

        fpv.set(0, &[0]);
        assert_eq!(fpv.occupied_len(), 0);

        for i in 0..8 {
            fpv.set(i, &[((i + 1) as u8)]);
            assert_eq!(fpv.occupied_len(), i + 1);
        }

        fpv.set(0, &[1]);
        assert_eq!(fpv.occupied_len(), 8);

        for i in 0..8 {
            assert_eq!(fpv.get(i), vec![((i + 1) as u8)]);
            fpv.set(i, &[0]);
            assert_eq!(fpv.occupied_len(), 8 - i - 1);
        }
    }

    #[test]
    fn test_bit_count_13() {
        let mut fpv = FingerprintVec::new(13, 8);

        fpv.set(0, &[0, 0]);
        assert_eq!(fpv.occupied_len(), 0);

        for i in 0..8 {
            fpv.set(i, &[((i + 1) as u8), !0]);
            assert_eq!(fpv.occupied_len(), i + 1);
        }

        fpv.set(0, &[1, !0]);
        assert_eq!(fpv.occupied_len(), 8);

        for i in 0..8 {
            assert_eq!(fpv.get(i), vec![((i + 1) as u8), 0b11111]);
            fpv.set(i, &[0, 0]);
            assert_eq!(fpv.occupied_len(), 8 - i - 1);
        }
    }

    #[test]
    fn test_bit_count_21() {
        let mut fpv = FingerprintVec::new(21, 8);

        fpv.set(0, &[0, 0, 0]);
        assert_eq!(fpv.occupied_len(), 0);

        for i in 0..8 {
            fpv.set(i, &[((i + 1) as u8), !0, !0]);
            assert_eq!(fpv.occupied_len(), i + 1);
        }

        fpv.set(0, &[1, !0, !0]);
        assert_eq!(fpv.occupied_len(), 8);

        for i in 0..8 {
            assert_eq!(fpv.get(i), vec![((i + 1) as u8), !0, 0b11111]);
            fpv.set(i, &[0, 0, 0]);
            assert_eq!(fpv.occupied_len(), 8 - i - 1);
        }
    }

    #[test]
    fn test_len() {
        let fpv = FingerprintVec::new(8, 10);
        assert_eq!(fpv.capacity(), 10);
    }

    #[test]
    fn test_fingerprint_bit_count() {
        let fpv = FingerprintVec::new(8, 10);
        assert_eq!(fpv.fingerprint_bit_count(), 8);
    }
}
