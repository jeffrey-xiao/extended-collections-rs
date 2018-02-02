use rand;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Result};

use ::KEY_LENGTH;

#[derive(Ord, PartialOrd, PartialEq, Eq, Clone, Hash, Serialize, Deserialize, Default, Copy)]
pub struct Key(pub [u8; KEY_LENGTH]);

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let hex_vec: Vec<String> = self.0.iter().map(|b| format!("{:02X}", b)).collect();
        write!(f, "{}", hex_vec.join(""))
    }
}

impl Key {
    pub fn new(data: [u8; KEY_LENGTH]) -> Self { Key(data) }

    pub fn rand() -> Self {
        let mut ret = [0; KEY_LENGTH];
        for byte in &mut ret {
            *byte = rand::random::<u8>();
        }
        Key(ret)
    }

    pub fn xor(&self, key: &Key) -> Key {
        let mut ret = [0; KEY_LENGTH];
        for (i, byte) in ret.iter_mut().enumerate() {
            *byte = self.0[i] ^ key.0[i];
        }
        Key(ret)
    }

    pub fn get_distance(&self) -> usize {
        let mut ret = 0;
        for i in 0..KEY_LENGTH {
            for j in 0..8 {
                if (self.0[i] >> (7 - j)) & 1 != 0 {
                    return ret + j;
                }
            }
            ret += 8;
        }
        ret
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
pub struct NodeData {
    pub addr: String,
    pub id: Key,
}

#[derive(Eq, Clone, Debug)]
pub struct NodeDataDistancePair(pub NodeData, pub Key);

impl PartialEq for NodeDataDistancePair {
    fn eq(&self, other: &NodeDataDistancePair) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd for NodeDataDistancePair {
    fn partial_cmp(&self, other: &NodeDataDistancePair) -> Option<Ordering> {
        Some(other.1.cmp(&self.1))
    }
}

impl Ord for NodeDataDistancePair {
    fn cmp(&self, other: &NodeDataDistancePair) -> Ordering {
        other.1.cmp(&self.1)
    }
}

#[cfg(test)]
mod tests {
}
