use rand;
use kademlia::KEY_LENGTH;

#[derive(PartialEq, Clone, Hash, Serialize, Deserialize, Debug)]
pub struct Key([u8; KEY_LENGTH]);

impl Eq for Key {}

impl Key {
    pub fn new() -> Self {
        let mut ret = [0; KEY_LENGTH];
        for i in 0..KEY_LENGTH {
            ret[i] = rand::random::<u8>();
        }
        Key(ret)
    }

    pub fn xor(&self, key: &Key) -> Key {
        let mut ret = [0; KEY_LENGTH];
        for i in 0..KEY_LENGTH {
            ret[i] = self.0[i] ^ key.0[i];
        }
        Key(ret)
    }

    pub fn get_distance(&self) -> usize {
        let mut ret = 0;
        for i in 0..KEY_LENGTH {
            if self.0[i] == 0 {
                ret += 8;
            } else {
                if self.0[i] & 0xF0 == 0 { ret += 4 }
                if self.0[i] & 0xC0 == 0 { ret += 2 }
                if self.0[i] & 0x80 == 0 { ret += 1 }
            }
        }
        ret
    }
}

#[derive(PartialEq, Clone, Serialize, Deserialize, Debug)]
pub struct NodeData {
    pub addr: String,
    pub id: Key,
}
