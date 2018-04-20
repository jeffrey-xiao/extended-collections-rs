extern crate extended_collections;
extern crate rand;

use self::rand::{thread_rng, Rng};
use extended_collections::radix::RadixMap;
use std::iter;
use std::vec::Vec;

#[test]
fn int_test_radixmap() {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut map = RadixMap::new();
    let mut expected = Vec::new();
    for _ in 0..100_000 {
        // generate a random length from [10, 99)
        let len = rng.gen_range(10, 99);
        let key = iter::repeat(())
            .map(|()| rng.gen::<u8>())
            .take(len)
            .collect::<Vec<u8>>();
        let val = rng.gen::<u32>();

        map.insert(key.as_slice(), val);
        expected.push((key, val));
    }

    expected.reverse();
    expected.sort_by(|l, r| l.0.cmp(&r.0));
    expected.dedup_by_key(|pair| pair.0.clone());

    assert_eq!(map.len(), expected.len());

    assert_eq!(map.min(), Some(expected[0].0.clone()));
    assert_eq!(map.max(), Some(expected[expected.len() - 1].0.clone()));

    for entry in &expected {
        assert!(map.contains_key(&entry.0));
        assert_eq!(map.get(&entry.0), Some(&entry.1));
    }

    for entry in &mut expected {
        let val_1 = rng.gen::<u32>();
        let val_2 = rng.gen::<u32>();

        let old_entry = map.insert(entry.0.as_slice(), val_1);
        assert_eq!(old_entry, Some((entry.0.clone(), entry.1)));
        {
            let old_val = map.get_mut(&entry.0);
            *old_val.unwrap() = val_2;
        }
        entry.1 = val_2;
        assert_eq!(map.get(&entry.0), Some(&val_2));
    }

    thread_rng().shuffle(&mut expected);

    let mut expected_len = expected.len();
    for entry in expected {
        let old_entry = map.remove(&entry.0);
        expected_len -= 1;
        assert_eq!(old_entry, Some((entry.0, entry.1)));
        assert_eq!(map.len(), expected_len);
    }
}
