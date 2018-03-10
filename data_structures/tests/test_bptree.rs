extern crate data_structures;
extern crate rand;

use data_structures::bptree::BPMap;
use std::fs;
use std::vec::Vec;
use self::rand::{thread_rng, Rng};

#[test]
fn int_test_bpmap() {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut map = BPMap::with_degrees("db.dat", 3, 3).expect("Could not create B+ tree.");
    let mut expected = Vec::new();
    for _ in 0..1000 {
        let key = rng.gen::<u32>();
        let val = rng.gen::<u32>();

        map.insert(key, val);
        expected.push((key, val));
    }

    expected.reverse();
    expected.sort_by(|l, r| l.0.cmp(&r.0));
    expected.dedup_by_key(|pair| pair.0);

    map = BPMap::open("db.dat").expect("Could not open B+ tree.");

    assert_eq!(map.len(), expected.len());

    assert_eq!(map.min(), Some(expected[0].0));
    assert_eq!(map.max(), Some(expected[expected.len() - 1].0));

    for entry in &expected {
        assert!(map.contains_key(&entry.0));
        assert_eq!(map.get(&entry.0), Some(entry.1));
    }

    thread_rng().shuffle(&mut expected);

    let mut expected_len = expected.len();

    for entry in expected {
        let old_entry = map.remove(&entry.0);
        expected_len -= 1;
        assert_eq!(old_entry, Some((entry.0, entry.1)));
        assert_eq!(map.len(), expected_len);
    }

    fs::remove_file("db.dat").expect("Could not remove database file.");
}
