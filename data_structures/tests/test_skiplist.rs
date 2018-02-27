extern crate data_structures;
extern crate rand;

use data_structures::skiplist::SkipList;
use data_structures::skiplist::SkipMap;
use std::vec::Vec;
use self::rand::{thread_rng, Rng};

#[test]
fn int_test_skipmap() {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut map = SkipMap::new();
    let mut expected = Vec::new();
    for _ in 0..100_000 {
        let key = rng.gen::<u32>();
        let val = rng.gen::<u32>();

        map.insert(key, val);
        expected.push((key, val));
    }

    expected.reverse();
    expected.sort_by(|l, r| l.0.cmp(&r.0));
    expected.dedup_by_key(|pair| pair.0);

    assert_eq!(map.size(), expected.len());

    assert_eq!(map.min(), Some(&expected[0].0));
    assert_eq!(map.max(), Some(&expected[expected.len() - 1].0));

    for entry in &expected {
        assert!(map.contains_key(&entry.0));
        assert_eq!(map.get(&entry.0), Some(&entry.1));
        assert_eq!(map.ceil(&entry.0), Some(&entry.0));
        assert_eq!(map.floor(&entry.0), Some(&entry.0));
    }

    for entry in &mut expected {
        let val_1 = rng.gen::<u32>();
        let val_2 = rng.gen::<u32>();

        let old_entry = map.insert(entry.0, val_1);
        assert_eq!(old_entry, Some((entry.0, entry.1)));
        {
            let old_val = map.get_mut(&entry.0);
            *old_val.unwrap() = val_2;
        }
        *entry = (entry.0, val_2);
        assert_eq!(map.get(&entry.0), Some(&val_2));
    }

    thread_rng().shuffle(&mut expected);

    let mut expected_size = expected.len();
    for entry in expected {
        let old_entry = map.remove(&entry.0);
        expected_size -= 1;
        assert_eq!(old_entry, Some((entry.0, entry.1)));
        assert_eq!(map.size(), expected_size);
    }
}

#[test]
fn int_test_skiplist() {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut list = SkipList::new();

    let mut expected = Vec::new();

    for i in 0..100_000 {
        let index = rng.gen_range(0, i + 1);
        let val = rng.gen::<u32>();

        list.insert(index, val);
        expected.insert(index, val);
    }

    assert_eq!(list.size(), expected.len());
    assert_eq!(
        list.iter().collect::<Vec<&u32>>(),
        expected.iter().collect::<Vec<&u32>>(),
    );

    for i in (0..100_000).rev() {
        let index = rng.gen_range(0, i + 1);
        let val = rng.gen::<u32>();

        list[index] = val;
        expected[index] = val;

        assert_eq!(list[index], expected[index]);
        assert_eq!(list.remove(index), expected.remove(index));
    }
}
