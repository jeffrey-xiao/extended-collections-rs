extern crate data_structures;
extern crate rand;

use data_structures::treap::TreapMap;
use std::vec::Vec;
use self::rand::Rng;

#[test]
fn int_test_treap() {
    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
    let mut tree = TreapMap::new();
    let mut expected = Vec::new();
    for _ in 0..100_000 {
        let key = rng.gen::<u32>();
        let val = rng.gen::<u32>();

        tree.insert(key, val);
        expected.push((key, val));
    }

    expected.reverse();
    expected.sort_by(|l, r| l.0.cmp(&r.0));
    expected.dedup_by_key(|pair| pair.0);

    assert_eq!(tree.size(), expected.len());

    assert_eq!(tree.min(), Some(&expected[0].0));
    assert_eq!(tree.max(), Some(&expected[expected.len() - 1].0));

    for entry in &expected {
        assert!(tree.contains(&entry.0));
        assert_eq!(tree.get(&entry.0), Some(&entry.1));
        assert_eq!(tree.ceil(&entry.0), Some(&entry.0));
        assert_eq!(tree.floor(&entry.0), Some(&entry.0));
    }

    for entry in &mut expected {
        let val_1 = rng.gen::<u32>();
        let val_2 = rng.gen::<u32>();

        let old_entry = tree.insert(entry.0, val_1);
        assert_eq!(old_entry, Some((entry.0, entry.1)));
        {
            let old_val = tree.get_mut(&entry.0);
            *old_val.unwrap() = val_2;
        }
        *entry = (entry.0, val_2);
        assert_eq!(tree.get(&entry.0), Some(&val_2));
    }

    let mut expected_size = expected.len();
    for entry in expected {
        let old_entry = tree.remove(&entry.0);
        expected_size -= 1;
        assert_eq!(old_entry, Some((entry.0, entry.1)));
        assert_eq!(tree.size(), expected_size);
    }
}
