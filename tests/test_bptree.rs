extern crate extended_collections;
extern crate rand;

use self::rand::{thread_rng, Rng};
use extended_collections::bptree::{BPMap, Result};
use std::fs;
use std::panic;
use std::vec::Vec;

fn teardown(test_name: &str) {
    fs::remove_file(format!("{}.dat", test_name)).ok();
}

fn run_test<T>(test: T, test_name: &str)
where T: FnOnce() -> Result<()> + panic::UnwindSafe {
    let result = panic::catch_unwind(|| test().unwrap());

    teardown(test_name);

    assert!(result.is_ok());
}

#[test]
fn int_test_bpmap() {
    let test_name = "int_test_bpmap";
    run_test(
        || {
            let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
            let mut map: BPMap<u32, u32> = BPMap::with_degrees(&format!("{}.dat", test_name), 5, 5)?;
            let mut expected = Vec::new();
            for _ in 0..10_000 {
                let key = rng.gen::<u32>();
                let val = rng.gen::<u32>();

                map.insert(key, val)?;
                expected.push((key, val));
            }

            expected.reverse();
            expected.sort_by(|l, r| l.0.cmp(&r.0));
            expected.dedup_by_key(|pair| pair.0);

            map = BPMap::open(&format!("{}.dat", test_name))?;

            assert_eq!(map.len(), expected.len());

            assert_eq!(map.min()?, Some(expected[0].0));
            assert_eq!(map.max()?, Some(expected[expected.len() - 1].0));

            for entry in &expected {
                assert!(map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, Some(entry.1));
            }

            thread_rng().shuffle(&mut expected);

            let mut expected_len = expected.len();

            for entry in expected {
                let old_entry = map.remove(&entry.0)?;
                expected_len -= 1;
                assert_eq!(old_entry, Some((entry.0, entry.1)));
                assert_eq!(map.len(), expected_len);
            }
            Ok(())
        },
        test_name,
    );
}
