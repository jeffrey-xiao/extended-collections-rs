extern crate extended_collections;
extern crate rand;

use self::rand::{thread_rng, Rng};
use extended_collections::lsm_tree::{LsmMap, Result, SizeTieredStrategy};
use std::fs;
use std::panic;
use std::vec::Vec;

fn teardown(test_name: &str) {
    fs::remove_dir_all(format!("{}", test_name)).ok();
}

fn run_test<T>(test: T, test_name: &str)
where
    T: FnOnce() -> Result<()> + panic::UnwindSafe,
{
    let result = panic::catch_unwind(|| test().unwrap());

    teardown(test_name);

    assert!(result.is_ok());
}

#[test]
fn int_test_lsm_map_size_tiered_strategy() {
    let test_name = "int_test_lsm_map";
    run_test(
        || {
            let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
            let mut sts = SizeTieredStrategy::new(test_name, 4, 400, 0.5, 1.5, 100)?;
            let mut map = LsmMap::new(sts);
            let mut expected = Vec::new();

            for _ in 0..10_000 {
                let key = rng.gen::<u32>();
                let val = rng.gen::<u64>();

                map.insert(key, val)?;
                expected.push((key, val));
            }

            expected.reverse();
            expected.sort_by(|l, r| l.0.cmp(&r.0));
            expected.dedup_by_key(|pair| pair.0);

            sts = SizeTieredStrategy::open(test_name)?;
            map = LsmMap::new(sts);

            for entry in &expected {
                assert!(map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, Some(entry.1));
            }

            thread_rng().shuffle(&mut expected);

            let mut expected_len = expected.len();

            for entry in expected {
                let old_entry = map.remove(entry.0)?;
                expected_len -= 1;
                assert!(!map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, None);
            }
            Ok(())
        },
        test_name,
    );
}
