use extended_collections::lsm_tree::compaction::{LeveledStrategy, SizeTieredStrategy};
use extended_collections::lsm_tree::{LsmMap, Result};
use rand::{thread_rng, Rng};
use std::fs;
use std::panic;
use std::vec::Vec;

fn teardown(test_name: &str) {
    fs::remove_dir_all(test_name).ok();
}

fn run_test<T>(test: T, test_name: &str) -> Result<()>
where
    T: FnOnce() -> Result<()>,
{
    let result = test();
    teardown(test_name);
    result
}

#[test]
fn int_test_lsm_map_size_tiered_strategy() -> Result<()> {
    let test_name = "int_test_lsm_map_size_tiered_strategy";
    run_test(
        || {
            let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
            let mut sts = SizeTieredStrategy::new(test_name, 1000, 4, 4000, 0.5, 1.5)?;
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

            assert_eq!(map.len()?, expected.len());
            assert_eq!(map.len_hint()?, expected.len());

            assert_eq!(map.min()?, Some(expected[0].0));
            assert_eq!(map.max()?, Some(expected[expected.len() - 1].0));

            map.flush()?;
            sts = SizeTieredStrategy::open(test_name)?;
            map = LsmMap::new(sts);

            for entry in &expected {
                assert!(map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, Some(entry.1));
            }

            thread_rng().shuffle(&mut expected);

            let mut expected_len = expected.len();

            for (index, entry) in expected.iter().rev().enumerate() {
                assert!(map.contains_key(&entry.0)?);
                map.remove(entry.0)?;
                expected_len -= 1;
                assert!(!map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, None);

                assert!(map.len_hint()? >= expected_len);
                if index % 5000 == 0 {
                    assert_eq!(map.len()?, expected_len);
                }
            }

            expected.clear();

            for _ in 0..1000 {
                let key = rng.gen::<u32>();
                let val = rng.gen::<u64>();

                map.insert(key, val)?;
                expected.push((key, val));
            }
            map.clear()?;

            for entry in &expected {
                assert!(!map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, None);
            }

            assert_eq!(map.min()?, None);
            assert_eq!(map.max()?, None);

            map.flush()?;
            Ok(())
        },
        test_name,
    )
}

#[test]
fn int_test_lsm_map_leveled_strategy() -> Result<()> {
    let test_name = "int_test_lsm_map_leveled_strategy";
    run_test(
        || {
            let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
            let mut ls = LeveledStrategy::new(test_name, 1000, 4, 4000, 10, 10)?;
            let mut map = LsmMap::new(ls);
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

            assert_eq!(map.len()?, expected.len());
            assert_eq!(map.len_hint()?, expected.len());

            assert_eq!(map.min()?, Some(expected[0].0));
            assert_eq!(map.max()?, Some(expected[expected.len() - 1].0));

            map.flush()?;
            ls = LeveledStrategy::open(test_name)?;
            map = LsmMap::new(ls);

            for entry in &expected {
                assert!(map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, Some(entry.1));
            }

            thread_rng().shuffle(&mut expected);

            let mut expected_len = expected.len();

            for (index, entry) in expected.iter().rev().enumerate() {
                assert!(map.contains_key(&entry.0)?);
                map.remove(entry.0)?;
                expected_len -= 1;
                assert!(!map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, None);

                assert!(map.len_hint()? >= expected_len);
                if index % 5000 == 0 {
                    assert_eq!(map.len()?, expected_len);
                }
            }

            expected.clear();

            for _ in 0..1000 {
                let key = rng.gen::<u32>();
                let val = rng.gen::<u64>();

                map.insert(key, val)?;
                expected.push((key, val));
            }
            map.clear()?;

            for entry in &expected {
                assert!(!map.contains_key(&entry.0)?);
                assert_eq!(map.get(&entry.0)?, None);
            }

            assert_eq!(map.min()?, None);
            assert_eq!(map.max()?, None);

            map.flush()?;
            Ok(())
        },
        test_name,
    )
}
