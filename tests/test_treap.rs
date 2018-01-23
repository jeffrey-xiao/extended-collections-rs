extern crate code;
extern crate rand;

use code::util::data_structures::treap::Tree;
use std::vec::Vec;
use self::rand::Rng;

#[test]
fn name() {
    let mut rng = rand::thread_rng();
    let mut t = Tree::new();
    let mut expected = Vec::new();
    for _ in 0..100000 {
        let key = rng.gen::<u32>();
        let val = rng.gen::<u32>();

        if !t.contains(&key) {
            t.insert(key, val);
            expected.push((key, val));
        }
    }

    let actual = t.traverse();

    expected.sort();
    expected.dedup_by_key(|pair| pair.0);

    assert_eq!(expected.len(), actual.len());
    for i in 0..expected.len() {
        assert_eq!(&expected[i].0, actual[i].0);
        assert_eq!(&expected[i].1, actual[i].1);
    }
}
