extern crate extended_collections;

use extended_collections::lsm::{Tree, SizeTieredStrategy, SSTable};
use std::path::PathBuf;

fn main() {
    // let sstable: SSTable<i32, i32> = SSTable::new("test_db/SlQCJlgT3IAjxpEcAmqZXByEcRPT2PG9").unwrap();
    // for item in sstable.data_iter().unwrap() {
    //     println!("{:?}", item);
    // }
    let sts = SizeTieredStrategy::new(
        "test_db",
        4,
        50,
        0.5,
        1.5,
        100,
    );

    let mut tree = Tree::new(sts).unwrap();

    for i in 0..100 {
        tree.insert(i, i).unwrap();
    }

    for i in 0..100 {
        println!("Finding {}", i);
        assert_eq!(tree.get(&i).unwrap(), Some(i));
    }
}
