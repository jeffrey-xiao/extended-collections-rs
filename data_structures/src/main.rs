extern crate data_structures;
use data_structures::btree;

fn main() {
    // let mut t = btree::Tree::open("test.db").unwrap();
    let mut t = btree::Tree::with_degrees("test.db", 3, 4).unwrap();
    t.insert(1, 1);
    t.insert(16, 16);
    t.insert(25, 25);
    t.insert(4, 4);
    t.insert(9, 9);
    t.insert(20, 20);
    t.insert(13, 13);
    t.insert(15, 15);
    t.insert(10, 10);
    t.insert(11, 11);
    t.insert(12, 12);
    t.remove(&13);
    t.remove(&15);
    t.remove(&11);
    t.remove(&25);
    t.print();
    println!("------------------");
}
