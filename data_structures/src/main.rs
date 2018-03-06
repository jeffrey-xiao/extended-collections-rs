extern crate data_structures;
use data_structures::btree;
use std::marker::PhantomData;

fn main() {
    // let mut t: btree::Tree<u32, u32> = btree::Tree::open("test.db").unwrap();
    let mut t: btree::Tree<u32, u32> = btree::Tree::new("test.db").unwrap();
    t.insert(1, 1);
    t.insert(16, 16);
    t.insert(25, 25);
    t.insert(4, 4);
    t.insert(9, 9);
    t.print();
    println!("------------------");

    t.insert(20, 20);
    t.print();
    println!("------------------");

    t.insert(13, 13);
    t.print();
    println!("------------------");

    t.insert(15, 15);
    t.print();
    println!("------------------");

    t.insert(10, 10);
    t.print();
    println!("------------------");

    t.insert(11, 11);
    t.print();
    println!("------------------");

    t.insert(12, 12);
    t.print();
    println!("------------------");
}
