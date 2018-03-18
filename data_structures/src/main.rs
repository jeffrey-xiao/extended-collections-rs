extern crate data_structures;
use data_structures::radix;

fn get_bytes(key: &str) -> Vec<u8> {
    String::from(key).into_bytes()
}

fn main () {
    let mut root = radix::RadixMap::new();
    root.insert(get_bytes("romane"), 1);
    root.insert(get_bytes("romanus"), 2);
    root.insert(get_bytes("romulus"), 3);
    root.insert(get_bytes("rubens"), 4);
    root.insert(get_bytes("ruber"), 5);
    root.insert(get_bytes("rubicon"), 6);
    root.insert(get_bytes("rubicundus"), 7);
    root.insert(get_bytes("ru"), 8);
    // println!("{:#?}", root);
    println!("{:?}", root.get(&get_bytes("romane")));
    { *root.get_mut(&get_bytes("romane")).unwrap() += 1; }
    println!("{:?}", root.get(&get_bytes("romane")));
    println!("{:?}", root.get(&get_bytes("romanus")));
    println!("{:?}", root.get(&get_bytes("romulus")));
    println!("{:?}", root.get(&get_bytes("rubens")));
    println!("{:?}", root.get(&get_bytes("ruber")));
    println!("{:?}", root.get(&get_bytes("rubicon")));
    println!("{:?}", root.get(&get_bytes("rubicundus")));
    println!("{:?}", root.get(&get_bytes("ru")));
    println!("{:?}", root.get(&get_bytes("ra")));

    for entry in root {
        println!("{:?}", entry);
    }

    // println!("{:?}", root.remove(&get_bytes("romane")));
    // println!("{:?}", root.remove(&get_bytes("romulus")));
    // println!("{:?}", root.remove(&get_bytes("romanus")));
    // println!("{:?}", root.remove(&get_bytes("rubens")));
    // println!("{:?}", root.remove(&get_bytes("rubicon")));
    // println!("{:?}", root.remove(&get_bytes("ruber")));
    // println!("{:?}", root.remove(&get_bytes("rubicundus")));
    // println!("{:?}", root.remove(&get_bytes("ru")));
    // println!("{:#?}", root);
}
