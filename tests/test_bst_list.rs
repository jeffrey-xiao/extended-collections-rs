const NUM_OF_OPERATIONS: usize = 100_000;

macro_rules! bst_list_tests {
    ($($module_name:ident: $type_name:ident$(,)*)*) => {
        $(
            mod $module_name {
                use extended_collections::$module_name::$type_name;
                use rand::Rng;
                use super::NUM_OF_OPERATIONS;

                #[test]
                fn int_test_list() {
                    let mut rng: rand::XorShiftRng = rand::SeedableRng::from_seed([1, 1, 1, 1]);
                    let mut list = $type_name::new();
                    let mut expected = Vec::new();

                    for i in 0..NUM_OF_OPERATIONS {
                        let index = rng.gen_range(0, i + 1);
                        let val = rng.gen::<u32>();

                        list.insert(index, val);
                        expected.insert(index, val);
                    }

                    assert_eq!(list.len(), expected.len());
                    assert_eq!(
                        list.iter().collect::<Vec<&u32>>(),
                        expected.iter().collect::<Vec<&u32>>(),
                    );

                    for i in (0..NUM_OF_OPERATIONS).rev() {
                        let index = rng.gen_range(0, i + 1);
                        let val = rng.gen::<u32>();

                        list[index] = val;
                        expected[index] = val;

                        assert_eq!(list[index], expected[index]);
                        assert_eq!(list.remove(index), expected.remove(index));
                    }
                }
            }
        )*
    }
}

bst_list_tests!(skiplist: SkipList, treap: TreapList);
