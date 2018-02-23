#![feature(alloc_system, global_allocator, allocator_api)]

extern crate alloc_system;

use alloc_system::System;

#[global_allocator]
static A: System = System;

extern crate rand;

use rand::Rng;
use rand::XorShiftRng;
use std::mem;
use std::ptr;
use std::fmt::Debug;

#[repr(C)]
#[derive(Debug)]
struct Node<T: Ord + Debug, U: Debug> {
    key: T,
    value: U,
    height: usize,
    data: [*mut Node<T, U>; 0],
}

const MAX_HEIGHT: usize = 64;

impl<T: Ord + Debug, U: Debug> Node<T, U> {
    pub fn new(key: T, value: U, height: usize) -> *mut Self {
        let ptr = unsafe { Self::allocate(height) };
        unsafe {
            ptr::write(&mut (*ptr).key, key);
            ptr::write(&mut (*ptr).value, value);
        }
        ptr
    }

    pub fn get_pointer(&mut self, height: usize) -> &mut *mut Node<T, U> {
        unsafe { self.data.get_unchecked_mut(height) }
    }

    fn get_size_in_u64s(height: usize) -> usize {
        let base_size = mem::size_of::<Node<T, U>>();
        let ptr_size = mem::size_of::<*mut Node<T, U>>();
        let u64_size = mem::size_of::<u64>();

        (base_size + ptr_size * height + u64_size - 1) / u64_size
    }

    unsafe fn allocate(height: usize) -> *mut Self {
        let mut v = Vec::<u64>::with_capacity(Self::get_size_in_u64s(height));
        let ptr = v.as_mut_ptr() as *mut Node<T, U>;
        mem::forget(v);
        ptr::write(&mut (*ptr).height, height);
        // fill with null pointers
        ptr::write_bytes((*ptr).data.get_unchecked_mut(0), 0, 2);
        ptr
    }

    unsafe fn free(ptr: *mut Self) {
        let height = (*ptr).height;
        let cap = Self::get_size_in_u64s(height);
        drop(Vec::from_raw_parts(ptr as *mut u64, 0, cap));
    }
}

pub struct SkipList<T: Ord + Debug, U: Debug> {
    head: *mut Node<T, U>,
    rng: XorShiftRng,
}

impl<T: Ord + Debug, U: Debug> SkipList<T, U> {
    pub fn new() -> Self {
        SkipList {
            head: unsafe { Node::allocate(MAX_HEIGHT + 1) },
            rng: XorShiftRng::new_unseeded(),
        }
    }

    fn gen_random_height(&mut self) -> usize {
        let mut n = self.rng.next_u64();
        let mut ret = 0;
        if n & 0x00000000FFFFFFFF == 0 { ret += 32; n >>= 32; }
        if n & 0x000000000000FFFF == 0 { ret += 16; n >>= 16; }
        if n & 0x00000000000000FF == 0 { ret +=  8; n >>= 8; }
        if n & 0x000000000000000F == 0 { ret +=  4; n >>= 4; }
        if n & 0x0000000000000003 == 0 { ret +=  2; n >>= 2; }
        if n & 0x0000000000000001 == 0 { ret +=  1; n >>= 1; }
        if n & 0x0000000000000001 == 0 { ret +=  1; }
        ret
    }

    pub fn insert(&mut self, key: T, value: U) -> Option<(T, U)> {
        unsafe {
            let new_height = self.gen_random_height();
            let new_node = Node::new(key, value, new_height + 1);
            let mut curr_node = &mut self.head;
            let mut curr_height = MAX_HEIGHT;
            let mut ret = None;
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() && (**next_node).key < (*new_node).key {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer(curr_height));
                }

                if !next_node.is_null() && (**next_node).key == (*new_node).key {
                    let temp = *next_node;
                    *(**curr_node).get_pointer(curr_height) = *(**next_node).get_pointer(curr_height);
                    if curr_height == 0 {
                        ret = Some((ptr::read(&(*temp).key), ptr::read(&(*temp).value)));
                        Node::free(temp);
                    }
                }

                if curr_height <= new_height {
                    *(*new_node).get_pointer(curr_height) = mem::replace(&mut *(**curr_node).get_pointer(curr_height), new_node);
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            ret
        }
    }

    pub fn print(&self) {
        unsafe {
            let mut curr_node = (*self.head).get_pointer(0);
            while !curr_node.is_null() {
                println!("{:?} {:?}", (**curr_node).key, (**curr_node).value);
                curr_node = (**curr_node).get_pointer(0);
            }
        }
    }
}

impl<T: Ord + Debug, U: Debug> Drop for SkipList<T, U> {
    fn drop(&mut self) {
        unsafe {
            Node::free(mem::replace(&mut self.head, *(*self.head).get_pointer(0)));
            while !self.head.is_null() {
                ptr::drop_in_place(&mut (*self.head).key);
                ptr::drop_in_place(&mut (*self.head).value);
                Node::free(mem::replace(&mut self.head, *(*self.head).get_pointer(0)));
            }
        }
    }
}

fn main () {
    let mut t: SkipList<u32, u32> = SkipList::new();
    assert_eq!(t.insert(2, 2), None);
    assert_eq!(t.insert(4, 4), None);
    assert_eq!(t.insert(3, 3), None);
    assert_eq!(t.insert(1, 1), None);
    assert_eq!(t.insert(0, 0), None);
    assert_eq!(t.insert(5, 5), None);
    assert_eq!(t.insert(5, 6), Some((5, 5)));
    assert_eq!(t.insert(0, 6), Some((0, 0)));
    t.print();
}
