extern crate rand;

use rand::Rng;
use rand::XorShiftRng;
use std::mem;
use std::ptr;
use std::fmt::Debug;

#[repr(C)]
#[derive(Debug)]
struct Node<T: Ord + Debug, U: Debug> {
    height: usize,
    value: U,
    key: T,
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

    pub fn get_pointer(&self, height: usize) -> &*mut Node<T, U> {
        unsafe { self.data.get_unchecked(height) }
    }

    pub fn get_pointer_mut(&mut self, height: usize) -> &mut *mut Node<T, U> {
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
        ptr::write_bytes((*ptr).data.get_unchecked_mut(0), 0, height);
        ptr
    }

    unsafe fn free(ptr: *mut Self) {
        let height = (*ptr).height;
        let cap = Self::get_size_in_u64s(height);
        drop(Vec::from_raw_parts(ptr as *mut u64, 0, cap));
    }
}

pub struct SkipMap<T: Ord + Debug, U: Debug> {
    head: *mut Node<T, U>,
    rng: XorShiftRng,
    size: usize,
}

impl<T: Ord + Debug, U: Debug> SkipMap<T, U> {
    pub fn new() -> Self {
        SkipMap {
            head: unsafe { Node::allocate(MAX_HEIGHT + 1) },
            rng: XorShiftRng::new_unseeded(),
            size: 0,
        }
    }

    fn get_starting_height(&self) -> usize {
        MAX_HEIGHT - self.size.leading_zeros() as usize
    }

    fn gen_random_height(&mut self) -> usize {
        self.rng.next_u64().leading_zeros() as usize
    }

    pub fn insert(&mut self, key: T, value: U) -> Option<(T, U)> {
        self.size += 1;
        let new_height = self.gen_random_height();
        let new_node = Node::new(key, value, new_height + 1);
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &mut self.head;
        let mut ret = None;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer_mut(curr_height);
                while !next_node.is_null() && (**next_node).key < (*new_node).key {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer_mut(curr_height));
                }

                if !next_node.is_null() && (**next_node).key == (*new_node).key {
                    let temp = *next_node;
                    *(**curr_node).get_pointer_mut(curr_height) = *(**next_node).get_pointer_mut(curr_height);
                    if curr_height == 0 {
                        ret = Some((ptr::read(&(*temp).key), ptr::read(&(*temp).value)));
                        Node::free(temp);
                        self.size -= 1;
                    }
                }

                if curr_height <= new_height {
                    *(*new_node).get_pointer_mut(curr_height) = mem::replace(&mut *(**curr_node).get_pointer_mut(curr_height), new_node);
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            ret
        }
    }

    pub fn remove(&mut self, key: &T) -> Option<(T, U)> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &mut self.head;
        let mut ret = None;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer_mut(curr_height);
                while !next_node.is_null() && (**next_node).key < *key {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer_mut(curr_height));
                }

                if !next_node.is_null() && (**next_node).key == *key {
                    let temp = *next_node;
                    *(**curr_node).get_pointer_mut(curr_height) = *(**next_node).get_pointer_mut(curr_height);
                    if curr_height == 0 {
                        ret = Some((ptr::read(&(*temp).key), ptr::read(&(*temp).value)));
                        Node::free(temp);
                        self.size -= 1;
                    }
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            ret
        }
    }

    pub fn contains(&self, key: &T) -> bool {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() && (**next_node).key < *key {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer(curr_height));
                }

                if !next_node.is_null() && (**next_node).key == *key {
                    return true;
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            false
        }
    }

    pub fn get(&self, key: &T) -> Option<&U> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() && (**next_node).key < *key {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer(curr_height));
                }

                if !next_node.is_null() && (**next_node).key == *key {
                    return Some(&(**next_node).value);
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            None
        }
    }

    pub fn get_mut(&mut self, key: &T) -> Option<&mut U> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &mut self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer_mut(curr_height);
                while !next_node.is_null() && (**next_node).key < *key {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer_mut(curr_height));
                }

                if !next_node.is_null() && (**next_node).key == *key {
                    return Some(&mut (**next_node).value);
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            None
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn clear(&mut self) {
        self.size = 0;
        unsafe {
            let mut curr_node = *(*self.head).get_pointer(0);
            while !curr_node.is_null() {
                ptr::drop_in_place(&mut (*curr_node).key);
                ptr::drop_in_place(&mut (*curr_node).value);
                Node::free(mem::replace(&mut curr_node, *(*curr_node).get_pointer(0)));
            }
            ptr::write_bytes((*self.head).data.get_unchecked_mut(0), 0, MAX_HEIGHT);
        }
    }

    pub fn ceil(&self, key: &T) -> Option<&T> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() && (**next_node).key < *key {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer(curr_height));
                }

                if curr_height == 0 {
                    return match next_node.is_null() {
                        false => Some(&(**next_node).key),
                        true => None,
                    };
                }

                curr_height -= 1;
            }
        }
    }

    pub fn floor(&self, key: &T) -> Option<&T> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() && (**next_node).key <= *key {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer(curr_height));
                }

                if curr_height == 0 {
                    return match curr_node == &self.head {
                        false => Some(&(**curr_node).key),
                        true => None,
                    };
                }

                curr_height -= 1;
            }
        }
    }

    pub fn min(&self) -> Option<&T> {
        unsafe {
            let min_node = (*self.head).get_pointer(0);
            match min_node.is_null() {
                false => Some(&(**min_node).key),
                true => None,
            }
        }
    }

    pub fn max(&self) -> Option<&T> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() {
                    curr_node = mem::replace(&mut next_node, (**next_node).get_pointer(curr_height));
                }

                if curr_height == 0 {
                    return match curr_node == &self.head {
                        false => Some(&(**curr_node).key),
                        true => None,
                    };
                }

                curr_height -= 1;
            }
        }
    }

    pub fn iter(&self) -> SkipMapIter<T, U> {
        unsafe { SkipMapIter { current: &*(*self.head).get_pointer(0) } }
    }

    pub fn iter_mut(&self) -> SkipMapIterMut<T, U> {
        unsafe { SkipMapIterMut { current: &mut *(*self.head).get_pointer_mut(0) } }
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

impl<T: Ord + Debug, U: Debug> Drop for SkipMap<T, U> {
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

impl<T: Ord + Debug, U: Debug> IntoIterator for SkipMap<T, U> {
    type Item = (T, U);
    type IntoIter = SkipMapIntoIter<T, U>;

    fn into_iter(self) -> Self::IntoIter {
        unsafe {
            let ret = SkipMapIntoIter { current: *(*self.head).data.get_unchecked_mut(0) };
            ptr::write_bytes((*self.head).data.get_unchecked_mut(0), 0, MAX_HEIGHT);
            ret
        }
    }
}

impl<'a, T: 'a + Ord + Debug, U: 'a + Debug> IntoIterator for &'a SkipMap<T, U> {
    type Item = (&'a T, &'a U);
    type IntoIter = SkipMapIter<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: 'a + Ord + Debug, U: 'a + Debug> IntoIterator for &'a mut SkipMap<T, U> {
    type Item = (&'a T, &'a mut U);
    type IntoIter = SkipMapIterMut<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

pub struct SkipMapIntoIter<T: Ord + Debug, U: Debug> {
    current: *mut Node<T, U>,
}

impl<T: Ord + Debug, U: Debug> Iterator for SkipMapIntoIter<T, U> {
    type Item = (T, U);

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.is_null() {
            false => unsafe {
                let ret = (
                    ptr::read(&(*self.current).key),
                    ptr::read(&(*self.current).value),
                );
                println!("FREEING BT READING");
                Node::free(mem::replace(&mut self.current, *(*self.current).get_pointer(0)));
                Some(ret)
            },
            true => None,
        }
    }
}

impl<T: Ord + Debug, U: Debug> Drop for SkipMapIntoIter<T, U> {
    fn drop(&mut self) {
        unsafe {
            while !self.current.is_null() {
                ptr::drop_in_place(&mut (*self.current).key);
                ptr::drop_in_place(&mut (*self.current).value);
                Node::free(mem::replace(&mut self.current, *(*self.current).get_pointer(0)));
            }
        }
    }
}

pub struct SkipMapIter<'a, T: 'a + Ord + Debug, U: 'a + Debug> {
    current: &'a *mut Node<T, U>,
}

impl<'a, T: 'a + Ord + Debug, U: 'a + Debug> Iterator for SkipMapIter<'a, T, U> {
    type Item = (&'a T, &'a U);

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.is_null() {
            false => unsafe {
                let ret = (
                    &(**self.current).key,
                    &(**self.current).value,
                );
                mem::replace(&mut self.current, &*(**self.current).get_pointer(0));
                Some(ret)
            },
            true => None,
        }
    }
}

pub struct SkipMapIterMut<'a, T: 'a + Ord + Debug, U: 'a + Debug> {
    current: &'a mut *mut Node<T, U>,
}

impl<'a, T: 'a + Ord + Debug, U: 'a + Debug> Iterator for SkipMapIterMut<'a, T, U> {
    type Item = (&'a T, &'a mut U);

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.is_null() {
            false => unsafe {
                let ret = (
                    &(**self.current).key,
                    &mut (**self.current).value,
                );
                mem::replace(&mut self.current, &mut *(**self.current).get_pointer_mut(0));
                Some(ret)
            },
            true => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SkipMap;

    #[test]
    fn test_size_empty() {
        let tree: SkipMap<u32, u32> = SkipMap::new();
        assert_eq!(tree.size(), 0);
    }

    #[test]
    fn test_is_empty() {
        let tree: SkipMap<u32, u32> = SkipMap::new();
        assert!(tree.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let tree: SkipMap<u32, u32> = SkipMap::new();
        assert_eq!(tree.min(), None);
        assert_eq!(tree.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut tree = SkipMap::new();
        tree.insert(1, 1);
        assert!(tree.contains(&1));
        assert_eq!(tree.get(&1), Some(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut tree = SkipMap::new();
        let ret_1 = tree.insert(1, 1);
        let ret_2 = tree.insert(1, 3);
        assert_eq!(tree.get(&1), Some(&3));
        assert_eq!(ret_1, None);
        assert_eq!(ret_2, Some((1, 1)));
    }

    #[test]
    fn test_remove() {
        let mut tree = SkipMap::new();
        tree.insert(1, 1);
        let ret = tree.remove(&1);
        assert!(!tree.contains(&1));
        assert_eq!(ret, Some((1, 1)));
    }

    #[test]
    fn test_min_max() {
        let mut tree = SkipMap::new();
        tree.insert(1, 1);
        tree.insert(3, 3);
        tree.insert(5, 5);

        assert_eq!(tree.min(), Some(&1));
        assert_eq!(tree.max(), Some(&5));
    }

    #[test]
    fn test_get_mut() {
        let mut tree = SkipMap::new();
        tree.insert(1, 1);
        {
            let value = tree.get_mut(&1);
            *value.unwrap() = 3;
        }
        assert_eq!(tree.get(&1), Some(&3));
    }

    #[test]
    fn test_floor_ceil() {
        let mut tree = SkipMap::new();
        tree.insert(1, 1);
        tree.insert(3, 3);
        tree.insert(5, 5);

        assert_eq!(tree.floor(&0), None);
        assert_eq!(tree.floor(&2), Some(&1));
        assert_eq!(tree.floor(&4), Some(&3));
        assert_eq!(tree.floor(&6), Some(&5));

        assert_eq!(tree.ceil(&0), Some(&1));
        assert_eq!(tree.ceil(&2), Some(&3));
        assert_eq!(tree.ceil(&4), Some(&5));
        assert_eq!(tree.ceil(&6), None);
    }

    #[test]
    fn test_into_iter() {
        let mut tree = SkipMap::new();
        tree.insert(1, 2);
        tree.insert(5, 6);
        tree.insert(3, 4);

        assert_eq!(
            tree.into_iter().collect::<Vec<(u32, u32)>>(),
            vec![(1, 2), (3, 4), (5, 6)],
        );
    }

    #[test]
    fn test_iter() {
        let mut tree = SkipMap::new();
        tree.insert(1, 2);
        tree.insert(5, 6);
        tree.insert(3, 4);

        assert_eq!(
            tree.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &2), (&3, &4), (&5, &6)],
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut tree = SkipMap::new();
        tree.insert(1, 2);
        tree.insert(5, 6);
        tree.insert(3, 4);

        for (_, value) in &mut tree {
            *value += 1;
        }

        assert_eq!(
            tree.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &3), (&3, &5), (&5, &7)],
        );
    }
}
