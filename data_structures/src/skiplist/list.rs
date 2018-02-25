extern crate rand;

use std::fmt::Debug;
use rand::Rng;
use rand::XorShiftRng;
use std::mem;
use std::ops::{Index, IndexMut};
use std::ptr;

#[repr(C)]
#[derive(Copy, Clone)]
struct Link<T: Debug + PartialEq> {
    next: *mut Node<T>,
    distance: usize,
}

#[repr(C)]
struct Node<T: Debug + PartialEq> {
    links_size: usize,
    key: T,
    links: [Link<T>; 0],
}

const MAX_HEIGHT: usize = 32;

impl<T: Debug + PartialEq> Node<T> {
    pub fn new(key: T, links_size: usize) -> *mut Self {
        let ptr = unsafe { Self::allocate(links_size) };
        unsafe { ptr::write(&mut (*ptr).key, key); }
        ptr
    }

    pub fn get_pointer(&self, height: usize) -> &Link<T> {
        unsafe { self.links.get_unchecked(height) }
    }

    pub fn get_pointer_mut(&mut self, height: usize) -> &mut Link<T> {
        unsafe { self.links.get_unchecked_mut(height) }
    }

    fn get_size_in_u64s(links_size: usize) -> usize {
        let base_size = mem::size_of::<Node<T>>();
        let link_size = mem::size_of::<Link<T>>();
        let u64_size = mem::size_of::<u64>();

        (base_size + link_size * links_size + u64_size - 1) / u64_size
    }

    unsafe fn allocate(links_size: usize) -> *mut Self {
        let mut v = Vec::<u64>::with_capacity(Self::get_size_in_u64s(links_size));
        let ptr = v.as_mut_ptr() as *mut Node<T>;
        mem::forget(v);
        ptr::write(&mut (*ptr).links_size, links_size);
        // fill with null pointers
        ptr::write_bytes((*ptr).links.get_unchecked_mut(0), 0, links_size);
        ptr
    }

    unsafe fn deallocate(ptr: *mut Self) {
        let links_size = (*ptr).links_size;
        let cap = Self::get_size_in_u64s(links_size);
        drop(Vec::from_raw_parts(ptr as *mut u64, 0, cap));
    }

    unsafe fn free(ptr: *mut Self) {
        ptr::drop_in_place(&mut (*ptr).key);
        Self::deallocate(ptr);
    }
}

pub struct SkipList<T: Debug + PartialEq> {
    head: *mut Node<T>,
    rng: XorShiftRng,
    size: usize,
}

impl<T: Debug + PartialEq> SkipList<T> {
    /// Constructs a new, empty `SkipList<T>`
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let list: SkipList<u32> = SkipList::new();
    /// ```
    pub fn new() -> Self {
        SkipList {
            head: unsafe { Node::allocate(MAX_HEIGHT + 1) },
            rng: rand::SeedableRng::from_seed([1235, 15212, 12512512, 12512521]),
            size: 0,
        }
    }

    fn gen_random_height(&mut self) -> usize {
        self.rng.next_u32().leading_zeros() as usize
    }

    /// Inserts a value into the list at a particular index, shifting elements one position to the
    /// right if needed.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.insert(0, 1);
    /// list.insert(0, 2);
    /// assert_eq!(list.get(0), Some(&2));
    /// assert_eq!(list.get(1), Some(&1));
    /// ```
    pub fn insert(&mut self, mut index: usize, value: T) {
        assert!(index <= self.size);
        self.size += 1;
        let new_height = self.gen_random_height();
        let new_node = Node::new(value, new_height + 1);
        let mut curr_height = MAX_HEIGHT;
        let mut last_nodes = [(self.head, 0); MAX_HEIGHT + 1];
        let mut curr_node = &mut self.head;

        unsafe {
            loop {
                let mut next_link = (**curr_node).get_pointer_mut(curr_height);
                while !next_link.next.is_null() && next_link.distance <= index {
                    last_nodes[curr_height].1 += next_link.distance;
                    index -= next_link.distance;
                    curr_node = &mut mem::replace(&mut next_link, (*next_link.next).get_pointer_mut(curr_height)).next;
                }
                last_nodes[curr_height].0 = *curr_node;

                if curr_height <= new_height {
                    *(*new_node).get_pointer_mut(curr_height) = mem::replace(
                        &mut next_link,
                        Link { next: new_node, distance: 1 },
                    );
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }

            for i in 1..MAX_HEIGHT + 1 {
                last_nodes[i].1 += last_nodes[i - 1].1;
                if i <= new_height {
                    (*last_nodes[i].0).get_pointer_mut(i).distance = last_nodes[i - 1].1 + 1;
                    (*new_node).get_pointer_mut(i).distance -= last_nodes[i - 1].1;
                } else {
                    (*last_nodes[i].0).get_pointer_mut(i).distance += 1;
                }
            }
        }
    }

    /// Removes a value at a particular index from the list. Returns the value at the index.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.insert(0, 1);
    /// assert_eq!(list.remove(0), 1);
    /// ```
    pub fn remove(&mut self, mut index: usize) -> T {
        assert!(index < self.size);
        let mut curr_height = MAX_HEIGHT;
        let mut curr_node = &mut self.head;

        unsafe {
            loop {
                let mut next_link = (**curr_node).get_pointer_mut(curr_height);
                while !next_link.next.is_null() && next_link.distance <= index {
                    index -= next_link.distance;
                    curr_node = &mut mem::replace(&mut next_link, (*next_link.next).get_pointer_mut(curr_height)).next;
                }

                if !next_link.next.is_null() {
                    if next_link.distance == index + 1 {
                        let Link { next, distance } = *next_link;
                        mem::swap(next_link, (*next).get_pointer_mut(curr_height));
                        next_link.distance += distance - 1;
                        if curr_height == 0 {
                            Node::deallocate(next);
                            self.size -= 1;
                            return ptr::read(&(*next).key);
                        }
                    } else {
                        next_link.distance -= 1;
                    }
                }

                curr_height -= 1;
            }
        }
    }

    /// Inserts a value at the front of the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.push_front(1);
    /// list.push_front(2);
    /// assert_eq!(list.get(0), Some(&2));
    /// ```
    pub fn push_front(&mut self, value: T) {
        self.insert(0, value);
    }

    /// Inserts a value at the back of the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.push_back(1);
    /// list.push_back(2);
    /// assert_eq!(list.get(0), Some(&1));
    /// ```
    pub fn push_back(&mut self, value: T) {
        let index = self.size();
        self.insert(index, value);
    }

    /// Removes a value at the front of the list.
    ///
    /// # Panics
    /// Panics if list is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.push_back(1);
    /// list.push_back(2);
    /// assert_eq!(list.pop_front(), 1);
    /// ```
    pub fn pop_front(&mut self) -> T {
        self.remove(0)
    }

    /// Removes a value at the back of the list.
    ///
    /// # Panics
    /// Panics if list is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.push_back(1);
    /// list.push_back(2);
    /// assert_eq!(list.pop_back(), 2);
    /// ```
    pub fn pop_back(&mut self) -> T {
        let index = self.size() - 1;
        self.remove(index)
    }

    /// Returns a mutable reference to the value at a particular index. Returns `None` if the
    /// index is out of bounds.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.insert(0, 1);
    /// *list.get_mut(0).unwrap() = 2;
    /// assert_eq!(list.get(0), Some(&2));
    /// ```
    pub fn get(&self, mut index: usize) -> Option<&T> {
        let mut curr_height = MAX_HEIGHT;
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_link = (**curr_node).get_pointer(curr_height);
                while !next_link.next.is_null() && next_link.distance <= index {
                    index -= next_link.distance;
                    curr_node = &mem::replace(&mut next_link, (*next_link.next).get_pointer(curr_height)).next;
                }

                if !next_link.next.is_null() {
                    if next_link.distance == index + 1 {
                        return Some(&(*next_link.next).key);
                    }
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            None
        }
    }

    /// Returns a mutable reference to the value at a particular index. Returns `None` if the
    /// index is out of bounds.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.insert(0, 1);
    /// *list.get_mut(0).unwrap() = 2;
    /// assert_eq!(list.get(0), Some(&2));
    /// ```
    pub fn get_mut(&mut self, mut index: usize) -> Option<&mut T> {
        let mut curr_height = MAX_HEIGHT;
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_link = (**curr_node).get_pointer_mut(curr_height);
                while !next_link.next.is_null() && next_link.distance <= index {
                    index -= next_link.distance;
                    curr_node = &mut mem::replace(&mut next_link, (*next_link.next).get_pointer_mut(curr_height)).next;
                }

                if !next_link.next.is_null() {
                    if next_link.distance == index + 1 {
                        return Some(&mut (*next_link.next).key);
                    }
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            None
        }
    }

    /// Returns the size of the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.insert(0, 1);
    /// assert_eq!(list.size(), 1);
    /// ```
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns `true` if the list is empty.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let list: SkipList<u32> = SkipList::new();
    /// assert!(list.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Clears the list, removing all values.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.insert(0, 1);
    /// list.insert(1, 2);
    /// list.clear();
    /// assert_eq!(list.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.size = 0;
        unsafe {
            let mut curr_node = (*self.head).get_pointer(0).next;
            while !curr_node.is_null() {
                Node::free(mem::replace(&mut curr_node, (*curr_node).get_pointer(0).next));
            }
            ptr::write_bytes((*self.head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);
        }
    }

    /// Returns an iterator over the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.insert(0, 1);
    /// list.insert(1, 2);
    ///
    /// let mut iterator = list.iter();
    /// assert_eq!(iterator.next(), Some(&1));
    /// assert_eq!(iterator.next(), Some(&2));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> SkipListIter<T> {
        unsafe { SkipListIter { current: &(*self.head).get_pointer(0).next } }
    }

    /// Returns a mutable iterator over the list.
    ///
    /// # Examples
    /// ```
    /// use data_structures::skiplist::SkipList;
    ///
    /// let mut list = SkipList::new();
    /// list.insert(0, 1);
    /// list.insert(1, 2);
    ///
    /// for value in &mut list {
    ///   *value += 1;
    /// }
    ///
    /// let mut iterator = list.iter();
    /// assert_eq!(iterator.next(), Some(&2));
    /// assert_eq!(iterator.next(), Some(&3));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter_mut(&mut self) -> SkipListIterMut<T> {
        unsafe { SkipListIterMut { current: &mut (*self.head).get_pointer_mut(0).next } }
    }

    pub fn assert(&mut self) {
        unsafe {
            let mut curr_node = &mut (*self.head).get_pointer_mut(0).next;
            let mut actual = vec![];
            while !curr_node.is_null() {
                actual.push(&(**curr_node).key);
                let mut next_link = (**curr_node).get_pointer_mut(0);
                curr_node = &mut mem::replace(&mut next_link, (*next_link.next).get_pointer_mut(0)).next;
            }

            for i in 1..MAX_HEIGHT + 1 {
                let mut curr_node = &mut (*self.head).get_pointer_mut(i).next;
                while !curr_node.is_null() {
                    let x = &(**curr_node).key;
                    let mut next_link = (**curr_node).get_pointer_mut(i);
                    let dist = next_link.distance;
                    curr_node = &mut mem::replace(&mut next_link, (*next_link.next).get_pointer_mut(0)).next;
                    if !curr_node.is_null() {
                        let y = &(**curr_node).key;

                        assert_eq!(
                            dist,
                            actual.iter().position(|&n| n == y).unwrap() - actual.iter().position(|&n| n == x).unwrap(),
                        );
                    }
                }
            }
        }
    }
}

impl<T: Debug + PartialEq> Drop for SkipList<T> {
    fn drop(&mut self) {
        unsafe {
            Node::deallocate(mem::replace(&mut self.head, (*self.head).get_pointer(0).next));
            while !self.head.is_null() {
                Node::free(mem::replace(&mut self.head, (*self.head).get_pointer(0).next));
            }
        }
    }
}

impl<T: Debug + PartialEq> IntoIterator for SkipList<T> {
    type Item = T;
    type IntoIter = SkipListIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        unsafe {
            let ret = SkipListIntoIter { current: (*self.head).links.get_unchecked_mut(0).next };
            ptr::write_bytes((*self.head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);
            ret
        }
    }
}

impl<'a, T: 'a + Debug + PartialEq> IntoIterator for &'a SkipList<T> {
    type Item = &'a T;
    type IntoIter = SkipListIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: 'a + Debug + PartialEq> IntoIterator for &'a mut SkipList<T> {
    type Item = &'a mut T;
    type IntoIter = SkipListIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// An owning iterator for `SkipList<T>`
///
/// This iterator traverses the elements of the list and yields owned entries.
pub struct SkipListIntoIter<T: Debug + PartialEq> {
    current: *mut Node<T>,
}

impl<T: Debug + PartialEq> Iterator for SkipListIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            None
        } else {
            unsafe {
                let ret = ptr::read(&(*self.current).key);
                Node::deallocate(mem::replace(&mut self.current, (*self.current).get_pointer(0).next));
                Some(ret)
            }
        }
    }
}

impl<T: Debug + PartialEq> Drop for SkipListIntoIter<T> {
    fn drop(&mut self) {
        unsafe {
            while !self.current.is_null() {
                ptr::drop_in_place(&mut (*self.current).key);
                Node::free(mem::replace(&mut self.current, (*self.current).get_pointer(0).next));
            }
        }
    }
}

/// An iterator for `SkipList<T>`
///
/// This iterator traverses the elements of the list in-order and yields immutable references.
pub struct SkipListIter<'a, T: 'a + Debug + PartialEq> {
    current: &'a *mut Node<T>,
}

impl<'a, T: 'a + Debug + PartialEq> Iterator for SkipListIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            None
        } else {
            unsafe {
                let ret = &(**self.current).key;
                mem::replace(&mut self.current, &(**self.current).get_pointer(0).next);
                Some(ret)
            }
        }
    }
}

/// A mutable iterator for `SkipList<T>`
///
/// This iterator traverses the elements of the list in-order and yields mutable references.
pub struct SkipListIterMut<'a, T: 'a + Debug + PartialEq> {
    current: &'a mut *mut Node<T>,
}

impl<'a, T: 'a + Debug + PartialEq> Iterator for SkipListIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            None
        } else {
            unsafe {
                let ret = &mut (**self.current).key;
                mem::replace(&mut self.current, &mut (**self.current).get_pointer_mut(0).next);
                Some(ret)
            }
        }
    }
}

impl<T: Debug + PartialEq> Default for SkipList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Debug + PartialEq> Index<usize> for SkipList<T> {
    type Output = T;
    fn index(&self, key: usize) -> &Self::Output {
        self.get(key).unwrap()
    }
}

impl<T: Debug + PartialEq> IndexMut<usize> for SkipList<T> {
    fn index_mut(&mut self, key: usize) -> &mut Self::Output {
        self.get_mut(key).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::SkipList;

    #[test]
    fn test_size_empty() {
        let list: SkipList<u32> = SkipList::new();
        assert_eq!(list.size(), 0);
    }

    #[test]
    fn test_is_empty() {
        let list: SkipList<u32> = SkipList::new();
        assert!(list.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        assert_eq!(list.get(0), Some(&1));
    }

    #[test]
    fn test_remove() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        let ret = list.remove(0);
        assert_eq!(list.get(0), None);
        assert_eq!(ret, 1);
    }

    #[test]
    fn test_get_mut() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        {
            let value = list.get_mut(0);
            *value.unwrap() = 3;
        }
        assert_eq!(list.get(0), Some(&3));
    }

    #[test]
    fn test_push_front() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        list.push_front(2);
        assert_eq!(list.get(0), Some(&2));
    }

    #[test]
    fn test_push_back() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        list.push_back(2);
        assert_eq!(list.get(1), Some(&2));
    }

    #[test]
    fn test_pop_front() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        list.insert(1, 2);
        assert_eq!(list.pop_front(), 1);
    }

    #[test]
    fn test_pop_back() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        list.insert(1, 2);
        assert_eq!(list.pop_back(), 2);
    }

    // #[test]
    // fn test_add() {
    //     let mut n = SkipList::new();
    //     n.insert(0, 1);
    //     n.insert(0, 2);
    //     n.insert(1, 3);

    //     let mut m = SkipList::new();
    //     m.insert(0, 4);
    //     m.insert(0, 5);
    //     m.insert(1, 6);

    //     let res = n + m;

    //     assert_eq!(
    //         res.iter().collect::<Vec<&u32>>(),
    //         vec![&2, &3, &1, &5, &6, &4],
    //     );
    //     assert_eq!(res.size(), 6);
    // }

    #[test]
    fn test_into_iter() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        list.insert(0, 2);
        list.insert(1, 3);

        assert_eq!(
            list.into_iter().collect::<Vec<u32>>(),
            vec![2, 3, 1],
        );
    }

    #[test]
    fn test_iter() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        list.insert(0, 2);
        list.insert(1, 3);

        assert_eq!(
            list.iter().collect::<Vec<&u32>>(),
            vec![&2, &3, &1],
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut list = SkipList::new();
        list.insert(0, 1);
        list.insert(0, 2);
        list.insert(1, 3);

        for value in &mut list {
            *value += 1;
        }

        assert_eq!(
            list.iter().collect::<Vec<&u32>>(),
            vec![&3, &4, &2],
        );
    }
}
