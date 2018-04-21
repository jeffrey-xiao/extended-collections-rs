use entry::Entry;
use rand::Rng;
use rand::XorShiftRng;
use std::cmp;
use std::mem;
use std::ops::{Add, Index, IndexMut, Sub};
use std::ptr;

#[repr(C)]
struct Node<T, U>
where T: Ord
{
    links_len: usize,
    entry: Entry<T, U>,
    links: [*mut Node<T, U>; 0],
}

const MAX_HEIGHT: usize = 32;

impl<T, U> Node<T, U>
where T: Ord
{
    pub fn new(key: T, value: U, links_len: usize) -> *mut Self {
        let ptr = unsafe { Self::allocate(links_len) };
        unsafe {
            ptr::write(&mut (*ptr).entry, Entry { key, value });
        }
        ptr
    }

    pub fn get_pointer(&self, height: usize) -> &*mut Node<T, U> {
        unsafe { self.links.get_unchecked(height) }
    }

    pub fn get_pointer_mut(&mut self, height: usize) -> &mut *mut Node<T, U> {
        unsafe { self.links.get_unchecked_mut(height) }
    }

    fn get_size_in_u64s(links_len: usize) -> usize {
        let base_size = mem::size_of::<Node<T, U>>();
        let ptr_size = mem::size_of::<*mut Node<T, U>>();
        let u64_size = mem::size_of::<u64>();

        (base_size + ptr_size * links_len + u64_size - 1) / u64_size
    }

    unsafe fn allocate(links_len: usize) -> *mut Self {
        let mut v = Vec::<u64>::with_capacity(Self::get_size_in_u64s(links_len));
        let ptr = v.as_mut_ptr() as *mut Node<T, U>;
        mem::forget(v);
        ptr::write(&mut (*ptr).links_len, links_len);
        // fill with null pointers
        ptr::write_bytes((*ptr).links.get_unchecked_mut(0), 0, links_len);
        ptr
    }

    unsafe fn deallocate(ptr: *mut Self) {
        let links_len = (*ptr).links_len;
        let cap = Self::get_size_in_u64s(links_len);
        drop(Vec::from_raw_parts(ptr as *mut u64, 0, cap));
    }

    unsafe fn free(ptr: *mut Self) {
        ptr::drop_in_place(&mut (*ptr).entry);
        Self::deallocate(ptr);
    }
}

/// An ordered map implemented by a skiplist.
///
/// A skiplist is a probabilistic data structure that allows for binary search tree operations by
/// maintaining a linked hierarchy of subsequences. The first subsequence is essentially a sorted
/// linked list of all the elements that it contains. Each successive subsequence contains
/// approximately half the elements of the previous subsequence. Using the sparser subsequences,
/// elements can be skipped and searching, insertion, and deletion of entries can be done in
/// approximately logarithm time.
///
/// # Examples
/// ```
/// use extended_collections::skiplist::SkipMap;
///
/// let mut map = SkipMap::new();
/// map.insert(0, 1);
/// map.insert(3, 4);
///
/// assert_eq!(map[&0], 1);
/// assert_eq!(map.get(&1), None);
/// assert_eq!(map.len(), 2);
///
/// assert_eq!(map.min(), Some(&0));
/// assert_eq!(map.ceil(&2), Some(&3));
///
/// map[&0] = 2;
/// assert_eq!(map.remove(&0), Some((0, 2)));
/// assert_eq!(map.remove(&1), None);
/// ```
pub struct SkipMap<T, U>
where T: Ord
{
    head: *mut Node<T, U>,
    rng: XorShiftRng,
    len: usize,
}

impl<T, U> SkipMap<T, U>
where T: Ord
{
    /// Constructs a new, empty `SkipMap<T, U>`.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let map: SkipMap<u32, u32> = SkipMap::new();
    /// ```
    pub fn new() -> Self {
        SkipMap {
            head: unsafe { Node::allocate(MAX_HEIGHT + 1) },
            rng: XorShiftRng::new_unseeded(),
            len: 0,
        }
    }

    fn get_starting_height(&self) -> usize {
        MAX_HEIGHT - (self.len as u32).leading_zeros() as usize
    }

    fn gen_random_height(&mut self) -> usize {
        self.rng.next_u32().leading_zeros() as usize
    }

    /// Inserts a key-value pair into the map. If the key already exists in the map, it will return
    /// and replace the old key-value pair.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// assert_eq!(map.insert(1, 1), None);
    /// assert_eq!(map.get(&1), Some(&1));
    /// assert_eq!(map.insert(1, 2), Some((1, 1)));
    /// assert_eq!(map.get(&1), Some(&2));
    /// ```
    pub fn insert(&mut self, key: T, value: U) -> Option<(T, U)> {
        self.len += 1;
        let new_height = self.gen_random_height();
        let new_node = Node::new(key, value, new_height + 1);
        let mut curr_height = MAX_HEIGHT;
        let mut curr_node = &mut self.head;
        let mut ret = None;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer_mut(curr_height);
                while !next_node.is_null() && (**next_node).entry.key < (*new_node).entry.key {
                    curr_node = mem::replace(
                        &mut next_node,
                        (**next_node).get_pointer_mut(curr_height),
                    );
                }

                if !next_node.is_null() && (**next_node).entry.key == (*new_node).entry.key {
                    let temp = *next_node;
                    *(**curr_node).get_pointer_mut(curr_height) = *(**next_node).get_pointer_mut(curr_height);
                    if curr_height == 0 {
                        ret = Some((
                            ptr::read(&(*temp).entry.key),
                            ptr::read(&(*temp).entry.value),
                        ));
                        Node::deallocate(temp);
                        self.len -= 1;
                    }
                }

                if curr_height <= new_height {
                    *(*new_node).get_pointer_mut(curr_height) = mem::replace(
                        &mut *(**curr_node).get_pointer_mut(curr_height),
                        new_node,
                    );
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            ret
        }
    }

    /// Removes a key-value pair from the map. If the key exists in the map, it will return the
    /// associated key-value pair. Otherwise it will return `None`.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.remove(&1), Some((1, 1)));
    /// assert_eq!(map.remove(&1), None);
    /// ```
    pub fn remove(&mut self, key: &T) -> Option<(T, U)> {
        let mut curr_height = MAX_HEIGHT;
        let mut curr_node = &mut self.head;
        let mut ret = None;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer_mut(curr_height);
                while !next_node.is_null() && (**next_node).entry.key < *key {
                    curr_node = mem::replace(
                        &mut next_node,
                        (**next_node).get_pointer_mut(curr_height),
                    );
                }

                if !next_node.is_null() && (**next_node).entry.key == *key {
                    let temp = *next_node;
                    *(**curr_node).get_pointer_mut(curr_height) = *(**next_node).get_pointer_mut(curr_height);
                    if curr_height == 0 {
                        ret = Some((
                            ptr::read(&(*temp).entry.key),
                            ptr::read(&(*temp).entry.value),
                        ));
                        Node::deallocate(temp);
                        self.len -= 1;
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

    /// Checks if a key exists in the map.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// assert!(!map.contains_key(&0));
    /// assert!(map.contains_key(&1));
    /// ```
    pub fn contains_key(&self, key: &T) -> bool {
        self.get(key).is_some()
    }

    /// Returns an immutable reference to the value associated with a particular key. It will
    /// return `None` if the key does not exist in the map.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.get(&0), None);
    /// assert_eq!(map.get(&1), Some(&1));
    /// ```
    pub fn get(&self, key: &T) -> Option<&U> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() && (**next_node).entry.key < *key {
                    curr_node = mem::replace(
                        &mut next_node,
                        (**next_node).get_pointer(curr_height),
                    );
                }

                if !next_node.is_null() && (**next_node).entry.key == *key {
                    return Some(&(**next_node).entry.value);
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            None
        }
    }

    /// Returns a mutable reference to the value associated with a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// *map.get_mut(&1).unwrap() = 2;
    /// assert_eq!(map.get(&1), Some(&2));
    /// ```
    pub fn get_mut(&mut self, key: &T) -> Option<&mut U> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &mut self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer_mut(curr_height);
                while !next_node.is_null() && (**next_node).entry.key < *key {
                    curr_node = mem::replace(
                        &mut next_node,
                        (**next_node).get_pointer_mut(curr_height),
                    );
                }

                if !next_node.is_null() && (**next_node).entry.key == *key {
                    return Some(&mut (**next_node).entry.value);
                }

                if curr_height == 0 {
                    break;
                }

                curr_height -= 1;
            }
            None
        }
    }

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let map: SkipMap<u32, u32> = SkipMap::new();
    /// assert!(map.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clears the map, removing all values.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    /// map.clear();
    /// assert_eq!(map.is_empty(), true);
    /// ```
    pub fn clear(&mut self) {
        self.len = 0;
        unsafe {
            let mut curr_node = *(*self.head).get_pointer(0);
            while !curr_node.is_null() {
                Node::free(mem::replace(&mut curr_node, *(*curr_node).get_pointer(0)));
            }
            ptr::write_bytes((*self.head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);
        }
    }

    /// Returns a key in the map that is less than or equal to a particular key. Returns `None` if
    /// such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.floor(&0), None);
    /// assert_eq!(map.floor(&2), Some(&1));
    /// ```
    pub fn floor(&self, key: &T) -> Option<&T> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() && (**next_node).entry.key <= *key {
                    curr_node = mem::replace(
                        &mut next_node,
                        (**next_node).get_pointer(curr_height),
                    );
                }

                if curr_height == 0 {
                    if curr_node == &self.head {
                        return None;
                    } else {
                        return Some(&(**curr_node).entry.key);
                    }
                }

                curr_height -= 1;
            }
        }
    }

    /// Returns a key in the map that is greater than or equal to a particular key. Returns `None`
    /// if such a key does not exist.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// assert_eq!(map.ceil(&0), Some(&1));
    /// assert_eq!(map.ceil(&2), None);
    /// ```
    pub fn ceil(&self, key: &T) -> Option<&T> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() && (**next_node).entry.key < *key {
                    curr_node = mem::replace(
                        &mut next_node,
                        (**next_node).get_pointer(curr_height),
                    );
                }

                if curr_height == 0 {
                    if next_node.is_null() {
                        return None;
                    } else {
                        return Some(&(**next_node).entry.key);
                    }
                }

                curr_height -= 1;
            }
        }
    }

    /// Returns the minimum key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// map.insert(3, 3);
    /// assert_eq!(map.min(), Some(&1));
    /// ```
    pub fn min(&self) -> Option<&T> {
        unsafe {
            let min_node = (*self.head).get_pointer(0);
            if min_node.is_null() {
                None
            } else {
                Some(&(**min_node).entry.key)
            }
        }
    }

    /// Returns the maximum key of the map. Returns `None` if the map is empty.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// map.insert(3, 3);
    /// assert_eq!(map.max(), Some(&3));
    /// ```
    pub fn max(&self) -> Option<&T> {
        let mut curr_height = self.get_starting_height();
        let mut curr_node = &self.head;

        unsafe {
            loop {
                let mut next_node = (**curr_node).get_pointer(curr_height);
                while !next_node.is_null() {
                    curr_node = mem::replace(
                        &mut next_node,
                        (**next_node).get_pointer(curr_height),
                    );
                }

                if curr_height == 0 {
                    if curr_node == &self.head {
                        return None;
                    } else {
                        return Some(&(**curr_node).entry.key);
                    };
                }

                curr_height -= 1;
            }
        }
    }

    /// Returns the union of two maps. If there is a key that is found in both `left` and `right`,
    /// the union will contain the value associated with the key in `left`. The `+`
    /// operator is implemented to take the union of two maps.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut n = SkipMap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = SkipMap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let union = SkipMap::union(n, m);
    /// assert_eq!(
    ///     union.iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1), (&2, &2), (&3, &3)],
    /// );
    /// ```
    pub fn union(mut left: Self, mut right: Self) -> Self {
        let mut ret = SkipMap {
            head: unsafe { Node::allocate(MAX_HEIGHT + 1) },
            rng: XorShiftRng::new_unseeded(),
            len: 0,
        };
        let mut curr_nodes = [ret.head; MAX_HEIGHT + 1];

        unsafe {
            let left_head = mem::replace(&mut left.head, *(*left.head).get_pointer(0));
            let right_head = mem::replace(&mut right.head, *(*right.head).get_pointer(0));
            ptr::write_bytes((*left_head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);
            ptr::write_bytes((*right_head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);

            loop {
                let next_node;
                match (left.head.is_null(), right.head.is_null()) {
                    (true, true) => break,
                    (false, false) => {
                        let cmp = (*left.head).entry.cmp(&(*right.head).entry);
                        match cmp {
                            cmp::Ordering::Equal => {
                                Node::free(mem::replace(
                                    &mut right.head,
                                    *(*right.head).get_pointer(0),
                                ));
                                continue;
                            },
                            cmp::Ordering::Less => next_node = mem::replace(&mut left.head, *(*left.head).get_pointer(0)),
                            cmp::Ordering::Greater => next_node = mem::replace(&mut right.head, *(*right.head).get_pointer(0)),
                        }
                    },
                    (true, false) => next_node = mem::replace(&mut right.head, *(*right.head).get_pointer(0)),
                    (false, true) => next_node = mem::replace(&mut left.head, *(*left.head).get_pointer(0)),
                }
                ret.len += 1;

                ptr::write_bytes((*next_node).links.get_unchecked_mut(0), 0, (*next_node).links_len);
                for i in 0..(*next_node).links_len {
                    *(*curr_nodes[i]).get_pointer_mut(i) = next_node;
                    curr_nodes[i] = next_node;
                }
            }
            left.head = left_head;
            right.head = right_head;
        }
        ret
    }

    /// Returns the intersection of two maps. If there is a key that is found in both `left` and
    /// `right`, the intersection will contain the value associated with the key in `left`.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut n = SkipMap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = SkipMap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let intersection = SkipMap::intersection(n, m);
    /// assert_eq!(
    ///     intersection.iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&2, &2)],
    /// );
    /// ```
    pub fn intersection(mut left: Self, mut right: Self) -> Self {
        let mut ret = SkipMap {
            head: unsafe { Node::allocate(MAX_HEIGHT + 1) },
            rng: XorShiftRng::new_unseeded(),
            len: 0,
        };
        let mut curr_nodes = [ret.head; MAX_HEIGHT + 1];

        unsafe {
            let left_head = mem::replace(&mut left.head, *(*left.head).get_pointer(0));
            let right_head = mem::replace(&mut right.head, *(*right.head).get_pointer(0));
            ptr::write_bytes((*left_head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);
            ptr::write_bytes((*right_head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);

            loop {
                let next_node;
                match (left.head.is_null(), right.head.is_null()) {
                    (true, true) => break,
                    (false, false) => {
                        let cmp = (*left.head).entry.cmp(&(*right.head).entry);
                        match cmp {
                            cmp::Ordering::Equal => {
                                next_node = mem::replace(&mut left.head, *(*left.head).get_pointer(0));
                                Node::free(mem::replace(&mut right.head, *(*right.head).get_pointer(0)));
                            },
                            cmp::Ordering::Less => {
                                Node::free(mem::replace(
                                    &mut left.head,
                                    *(*left.head).get_pointer(0),
                                ));
                                continue;
                            },
                            cmp::Ordering::Greater => {
                                Node::free(mem::replace(
                                    &mut right.head,
                                    *(*right.head).get_pointer(0),
                                ));
                                continue;
                            },
                        }
                    },
                    (true, false) => {
                        Node::free(mem::replace(&mut right.head, *(*right.head).get_pointer(0)));
                        continue;
                    },
                    (false, true) => {
                        Node::free(mem::replace(&mut left.head, *(*left.head).get_pointer(0)));
                        continue;
                    },
                }
                ret.len += 1;

                ptr::write_bytes((*next_node).links.get_unchecked_mut(0), 0, (*next_node).links_len);
                for i in 0..(*next_node).links_len {
                    *(*curr_nodes[i]).get_pointer_mut(i) = next_node;
                    curr_nodes[i] = next_node;
                }
            }
            left.head = left_head;
            right.head = right_head;
        }
        ret
    }

    fn map_difference(mut left: Self, mut right: Self, symmetric: bool) -> Self {
        let mut ret = SkipMap {
            head: unsafe { Node::allocate(MAX_HEIGHT + 1) },
            rng: XorShiftRng::new_unseeded(),
            len: 0,
        };
        let mut curr_nodes = [ret.head; MAX_HEIGHT + 1];

        unsafe {
            let left_head = mem::replace(&mut left.head, *(*left.head).get_pointer(0));
            let right_head = mem::replace(&mut right.head, *(*right.head).get_pointer(0));
            ptr::write_bytes((*left_head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);
            ptr::write_bytes((*right_head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);

            loop {
                let next_node;
                match (left.head.is_null(), right.head.is_null()) {
                    (true, true) => break,
                    (false, false) => {
                        let cmp = (*left.head).entry.cmp(&(*right.head).entry);
                        match cmp {
                            cmp::Ordering::Equal => {
                                Node::free(mem::replace(
                                    &mut left.head,
                                    *(*left.head).get_pointer(0),
                                ));
                                Node::free(mem::replace(
                                    &mut right.head,
                                    *(*right.head).get_pointer(0),
                                ));
                                continue;
                            },
                            cmp::Ordering::Less => next_node = mem::replace(&mut left.head, *(*left.head).get_pointer(0)),
                            cmp::Ordering::Greater => {
                                if symmetric {
                                    next_node = mem::replace(
                                        &mut right.head,
                                        *(*right.head).get_pointer(0),
                                    );
                                } else {
                                    Node::free(mem::replace(
                                        &mut right.head,
                                        *(*right.head).get_pointer(0),
                                    ));
                                    continue;
                                }
                            },
                        }
                    },
                    (true, false) => {
                        if symmetric {
                            next_node = mem::replace(
                                &mut right.head,
                                *(*right.head).get_pointer(0),
                            );
                        } else {
                            Node::free(mem::replace(
                                &mut right.head,
                                *(*right.head).get_pointer(0),
                            ));
                            continue;
                        }
                    },
                    (false, true) => {
                        next_node = mem::replace(
                            &mut right.head,
                            *(*right.head).get_pointer(0),
                        );
                    },
                }
                ret.len += 1;

                ptr::write_bytes((*next_node).links.get_unchecked_mut(0), 0, (*next_node).links_len);
                for i in 0..(*next_node).links_len {
                    *(*curr_nodes[i]).get_pointer_mut(i) = next_node;
                    curr_nodes[i] = next_node;
                }
            }
            left.head = left_head;
            right.head = right_head;
        }
        ret
    }

    /// Returns the difference of `left` and `right`. The returned map will contain all entries
    /// that do not have a key in `right`. The `-` operator is implemented to take the difference
    /// of two maps.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut n = SkipMap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = SkipMap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let difference = SkipMap::difference(n, m);
    /// assert_eq!(
    ///     difference.iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1)],
    /// );
    /// ```
    pub fn difference(left: Self, right: Self) -> Self {
        Self::map_difference(left, right, false)
    }

    /// Returns the symmetric difference of `left` and `right`. The returned map will contain all
    /// entries that exist in one map, but not both maps.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut n = SkipMap::new();
    /// n.insert(1, 1);
    /// n.insert(2, 2);
    ///
    /// let mut m = SkipMap::new();
    /// m.insert(2, 3);
    /// m.insert(3, 3);
    ///
    /// let symmetric_difference = SkipMap::symmetric_difference(n, m);
    /// assert_eq!(
    ///     symmetric_difference.iter().collect::<Vec<(&u32, &u32)>>(),
    ///     vec![(&1, &1), (&3, &3)],
    /// );
    /// ```
    pub fn symmetric_difference(left: Self, right: Self) -> Self {
        Self::map_difference(left, right, true)
    }

    /// Returns an iterator over the map. The iterator will yield key-value pairs in ascending
    /// order.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    ///
    /// let mut iterator = map.iter();
    /// assert_eq!(iterator.next(), Some((&1, &1)));
    /// assert_eq!(iterator.next(), Some((&2, &2)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter(&self) -> SkipMapIter<T, U> {
        unsafe {
            SkipMapIter {
                current: &*(*self.head).get_pointer(0),
            }
        }
    }

    /// Returns a mutable iterator over the map. The iterator will yield key-value pairs in
    /// ascending order.
    ///
    /// # Examples
    /// ```
    /// use extended_collections::skiplist::SkipMap;
    ///
    /// let mut map = SkipMap::new();
    /// map.insert(1, 1);
    /// map.insert(2, 2);
    ///
    /// for (key, value) in &mut map {
    ///     *value += 1;
    /// }
    ///
    /// let mut iterator = map.iter_mut();
    /// assert_eq!(iterator.next(), Some((&1, &mut 2)));
    /// assert_eq!(iterator.next(), Some((&2, &mut 3)));
    /// assert_eq!(iterator.next(), None);
    /// ```
    pub fn iter_mut(&self) -> SkipMapIterMut<T, U> {
        unsafe {
            SkipMapIterMut {
                current: &mut *(*self.head).get_pointer_mut(0),
            }
        }
    }
}

impl<T, U> Drop for SkipMap<T, U>
where T: Ord
{
    fn drop(&mut self) {
        unsafe {
            Node::deallocate(mem::replace(&mut self.head, *(*self.head).get_pointer(0)));
            while !self.head.is_null() {
                Node::free(mem::replace(&mut self.head, *(*self.head).get_pointer(0)));
            }
        }
    }
}

impl<T, U> IntoIterator for SkipMap<T, U>
where T: Ord
{
    type Item = (T, U);
    type IntoIter = SkipMapIntoIter<T, U>;

    fn into_iter(self) -> Self::IntoIter {
        unsafe {
            let ret = Self::IntoIter {
                current: *(*self.head).links.get_unchecked_mut(0),
            };
            ptr::write_bytes((*self.head).links.get_unchecked_mut(0), 0, MAX_HEIGHT + 1);
            ret
        }
    }
}

impl<'a, T, U> IntoIterator for &'a SkipMap<T, U>
where
    T: 'a + Ord,
    U: 'a,
{
    type Item = (&'a T, &'a U);
    type IntoIter = SkipMapIter<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T, U> IntoIterator for &'a mut SkipMap<T, U>
where
    T: 'a + Ord,
    U: 'a,
{
    type Item = (&'a T, &'a mut U);
    type IntoIter = SkipMapIterMut<'a, T, U>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// An owning iterator for `SkipMap<T, U>`.
///
/// This iterator traverses the elements of a map in ascending order and yields owned entries.
pub struct SkipMapIntoIter<T, U>
where T: Ord
{
    current: *mut Node<T, U>,
}

impl<T, U> Iterator for SkipMapIntoIter<T, U>
where T: Ord
{
    type Item = (T, U);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            None
        } else {
            unsafe {
                let Entry { key, value } = ptr::read(&(*self.current).entry);
                Node::deallocate(mem::replace(
                    &mut self.current,
                    *(*self.current).get_pointer(0),
                ));
                Some((key, value))
            }
        }
    }
}

impl<T, U> Drop for SkipMapIntoIter<T, U>
where T: Ord
{
    fn drop(&mut self) {
        unsafe {
            while !self.current.is_null() {
                ptr::drop_in_place(&mut (*self.current).entry);
                Node::free(mem::replace(
                    &mut self.current,
                    *(*self.current).get_pointer(0),
                ));
            }
        }
    }
}

/// An iterator for `SkipMap<T, U>`.
///
/// This iterator traverses the elements of a map in ascending order and yields immutable
/// references.
pub struct SkipMapIter<'a, T, U>
where
    T: 'a + Ord,
    U: 'a,
{
    current: &'a *mut Node<T, U>,
}

impl<'a, T, U> Iterator for SkipMapIter<'a, T, U>
where
    T: 'a + Ord,
    U: 'a,
{
    type Item = (&'a T, &'a U);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            None
        } else {
            unsafe {
                let Entry { ref key, ref value } = (**self.current).entry;
                mem::replace(&mut self.current, &*(**self.current).get_pointer(0));
                Some((key, value))
            }
        }
    }
}

/// A mutable iterator for `SkipMap<T, U>`.
///
/// This iterator traverses the elements of a map in ascending order and yields mutable references.
pub struct SkipMapIterMut<'a, T, U>
where
    T: 'a + Ord,
    U: 'a,
{
    current: &'a mut *mut Node<T, U>,
}

impl<'a, T, U> Iterator for SkipMapIterMut<'a, T, U>
where
    T: 'a + Ord,
    U: 'a,
{
    type Item = (&'a T, &'a mut U);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            None
        } else {
            unsafe {
                let Entry { ref key, ref mut value } = (**self.current).entry;
                mem::replace(&mut self.current, &mut *(**self.current).get_pointer_mut(0));
                Some((key, value))
            }
        }
    }
}

impl<T, U> Default for SkipMap<T, U>
where T: Ord
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, U> Add for SkipMap<T, U>
where T: Ord
{
    type Output = SkipMap<T, U>;

    fn add(self, other: SkipMap<T, U>) -> SkipMap<T, U> {
        Self::union(self, other)
    }
}

impl<T, U> Sub for SkipMap<T, U>
where T: Ord
{
    type Output = SkipMap<T, U>;

    fn sub(self, other: SkipMap<T, U>) -> SkipMap<T, U> {
        Self::difference(self, other)
    }
}

impl<'a, T, U> Index<&'a T> for SkipMap<T, U>
where T: Ord
{
    type Output = U;
    fn index(&self, key: &T) -> &Self::Output {
        self.get(key).expect("Key does not exist.")
    }
}

impl<'a, T, U> IndexMut<&'a T> for SkipMap<T, U>
where T: Ord
{
    fn index_mut(&mut self, key: &T) -> &mut Self::Output {
        self.get_mut(key).expect("Key does not exist.")
    }
}

#[cfg(test)]
mod tests {
    use super::SkipMap;

    #[test]
    fn test_len_empty() {
        let map: SkipMap<u32, u32> = SkipMap::new();
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_is_empty() {
        let map: SkipMap<u32, u32> = SkipMap::new();
        assert!(map.is_empty());
    }

    #[test]
    fn test_min_max_empty() {
        let map: SkipMap<u32, u32> = SkipMap::new();
        assert_eq!(map.min(), None);
        assert_eq!(map.max(), None);
    }

    #[test]
    fn test_insert() {
        let mut map = SkipMap::new();
        assert_eq!(map.insert(1, 1), None);
        assert!(map.contains_key(&1));
        assert_eq!(map.get(&1), Some(&1));
    }

    #[test]
    fn test_insert_replace() {
        let mut map = SkipMap::new();
        assert_eq!(map.insert(1, 1), None);
        assert_eq!(map.insert(1, 3), Some((1, 1)));
        assert_eq!(map.get(&1), Some(&3));
    }

    #[test]
    fn test_remove() {
        let mut map = SkipMap::new();
        map.insert(1, 1);
        assert_eq!(map.remove(&1), Some((1, 1)));
        assert!(!map.contains_key(&1));
    }

    #[test]
    fn test_min_max() {
        let mut map = SkipMap::new();
        map.insert(1, 1);
        map.insert(3, 3);
        map.insert(5, 5);

        assert_eq!(map.min(), Some(&1));
        assert_eq!(map.max(), Some(&5));
    }

    #[test]
    fn test_get_mut() {
        let mut map = SkipMap::new();
        map.insert(1, 1);
        {
            let value = map.get_mut(&1);
            *value.unwrap() = 3;
        }
        assert_eq!(map.get(&1), Some(&3));
    }

    #[test]
    fn test_floor_ceil() {
        let mut map = SkipMap::new();
        map.insert(1, 1);
        map.insert(3, 3);
        map.insert(5, 5);

        assert_eq!(map.floor(&0), None);
        assert_eq!(map.floor(&2), Some(&1));
        assert_eq!(map.floor(&4), Some(&3));
        assert_eq!(map.floor(&6), Some(&5));

        assert_eq!(map.ceil(&0), Some(&1));
        assert_eq!(map.ceil(&2), Some(&3));
        assert_eq!(map.ceil(&4), Some(&5));
        assert_eq!(map.ceil(&6), None);
    }

    #[test]
    fn test_union() {
        let mut n = SkipMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = SkipMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let union = n + m;

        assert_eq!(
            union.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2), (&3, &3), (&4, &4), (&5, &5)],
        );
        assert_eq!(union.len(), 5);
    }

    #[test]
    fn test_intersection() {
        let mut n = SkipMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = SkipMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let intersection = SkipMap::intersection(n, m);

        assert_eq!(
            intersection.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&3, &3)],
        );
        assert_eq!(intersection.len(), 1);
    }

    #[test]
    fn test_difference() {
        let mut n = SkipMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = SkipMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let difference = n - m;

        assert_eq!(
            difference.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2)],
        );
        assert_eq!(difference.len(), 2);
    }

    #[test]
    fn test_symmetric_difference() {
        let mut n = SkipMap::new();
        n.insert(1, 1);
        n.insert(2, 2);
        n.insert(3, 3);

        let mut m = SkipMap::new();
        m.insert(3, 5);
        m.insert(4, 4);
        m.insert(5, 5);

        let symmetric_difference = SkipMap::symmetric_difference(n, m);

        assert_eq!(
            symmetric_difference.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &1), (&2, &2), (&4, &4), (&5, &5)],
        );
        assert_eq!(symmetric_difference.len(), 4);
    }

    #[test]
    fn test_into_iter() {
        let mut map = SkipMap::new();
        map.insert(1, 2);
        map.insert(5, 6);
        map.insert(3, 4);

        assert_eq!(
            map.into_iter().collect::<Vec<(u32, u32)>>(),
            vec![(1, 2), (3, 4), (5, 6)],
        );
    }

    #[test]
    fn test_iter() {
        let mut map = SkipMap::new();
        map.insert(1, 2);
        map.insert(5, 6);
        map.insert(3, 4);

        assert_eq!(
            map.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &2), (&3, &4), (&5, &6)],
        );
    }

    #[test]
    fn test_iter_mut() {
        let mut map = SkipMap::new();
        map.insert(1, 2);
        map.insert(5, 6);
        map.insert(3, 4);

        for (_, value) in &mut map {
            *value += 1;
        }

        assert_eq!(
            map.iter().collect::<Vec<(&u32, &u32)>>(),
            vec![(&1, &3), (&3, &5), (&5, &7)],
        );
    }
}
