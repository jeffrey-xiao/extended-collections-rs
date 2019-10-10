//! Fast, but limited allocator.

use std::mem;
use std::ops::{Index, IndexMut};
use std::vec::Vec;

/// A struct representing an entry to `TypedArena<T>`
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
    chunk_index: usize,
    block_index: usize,
}

enum Block<T> {
    Occupied(T),
    Vacant(Option<Entry>),
}

/// A fast, but limited allocator that only allocates a single type of object.
///
/// All objects inside the arena will be destroyed when the typed arena is destroyed. This typed
/// arena also supports deallocation of objects once they are allocated and yields both mutable and
/// immutable references to objects. Additionally, the underlying container is simply a `Vec` so
/// the code itself is very simple and uses no unsafe code. When the typed arena is full, it will
/// allocate another chunk of objects so no memory is reallocated.
///
/// # Examples
///
/// ```
/// use extended_collections::arena::TypedArena;
///
/// let mut arena = TypedArena::new(1024);
///
/// let x = arena.allocate(1);
/// assert_eq!(arena[x], 1);
///
/// arena[x] += 1;
/// assert_eq!(arena[x], 2);
///
/// assert_eq!(arena.free(&x), 2);
/// ```
pub struct TypedArena<T> {
    head: Option<Entry>,
    chunks: Vec<Vec<Block<T>>>,
    chunk_size: usize,
    size: usize,
    capacity: usize,
}

impl<T> TypedArena<T> {
    fn is_valid_entry(&self, entry: &Entry) -> bool {
        entry.chunk_index < self.chunks.len()
            && entry.block_index < self.chunks[entry.chunk_index].len()
    }

    /// Constructs a new, empty `TypedArena<T>` with a specific number of objects per chunk.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::arena::TypedArena;
    ///
    /// // creates a new TypedArena<T> that contains a maximum of 1024 u32's per chunk
    /// let arena: TypedArena<u32> = TypedArena::new(1024);
    /// ```
    pub fn new(chunk_size: usize) -> Self {
        TypedArena {
            head: None,
            chunks: Vec::new(),
            chunk_size,
            size: 0,
            capacity: 0,
        }
    }

    /// Allocates an object in the typed arena and returns an Entry. The Entry can later be used to
    /// index retrieve mutable and immutable references to the object, and dellocate the object.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::arena::TypedArena;
    ///
    /// let mut arena = TypedArena::new(1024);
    /// let x = arena.allocate(0);
    /// ```
    pub fn allocate(&mut self, value: T) -> Entry {
        if self.size == self.capacity {
            self.chunks.push(Vec::with_capacity(self.chunk_size));
            self.capacity += self.chunk_size;
        }
        self.size += 1;

        match self.head.take() {
            None => {
                let chunk_count = self.chunks.len();
                let last_chunk = &mut self.chunks[chunk_count - 1];
                last_chunk.push(Block::Occupied(value));
                Entry {
                    chunk_index: chunk_count - 1,
                    block_index: last_chunk.len() - 1,
                }
            }
            Some(entry) => {
                let vacant_block = mem::replace(
                    &mut self.chunks[entry.chunk_index][entry.block_index],
                    Block::Occupied(value),
                );

                match vacant_block {
                    Block::Vacant(next_entry) => {
                        let ret = entry;
                        self.head = next_entry;
                        ret
                    }
                    Block::Occupied(_) => panic!("Expected an occupied block."),
                }
            }
        }
    }

    /// Deallocates an object in the typed arena and returns the object.
    ///
    /// # Panics
    ///
    /// Panics if entry corresponds to an invalid or vacant value.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::arena::TypedArena;
    ///
    /// let mut arena = TypedArena::new(1024);
    /// let x = arena.allocate(0);
    /// assert_eq!(arena.free(&x), 0);
    /// ```
    pub fn free(&mut self, entry: &Entry) -> T {
        if !self.is_valid_entry(entry) {
            panic!("Error: attempting to free invalid block.");
        }
        let old_block = mem::replace(
            &mut self.chunks[entry.chunk_index][entry.block_index],
            Block::Vacant(self.head.take()),
        );
        match old_block {
            Block::Vacant(_) => panic!("Error: attempting to free vacant block."),
            Block::Occupied(value) => {
                self.size -= 1;
                self.head = Some(Entry {
                    chunk_index: entry.chunk_index,
                    block_index: entry.block_index,
                });
                value
            }
        }
    }

    /// Returns an immutable reference to an object in the typed arena. Returns `None` if the entry
    /// does not correspond to a valid object.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::arena::TypedArena;
    ///
    /// let mut arena = TypedArena::new(1024);
    /// let x = arena.allocate(0);
    /// assert_eq!(arena.get(&x), Some(&0));
    /// ```
    pub fn get(&self, entry: &Entry) -> Option<&T> {
        if !self.is_valid_entry(entry) {
            return None;
        }
        match self.chunks[entry.chunk_index][entry.block_index] {
            Block::Occupied(ref value) => Some(value),
            Block::Vacant(_) => None,
        }
    }

    /// Returns a mutable reference to an object in the typed arena. Returns `None` if the entry
    /// does not correspond to a valid object.
    ///
    /// # Examples
    ///
    /// ```
    /// use extended_collections::arena::TypedArena;
    ///
    /// let mut arena = TypedArena::new(1024);
    /// let x = arena.allocate(0);
    /// assert_eq!(arena.get_mut(&x), Some(&mut 0));
    /// ```
    pub fn get_mut(&mut self, entry: &Entry) -> Option<&mut T> {
        if !self.is_valid_entry(entry) {
            return None;
        }
        match self.chunks[entry.chunk_index][entry.block_index] {
            Block::Occupied(ref mut value) => Some(value),
            Block::Vacant(_) => None,
        }
    }
}

impl<T> Index<Entry> for TypedArena<T> {
    type Output = T;

    fn index(&self, entry: Entry) -> &Self::Output {
        self.get(&entry).expect("Error: entry out of bounds.")
    }
}

impl<T> IndexMut<Entry> for TypedArena<T> {
    fn index_mut(&mut self, entry: Entry) -> &mut Self::Output {
        self.get_mut(&entry).expect("Error: entry out of bounds.")
    }
}

#[cfg(test)]
mod tests {
    use super::Entry;
    use super::TypedArena;

    #[test]
    #[should_panic]
    fn test_free_invalid_block() {
        let mut arena: TypedArena<u32> = TypedArena::new(1024);
        arena.free(&Entry {
            chunk_index: 0,
            block_index: 0,
        });
    }

    #[test]
    #[should_panic]
    fn test_free_vacant_block() {
        let mut arena = TypedArena::new(1024);
        arena.allocate(0);
        arena.free(&Entry {
            chunk_index: 0,
            block_index: 1,
        });
    }

    #[test]
    fn test_insert() {
        let mut pool = TypedArena::new(1024);
        assert_eq!(
            pool.allocate(0),
            Entry {
                chunk_index: 0,
                block_index: 0
            },
        );
        assert_eq!(
            pool.allocate(0),
            Entry {
                chunk_index: 0,
                block_index: 1
            },
        );
        assert_eq!(
            pool.allocate(0),
            Entry {
                chunk_index: 0,
                block_index: 2
            },
        );
    }

    #[test]
    fn test_insert_multiple_chunks() {
        let mut pool = TypedArena::new(2);
        assert_eq!(
            pool.allocate(0),
            Entry {
                chunk_index: 0,
                block_index: 0
            },
        );
        assert_eq!(
            pool.allocate(0),
            Entry {
                chunk_index: 0,
                block_index: 1
            },
        );
        assert_eq!(
            pool.allocate(0),
            Entry {
                chunk_index: 1,
                block_index: 0
            },
        );
    }

    #[test]
    fn test_free() {
        let mut pool = TypedArena::new(1024);
        let entry = pool.allocate(0);
        assert_eq!(
            entry,
            Entry {
                chunk_index: 0,
                block_index: 0
            },
        );
        assert_eq!(pool.free(&entry), 0);
        assert_eq!(pool.allocate(0), entry);
    }

    #[test]
    fn test_get() {
        let mut pool = TypedArena::new(1024);
        let entry = pool.allocate(0);
        assert_eq!(pool.get(&entry), Some(&0));
    }

    #[test]
    fn test_get_invalid_block() {
        let pool: TypedArena<u32> = TypedArena::new(1024);
        assert_eq!(
            pool.get(&Entry {
                chunk_index: 0,
                block_index: 0
            }),
            None,
        );
    }

    #[test]
    fn test_get_vacant_block() {
        let mut pool = TypedArena::new(1024);
        pool.allocate(0);
        assert_eq!(
            pool.get(&Entry {
                chunk_index: 0,
                block_index: 1
            }),
            None,
        );
    }

    #[test]
    fn test_get_mut() {
        let mut pool = TypedArena::new(1024);
        let entry = pool.allocate(0);
        *pool.get_mut(&entry).unwrap() = 1;
        assert_eq!(pool.get(&entry), Some(&1));
    }

    #[test]
    fn test_get_mut_invalid_block() {
        let mut pool: TypedArena<u32> = TypedArena::new(1024);
        assert_eq!(
            pool.get_mut(&Entry {
                chunk_index: 0,
                block_index: 0
            }),
            None,
        );
    }

    #[test]
    fn test_get_mut_vacant_block() {
        let mut pool = TypedArena::new(1024);
        pool.allocate(0);
        assert_eq!(
            pool.get_mut(&Entry {
                chunk_index: 0,
                block_index: 1
            }),
            None,
        );
    }
}
