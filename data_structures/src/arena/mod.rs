use std::mem;
use std::vec::Vec;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Entry {
    chunk_index: usize,
    block_index: usize,
}

enum Block<T> {
    Occupied(T),
    Vacant(Option<Entry>),
}

pub struct TypedArena<T> {
    head: Option<Entry>,
    chunks: Vec<Vec<Block<T>>>,
    chunk_size: usize,
    size: usize,
    capacity: usize,
}

impl<T> TypedArena<T> {
    pub fn new(chunk_size: usize) -> Self {
        TypedArena {
            head: None,
            chunks: Vec::new(),
            chunk_size,
            size: 0,
            capacity: 0,
        }
    }

    pub fn allocate(&mut self, value: T) -> Entry {
        if self.size == self.capacity {
            self.chunks.push(Vec::with_capacity(self.chunk_size));
            self.capacity += self.chunk_size;
        }
        self.size += 1;

        match self.head.take() {
            None => {
                let chunk_count = self.chunks.len();
                let mut last_chunk = &mut self.chunks[chunk_count - 1];
                last_chunk.push(Block::Occupied(value));
                Entry {
                    chunk_index: chunk_count - 1,
                    block_index: last_chunk.len() - 1,
                }
            },
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
                    },
                    Block::Occupied(_) => unreachable!(),
                }
            },
        }
    }

    pub fn free(&mut self, entry: &Entry) -> T {
        let old_block = mem::replace(&mut self.chunks[entry.chunk_index][entry.block_index], Block::Vacant(self.head.take()));
        match old_block {
            Block::Vacant(_) => panic!("Attemping to free vacant block"),
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

    pub fn get(&self, entry: &Entry) -> &T {
        match self.chunks[entry.chunk_index][entry.block_index] {
            Block::Occupied(ref value) => value,
            Block::Vacant(_) => panic!("Value does not exist"),
        }
    }

    pub fn get_mut(&mut self, entry: &Entry) -> &mut T {
        match self.chunks[entry.chunk_index][entry.block_index] {
            Block::Occupied(ref mut value) => value,
            Block::Vacant(_) => panic!("Value does not exist"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TypedArena;
    use super::Entry;

    #[test]
    fn test_insert() {
        let mut pool = TypedArena::new(1024);
        assert_eq!(pool.allocate(0), Entry { chunk_index: 0, block_index: 0 });
        assert_eq!(pool.allocate(0), Entry { chunk_index: 0, block_index: 1 });
        assert_eq!(pool.allocate(0), Entry { chunk_index: 0, block_index: 2 });
    }

    #[test]
    fn test_insert_multiple_chunks() {
        let mut pool = TypedArena::new(2);
        assert_eq!(pool.allocate(0), Entry { chunk_index: 0, block_index: 0 });
        assert_eq!(pool.allocate(0), Entry { chunk_index: 0, block_index: 1 });
        assert_eq!(pool.allocate(0), Entry { chunk_index: 1, block_index: 0 });
    }

    #[test]
    fn test_remove() {
        let mut pool = TypedArena::new(1024);
        let entry = pool.allocate(0);
        assert_eq!(entry, Entry { chunk_index: 0, block_index: 0 });
        assert_eq!(pool.free(&entry), 0);
        assert_eq!(pool.allocate(0), entry);
    }

    #[test]
    fn test_get() {
        let mut pool = TypedArena::new(1024);
        let entry = pool.allocate(0);
        assert_eq!(pool.get(&entry), &0);
    }

    #[test]
    fn test_get_mut() {
        let mut pool = TypedArena::new(1024);
        let entry = pool.allocate(0);
        *pool.get_mut(&entry) = 1;
        assert_eq!(pool.get(&entry), &1);
    }
}
