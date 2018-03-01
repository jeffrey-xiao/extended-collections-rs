use std::cmp::Ordering;

pub struct Entry<T: Ord, U> {
    pub key: T,
    pub value: U,
}

impl<T: Ord, U> Ord for Entry<T, U> {
    fn cmp(&self, other: &Entry<T, U>) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<T: Ord, U> PartialOrd for Entry<T, U> {
    fn partial_cmp(&self, other: &Entry<T, U>) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<T: Ord, U> PartialEq for Entry<T, U> {
    fn eq(&self, other: &Entry<T, U>) -> bool {
        self.key == other.key
    }
}

impl<T: Ord, U> Eq for Entry<T, U> {}
