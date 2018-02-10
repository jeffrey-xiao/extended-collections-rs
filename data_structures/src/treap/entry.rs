use std::cmp::Ordering;

pub trait Entry {
    type Output: Ord;
    fn get_key(&self) -> &Self::Output;
}

pub struct MapEntry<T: Ord, U> {
    pub key: T,
    pub value: U,
}

impl<T: Ord, U> Ord for MapEntry<T, U> {
    fn cmp(&self, other: &MapEntry<T, U>) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<T: Ord, U> PartialOrd for MapEntry<T, U> {
    fn partial_cmp(&self, other: &MapEntry<T, U>) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<T: Ord, U> PartialEq for MapEntry<T, U> {
    fn eq(&self, other: &MapEntry<T, U>) -> bool {
        self.key == other.key
    }
}

impl<T: Ord, U> Eq for MapEntry<T, U> {}

impl<T: Ord, U> Entry for MapEntry<T, U> {
    type Output = T;
    fn get_key(&self) -> &Self::Output {
        &self.key
    }
}

impl<'a, T: Ord, U> Entry for &'a MapEntry<T, U> {
    type Output = T;
    fn get_key(&self) -> &Self::Output {
        &self.key
    }
}

pub struct SetEntry<T: Ord>(pub T);

impl<T: Ord> Ord for SetEntry<T> {
    fn cmp(&self, other: &SetEntry<T>) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T: Ord> PartialOrd for SetEntry<T> {
    fn partial_cmp(&self, other: &SetEntry<T>) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl<T: Ord> PartialEq for SetEntry<T> {
    fn eq(&self, other: &SetEntry<T>) -> bool {
        self.0 == other.0
    }
}

impl<T: Ord> Eq for SetEntry<T> {}

impl<'a, T: Ord> Entry for SetEntry<T> {
    type Output = T;
    fn get_key(&self) -> &Self::Output {
        &self.0
    }
}

impl<'a, T: Ord> Entry for &'a SetEntry<T> {

    type Output = T;
    fn get_key(&self) -> &Self::Output {
        &self.0
    }
}
