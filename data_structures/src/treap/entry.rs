use std::cmp::Ordering;

pub trait Entry {
    type Output: Ord;
    fn get_key(&self) -> &Self::Output;
}

pub struct PairEntry<T: Ord, U> {
    pub key: T,
    pub value: U,
}

impl<T: Ord, U> Ord for PairEntry<T, U> {
    fn cmp(&self, other: &PairEntry<T, U>) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<T: Ord, U> PartialOrd for PairEntry<T, U> {
    fn partial_cmp(&self, other: &PairEntry<T, U>) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<T: Ord, U> PartialEq for PairEntry<T, U> {
    fn eq(&self, other: &PairEntry<T, U>) -> bool {
        self.key == other.key
    }
}

impl<T: Ord, U> Eq for PairEntry<T, U> {}

impl<T: Ord, U> Entry for PairEntry<T, U> {
    type Output = T;
    fn get_key(&self) -> &Self::Output {
        &self.key
    }
}

impl<'a, T: Ord, U> Entry for &'a PairEntry<T, U> {
    type Output = T;
    fn get_key(&self) -> &Self::Output {
        &self.key
    }
}

pub struct UnitEntry<T: Ord>(pub T);

impl<T: Ord> Ord for UnitEntry<T> {
    fn cmp(&self, other: &UnitEntry<T>) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T: Ord> PartialOrd for UnitEntry<T> {
    fn partial_cmp(&self, other: &UnitEntry<T>) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl<T: Ord> PartialEq for UnitEntry<T> {
    fn eq(&self, other: &UnitEntry<T>) -> bool {
        self.0 == other.0
    }
}

impl<T: Ord> Eq for UnitEntry<T> {}

impl<'a, T: Ord> Entry for UnitEntry<T> {
    type Output = T;
    fn get_key(&self) -> &Self::Output {
        &self.0
    }
}

impl<'a, T: Ord> Entry for &'a UnitEntry<T> {
    type Output = T;
    fn get_key(&self) -> &Self::Output {
        &self.0
    }
}
