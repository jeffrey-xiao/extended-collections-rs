use cuckoo::CuckooFilter;
use std::hash::Hash;

pub struct ScalableCuckooFilter<T: Hash> {
    filters: Vec<CuckooFilter<T>>,
    initial_fpp: f64,
    growth_ratio: f64,
    tightening_ratio: f64,
}

impl<T: Hash> ScalableCuckooFilter<T> {
    pub fn from_entries_per_index(
        item_count: usize,
        fpp: f64,
        entries_per_index: usize,
        growth_ratio: f64,
        tightening_ratio: f64,
    ) -> Self {
        ScalableCuckooFilter {
            filters: vec![CuckooFilter::from_entries_per_index(item_count, fpp, entries_per_index)],
            initial_fpp: fpp,
            growth_ratio,
            tightening_ratio,
        }
    }
}
