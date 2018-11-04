# Changelog

## 0.6.0 - 2018-09-11

### Added

 - `splay_tree` module with `SplayMap`, and `SplaySet`.
 - `red_black_tree` module with `RedBlackMap`, and `RedBlackSet`.

### Changed

 - Abstract more logic from `radix::tree` to `radix::Node`.
 - Refactor rotate for `avl_tree::tree`, and `splay_tree::tree`.

## 0.5.0 - 2018-09-08

### Changed

 - APU uses `Borrow` instead of `&` where applicable.
 - Refactor `where` statements.

## 0.4.0 - 2018-09-06

### Removed

 - Move `bloom`, and `cuckoo` to a new crate:
   [`probabilistic-collections`](https://crates.io/crates/probabilistic-collections).

### Fixed

 - Minor clippy warning fixes.

## 0.3.1 - 2018-09-05

### Fixed

 - Fix AVL node update.

## 0.3.0 - 2018-09-05

### Added

 - `avl_tree` module with `AvlMap`, and `AvlSet`.

## 0.2.0 - 2018-05-23

### Added

 - `lsm_tree` module with `LsmMap`.
 - Implement `Serialize` and `Deserialize` for `BloomFilter`.
 - Add trait bounds to items in `cuckoo`, and `bloom`.

### Changed

 - Rename `bptree` to `bp_tree`.
 - Update documentation and formatting.
 - More permissive trait bounds.
 - More consistent error messages and replace `unwrap` with `expect`.

## 0.1.0 - 2018-04-05

### Added

 - `TypedArena`.
 - `bloom` module with `BloomFilter`, `BSBloomFilter`, `BSSDBloomFilter`, `RLBSBloomFilter`,
   `PartitionedBloomFilter`, and `ScalableBloomFilter`.
 - `bptree` module with `BPMap`.
 - `cuckoo` module with `CuckooFilter`, and `ScalableCuckooFilter`.
 - `hyperloglog` module with `HyperLogLog`.
 - `radix` module with `RadixMap`, and `RadixSet`.
 - `skiplist` module with `SkipMap`, and `SkipSet`.
 - `sync` module with `Stack`.
 - `treap` module with `TreapList`, `TreapMap`, and `TreapSet`.
