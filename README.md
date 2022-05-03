# Disaggregated RocksDB

## TODO

### D-RocksDB

#### Intuitive Version

- [x] 1. a simple remote memory allocator
- [x] 2. copy and tidy up the LRUCache codes and create RMLRUCache, remove Secondary Cache Logics for convenient
- [x] 3. a simple rdma server or interface for convenient fetching and storing operations.
  - [x] a server handling control message and cm events
  - [x] a client that has one qp to write/read remote memory 
  - [x] unit test above
- [x] 4. embed the rdma interface into rocksdb
- [x] 5. implement the remote memory logic for LRUCache.
  - [x] LRUHandle. modify its fields to support below operations
  - [x] rm_lru. implement rm_lru related methods (the simplest lru)
  - [x] eviction. 
    - [x] evict local block to remote if exceeding local memory
    - [x] evict remtote block if exceeding total memory
      - [x] shard remote memory so that any shard can control its own rm (otherwise, it may fail when allocate a space but memory is framented by other shards)
  - [x] fetch if remote
  - [x] statistics about the remote memory
    - [x] count of hit in rm/hit in lm
    - [x] time of hit in rm/hit in lm (or rm overhead)
    - [x] stats of lm/rm usage (through `GetMapProperty`)

#### Improved Version 0

- [ ] 1. do not promote an entry every lookup.

### YCSB

- [ ] support configuration of using `d_lru_cache` or normal `lru_cache`
- [x] support configuration of using `rm_ratio`
- [x] modify value generator to use transformation of key, for easier verification of the correctness.

