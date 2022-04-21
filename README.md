# Disaggregated RocksDB

## TODO

- [x] 1. a simple remote memory allocator
- [x] 2. copy and tidy up the LRUCache codes and create RMLRUCache, remove Secondary Cache Logics for convenient
- [ ] 3. a simple rdma server or interface for convenient fetching and storing operations.
  - [x] a server handling control message and cm events
  - [x] a client that has one qp to write/read remote memory 
  - [x] unit test above
  - [ ] [Optional] a client that supports multiple queue pairs for better maybe performance.
  - [ ] [Optional] unit test above
- [x] 4. embed the rdma interface into rocksdb
- [ ] 5. implement the remote memory logic for LRUCache.
  - [x] LRUHandle. modify its fields to support below operations
  - [ ] rm_lru. implement rm_lru related methods (the simplest lru)
  - [ ] eviction. 
    - [ ] evict local block to remote if exceeding local memory
    - [ ] evict remtote block if exceeding total memory
  - [ ] fetch if remote
