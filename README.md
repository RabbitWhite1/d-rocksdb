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

#### Improved Version v1.1.0

- [x] try to treat remote memory as blocks, i.e., only allocate a block for each cache block (here is an assumption that the block size can always be fit in a cache)

#### Improved Version v2.0.0

- [x] support async read/write
  - [x] modify rdma_transport to support async read/write (ignore potential race)
  - [x] modify rm to support async ops
    - [x] AsyncRequest with a buffer to recv remote value or a pointer to buffer that will be sent
  - [x] modify DLRUCache to use async ops
    - [x] do the transfering out of mutex
    - [x] invoke `wait` upon using the DLRUHandle (e.g., Lookup), and do free if necessary.
- [x] modify rm to support rdma_transport pool (avoid contention)
- [x] overlap rdma read/write as much as possible
  - [x] overlap read/write exchange in Lookup

#### Improved Version v3.0.0

- [x] local BlockBasedMemoryAllocator
- [ ] register local BlockBasedMemoryAllocator for RDMA
- [ ] directly read/write to avoid copy

### YCSB

- [ ] support configuration of using `d_lru_cache` or normal `lru_cache`
- [x] support configuration of using `rm_ratio`
- [x] modify value generator to use transformation of key, for easier verification of the correctness.

