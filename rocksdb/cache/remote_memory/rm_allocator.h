#pragma once

#include <iostream>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <cassert>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct RMRegion {
  uint64_t addr;
  size_t size;
  bool is_free;

  struct RMRegion *prev;
  struct RMRegion *next;

  struct RMRegion *next_free;
  struct RMRegion *prev_free;

  void print() {
    if (is_free) {
      printf("{[0x%lx, 0x%lx), 0x%lu, free}", addr, addr + size, size);
    } else {
      printf("{[0x%lx, 0x%lx), 0x%lu, used}", addr, addr + size, size);
    }
  }
};

class RemoteMemoryAllocator {
 private:
  std::mutex mutex_;

  uint64_t rm_addr_;
  size_t rm_size_;

  RMRegion *head_;
  RMRegion *free_head_;

  std::unordered_map<uint64_t, RMRegion *> addr_to_region_;

  RMRegion *split_and_use_region(RMRegion *region, size_t first_size);
  RMRegion *extract_from_free_list(RMRegion *region);
  RMRegion *prepend_free(RMRegion *region);

 public:
  enum {
    NOSPACE = 0,
    NUM_STATUS = -1,
  };
  RemoteMemoryAllocator(uint64_t addr, size_t size);
  ~RemoteMemoryAllocator();

  uint64_t rmalloc(size_t size);
  size_t rmfree(uint64_t addr);
  void print_size_info();
  void print();
};
}  // namespace ROCKSDB_NAMESPACE