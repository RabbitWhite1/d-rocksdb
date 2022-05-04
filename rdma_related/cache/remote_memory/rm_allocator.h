#pragma once

#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>

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
 public:
  enum {
    NOSPACE = 0,
    NUM_STATUS = -1,
  };

  virtual ~RemoteMemoryAllocator(){};

  virtual void init(uint64_t addr, size_t size) = 0;
  virtual RMRegion *rmalloc(size_t size) = 0;
  virtual size_t rmfree(RMRegion *rm_region) = 0;
  virtual void print_size_info() = 0;
  virtual void print(bool only_free = false) = 0;
};
}  // namespace ROCKSDB_NAMESPACE