#pragma once

#include <atomic>
#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "cache/remote_memory/rm_allocator.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class FFBasedRemoteMemoryAllocator : public RemoteMemoryAllocator {
 private:
  std::mutex mutex_;

  uint64_t rm_addr_;
  size_t rm_size_;

  RMRegion *head_;
  RMRegion *free_head_;

  RMRegion *split_and_use_region(RMRegion *region, size_t first_size);
  RMRegion *extract_from_free_list(RMRegion *region);
  RMRegion *prepend_free(RMRegion *region);

 public:
  enum {
    NOSPACE = 0,
    NUM_STATUS = -1,
  };
  FFBasedRemoteMemoryAllocator();
  ~FFBasedRemoteMemoryAllocator();

  void init(uint64_t addr, size_t size);
  RMRegion *rmalloc(size_t size);
  size_t rmfree(RMRegion *region);
  void print_size_info();
  void print(bool only_free = false);
};
}  // namespace ROCKSDB_NAMESPACE