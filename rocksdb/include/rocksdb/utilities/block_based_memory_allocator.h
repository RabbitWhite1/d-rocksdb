#pragma once

#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "rocksdb/memory_allocator.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct MemoryRegion {
  char *addr;
  size_t size;  // representing real size, however, we always allocate a block
  bool is_free;

  struct MemoryRegion *prev;
  struct MemoryRegion *next;

  struct MemoryRegion *next_free;
  struct MemoryRegion *prev_free;
};

class BlockBasedMemoryAllocator : public MemoryAllocator {
 private:
  std::mutex mutex_;

  size_t block_size_;

  char *addr_;
  size_t size_;

  MemoryRegion *head_;
  MemoryRegion *free_head_;

  MemoryRegion *split_and_use_region(MemoryRegion *region, size_t first_size);
  MemoryRegion *extract_from_free_list(MemoryRegion *region);
  MemoryRegion *prepend_free(MemoryRegion *region);

 public:
  enum {
    NOSPACE = 0,
    NUM_STATUS = -1,
  };
  BlockBasedMemoryAllocator(size_t size, size_t block_size);
  ~BlockBasedMemoryAllocator();

  static const char *kClassName() { return "BlockBasedMemoryAllocator"; }
  const char *Name() const override { return kClassName(); }

  MemoryRegion *rmalloc(size_t size);
  size_t rmfree(MemoryRegion *region);

  /***********************************************
   * Interface of MemoryAllocator
   ***********************************************/
  // Allocate a block of at least size. Has to be thread-safe.
  void *Allocate(size_t size) override {
    // return reinterpret_cast<void*>(new char[size]);
    return (void *)rmalloc(size);
  }

  // Deallocate previously allocated block. Has to be thread-safe.
  void Deallocate(void *p) override {
    // delete[] static_cast<char*>(p);
    rmfree(reinterpret_cast<MemoryRegion *>(p));
  }

  // Returns the memory size of the block allocated at p. The default
  // implementation that just returns the original allocation_size is fine.
  size_t UsableSize(void * /*p*/, size_t allocation_size) const {
    // default implementation just returns the allocation size
    return allocation_size;
  }

  std::string GetId() const override { return GenerateIndividualId(); }
};
}  // namespace ROCKSDB_NAMESPACE