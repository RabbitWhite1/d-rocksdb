//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include "rocksdb/memory_allocator.h"
#include "rocksdb/utilities/block_based_memory_allocator.h"

namespace ROCKSDB_NAMESPACE {

struct CustomDeleter {
  CustomDeleter(MemoryAllocator* a = nullptr,
                MemoryRegion* memory_region = nullptr)
      : allocator(a), memory_region(memory_region) {}

  void operator()(char* ptr) const {
    if (allocator) {
      if (memory_region) {
        allocator->Deallocate(reinterpret_cast<void*>(memory_region));
      } else {
        allocator->Deallocate(reinterpret_cast<void*>(ptr));
      }
    } else {
      delete[] ptr;
    }
  }

  MemoryAllocator* allocator;
  MemoryRegion* memory_region;  // for `BlockBasedMemoryAllocator`
};

using CacheAllocationPtr = std::unique_ptr<char[], CustomDeleter>;

inline CacheAllocationPtr AllocateBlock(size_t size,
                                        MemoryAllocator* allocator) {
  if (allocator) {
    if (strcmp(allocator->Name(), "BlockBasedMemoryAllocator") == 0) {
      auto memory_region =
          reinterpret_cast<MemoryRegion*>(allocator->Allocate(size));
      return CacheAllocationPtr(reinterpret_cast<char*>(memory_region->addr),
                                CustomDeleter(allocator, memory_region));
      // auto block = reinterpret_cast<char*>(allocator->Allocate(size));
      // return CacheAllocationPtr(block, CustomDeleter(allocator, nullptr));
    } else {
      auto block = reinterpret_cast<char*>(allocator->Allocate(size));
      return CacheAllocationPtr(block, CustomDeleter(allocator, nullptr));
    }
  }
  return CacheAllocationPtr(new char[size], CustomDeleter(nullptr, nullptr));
}

}  // namespace ROCKSDB_NAMESPACE
