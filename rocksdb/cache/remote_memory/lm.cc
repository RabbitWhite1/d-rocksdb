#include "lm.h"

#include <iostream>

namespace ROCKSDB_NAMESPACE {

LocalMemory::LocalMemory(MemoryAllocator* allocator) {
  allocator_ = allocator;
  if (allocator &&
      strcmp(allocator->Name(), "BlockBasedMemoryAllocator") == 0) {
    auto block_based_allocator =
        reinterpret_cast<BlockBasedMemoryAllocator*>(allocator_);
    lm_addr_ = block_based_allocator->get_addr();
    lm_size_ = block_based_allocator->get_size();
    lm_mr_ = nullptr;
  }
  // TODO: reg lm and tell remote_memorys
}

LocalMemory::~LocalMemory() {}

}  // namespace ROCKSDB_NAMESPACE