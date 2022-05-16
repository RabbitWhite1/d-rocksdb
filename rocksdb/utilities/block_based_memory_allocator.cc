#include "rocksdb/utilities/block_based_memory_allocator.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

BlockBasedMemoryAllocator::BlockBasedMemoryAllocator(size_t size,
                                                     size_t block_size) {
  block_size_ = block_size;
  addr_ = new char[size];
  size_ = size;
  head_ = (MemoryRegion *)malloc(sizeof(MemoryRegion));
  head_->addr = addr_;
  head_->size = size_;
  head_->is_free = true;
  head_->prev = nullptr;
  head_->next = nullptr;
  head_->next_free = nullptr;
  head_->prev_free = nullptr;
  free_head_ = head_;
  printf("lm range: [%p, %p)\n", addr_, addr_ + size_);
}

BlockBasedMemoryAllocator::~BlockBasedMemoryAllocator() {
  // TODO: free all regions
  delete[] addr_;
}

struct MemoryRegion *BlockBasedMemoryAllocator::split_and_use_region(
    MemoryRegion *region, size_t first_size) {
  // TODO: move this method outside to RemoteMemory (which is more convenient to
  // manager the linked list) return the possible new free region, which is also
  // a possible new head
  assert(first_size < region->size);
  struct MemoryRegion *new_region = new MemoryRegion();
  // set region list
  new_region->addr = region->addr + first_size;
  new_region->size = region->size - first_size;
  new_region->is_free = true;
  new_region->prev = region;
  new_region->next = region->next;
  if (region->next != nullptr) {
    region->next->prev = new_region;
  }
  region->next = new_region;
  // set free region list
  new_region->next_free = region->next_free;
  new_region->prev_free = region->prev_free;
  if (region->next_free != nullptr) {
    region->next_free->prev_free = new_region;
  }
  if (region->prev_free != nullptr) {
    region->prev_free->next_free = new_region;
  }
  region->next_free = nullptr;
  region->prev_free = nullptr;
  // set this region metadata
  region->size = first_size;
  region->is_free = false;
  return new_region;
}

MemoryRegion *BlockBasedMemoryAllocator::rmalloc(size_t size) {
  if (size > block_size_) {
    throw "size larger than block size";
  }

  MemoryRegion *free_region = nullptr;
  size_t free_region_size = 0;

  std::lock_guard<std::mutex> lock(mutex_);
  {
    free_region = free_head_;
    while (free_region != nullptr) {
      if (block_size_ < (free_region_size = free_region->size)) {
        MemoryRegion *new_free_region =
            split_and_use_region(free_region, block_size_);
        if (free_region == free_head_) {
          free_head_ = new_free_region;
        }
        break;
      } else if (block_size_ == free_region->size) {
        free_region->is_free = false;
        if (free_region == free_head_) {
          free_head_ = free_region->next_free;
        } else {
          assert(free_region->prev_free != nullptr);
          free_region->prev_free->next_free = free_region->next_free;
          if (free_region->next_free) {
            free_region->next_free->prev_free = free_region->prev_free;
          }
        }
        free_region->next_free = nullptr;
        free_region->prev_free = nullptr;
        break;
      }
      free_region = free_region->next_free;
    }
    if (free_region == nullptr) {
      return nullptr;
    }
    // printf(
    //     "[%-16s] allocated ([0x%lx, 0x%lx), size=0x%lx) from region([0x%lx, "
    //     "0x%lx), size=0x%lx)\n",
    //     "Info", allocated_addr, allocated_addr + size, size,
    //     free_region->addr, free_region->addr + free_region_size,
    //     free_region_size);
  }

  return free_region;
}

MemoryRegion *BlockBasedMemoryAllocator::extract_from_free_list(
    MemoryRegion *region) {
  if (region->prev_free) {
    region->prev_free->next_free = region->next_free;
  }
  if (region->next_free) {
    region->next_free->prev_free = region->prev_free;
  }
  if (region == free_head_) {
    free_head_ = region->next_free;
  }
  region->prev_free = nullptr;
  region->next_free = nullptr;

  return region;
}

MemoryRegion *BlockBasedMemoryAllocator::prepend_free(MemoryRegion *region) {
  region->next_free = free_head_;
  region->prev_free = nullptr;
  if (free_head_) {
    free_head_->prev_free = region;
  }
  free_head_ = region;
  return region;
}

size_t BlockBasedMemoryAllocator::rmfree(MemoryRegion *region) {
  // printf("[%-16s] free 0x%lx\n", "Info", addr);
  std::lock_guard<std::mutex> lock(mutex_);
  size_t size_to_free = region->size;
  // try to merge prev region if free
  if (region->prev != nullptr and region->prev->is_free) {
    MemoryRegion *prev = region->prev;
    prev->size += region->size;
    prev->next = region->next;
    if (region->next != nullptr) {
      region->next->prev = prev;
    }
    extract_from_free_list(prev);
    delete region;
    region = prev;
  }
  // try to merge next region if free
  if (region->next != nullptr and region->next->is_free) {
    MemoryRegion *next = region->next;
    region->size += next->size;
    region->next = next->next;
    if (next->next != nullptr) {
      next->next->prev = region;
    }
    extract_from_free_list(next);
    delete next;
  }
  region->is_free = true;

  prepend_free(region);

  return size_to_free;
}

}  // namespace ROCKSDB_NAMESPACE