#include "cache/remote_memory/block_based_rm_allocator.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

BlockBasedRemoteMemoryAllocator::BlockBasedRemoteMemoryAllocator(
    size_t block_size) {
  block_size_ = block_size;
}

void BlockBasedRemoteMemoryAllocator::init(uint64_t addr, size_t size) {
  rm_addr_ = addr;
  rm_size_ = size;
  head_ = (RMRegion *)malloc(sizeof(RMRegion));
  head_->addr = rm_addr_;
  head_->size = rm_size_;
  head_->is_free = true;
  head_->prev = nullptr;
  head_->next = nullptr;
  head_->next_free = nullptr;
  head_->prev_free = nullptr;
  free_head_ = head_;
}

BlockBasedRemoteMemoryAllocator::~BlockBasedRemoteMemoryAllocator() {
  // TODO: free all regions
}

struct RMRegion *BlockBasedRemoteMemoryAllocator::split_and_use_region(
    RMRegion *region, size_t first_size) {
  // TODO: move this method outside to RemoteMemory (which is more convenient to
  // manager the linked list) return the possible new free region, which is also
  // a possible new head
  assert(first_size < region->size);
  struct RMRegion *new_region = new RMRegion();
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

RMRegion *BlockBasedRemoteMemoryAllocator::rmalloc(size_t size) {
  if (size > block_size_) {
    throw "size larger than block size";
  }

  RMRegion *free_region = nullptr;
  size_t free_region_size = 0;

  std::lock_guard<std::mutex> lock(mutex_);
  {
    free_region = free_head_;
    while (free_region != nullptr) {
      if (block_size_ < (free_region_size = free_region->size)) {
        RMRegion *new_free_region =
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

RMRegion *BlockBasedRemoteMemoryAllocator::extract_from_free_list(
    RMRegion *region) {
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

RMRegion *BlockBasedRemoteMemoryAllocator::prepend_free(RMRegion *region) {
  region->next_free = free_head_;
  region->prev_free = nullptr;
  if (free_head_) {
    free_head_->prev_free = region;
  }
  free_head_ = region;
  return region;
}

size_t BlockBasedRemoteMemoryAllocator::rmfree(RMRegion *region) {
  // printf("[%-16s] free 0x%lx\n", "Info", addr);
  std::lock_guard<std::mutex> lock(mutex_);
  size_t size_to_free = region->size;
  // try to merge prev region if free
  if (region->prev != nullptr and region->prev->is_free) {
    RMRegion *prev = region->prev;
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
    RMRegion *next = region->next;
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

void BlockBasedRemoteMemoryAllocator::print_size_info() {
  size_t total_size = 0;
  size_t free_size = 0;
  RMRegion *region = head_;
  while (region != nullptr) {
    if (region->is_free) {
      free_size += region->size;
    }
    total_size += region->size;
    region = region->next;
  }
  printf("free_size/total_size=%lu/%lu\n", free_size, total_size);
}

void BlockBasedRemoteMemoryAllocator::print(bool only_free) {
  // print region list
  RMRegion *region;
  if (not only_free) {
    region = head_;
    while (region != nullptr) {
      if (region->is_free) {
        printf("-->{[0x%lx, 0x%lx), 0x%lx, free}", region->addr,
               region->addr + region->size, region->size);
      } else {
        printf("-->{[0x%lx, 0x%lx), 0x%lx, used}", region->addr,
               region->addr + region->size, region->size);
      }
      region = region->next;
    }
    putchar('\n');
  }
  // print free region list
  region = free_head_;
  while (region != nullptr) {
    if (region->is_free) {
      printf("-->{[0x%lx, 0x%lx), 0x%lx, free}", region->addr,
             region->addr + region->size, region->size);
    } else {
      printf("-->{[0x%lx, 0x%lx), 0x%lx, used}", region->addr,
             region->addr + region->size, region->size);
    }
    region = region->next_free;
  }
  putchar('\n');
}

}  // namespace ROCKSDB_NAMESPACE