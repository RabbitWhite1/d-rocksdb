#pragma once
#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>

#include "rdma_transport.h"
#include "rdma_utils.h"
#include "rm_allocator.h"
#include "rm_async_request.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/utilities/block_based_memory_allocator.h"

namespace ROCKSDB_NAMESPACE {

class LocalMemory {
 public:
  LocalMemory(MemoryAllocator *allocator);
  ~LocalMemory();

  MemoryAllocator *get_allocator() { return allocator_; }
  char *get_lm_addr() { return lm_addr_; }
  size_t get_lm_size() { return lm_size_; }
  struct ibv_mr *get_lm_mr() {
    return lm_mr_;
  }
  void set_lm_mr(struct ibv_mr *lm_mr) {
    // use value copy
    lm_mr_ = lm_mr;
  }

 private:
  MemoryAllocator *allocator_;
  char *lm_addr_;
  size_t lm_size_;
  struct ibv_mr *lm_mr_;
};

}  // namespace ROCKSDB_NAMESPACE