#pragma once

#include <chrono>

#include "rdma_transport.h"
#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct RMAsyncRequest {
  rdma::RDMAAsyncRequest *rdma_async_request;

  bool is_read;  // true for read; false for write
  void *rdma_buf;
  void *target_buf; // read dst or `e->value` for write
  size_t size;

  std::mutex *mutex;

  RMRegion *rm_region;  // temporaly store the rm_region

  int wait() {
    int ret;
    ret = rdma_async_request->wait();

    if (is_read) {
      if (target_buf) {
        memcpy(target_buf, rdma_buf, size);
      }
    }

    if (ret) {
      throw "rdma_async_request->wait() failed";
    }
    // printf("wait done(is_read=%d), rm_region: [0x%lx,0x%lx)\n", is_read, rm_region->addr, rm_region->addr + rm_region->size);
    return 0;
  }

  void unlock_and_reset() {
    rdma_async_request->reset_cm_id_state();
    mutex->unlock();
    
    // printf("request{is_read=%d, rm_region=[0x%lx,0x%lx)} is unlocked\n", is_read, rm_region->addr, rm_region->addr + rm_region->size);
  }
};
}  // namespace ROCKSDB_NAMESPACE
