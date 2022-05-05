#pragma once

#include "rdma_transport.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
  
  struct AsyncRequest {
    rdma::AsyncRequest *rdma_async_request;
    std::unique_ptr<char[]> buf_data;

    int wait() {
      return rdma_async_request->wait();
    }
  };
} // namespace ROCKSDB_NAMESPACE
