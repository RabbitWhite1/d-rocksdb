#pragma once
#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>

#include "async_request.h"
#include "rdma_transport.h"
#include "rdma_utils.h"
#include "rm_allocator.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class RemoteMemory {
public:
  RemoteMemory(RemoteMemoryAllocator *rm_allocator, std::string server_name,
               const size_t size);
  ~RemoteMemory();

  void print();

  RMRegion *rmalloc(size_t size);
  void rmfree(RMRegion *rm_region);
  void rmfree(RMRegion *rm_region, size_t size); // free with verify.
  int read(uint64_t rm_addr, void *buf, size_t size,
           AsyncRequest *async_request = nullptr);
  int write(uint64_t rm_addr, void *buf, size_t size,
            AsyncRequest *async_request = nullptr);

private:
  std::string server_name_;
  size_t rm_size_;
  std::mutex mutex_;

  rdma::Transport *transport_;
  RemoteMemoryAllocator *allocator_;
};

class RemoteMemoryServer {
public:
  RemoteMemoryServer(std::string server_name);
  ~RemoteMemoryServer();

private:
  std::string server_name_;
  size_t rm_size_;

  rdma::Transport *transport_;
  RemoteMemoryAllocator *allocator_;
};

} // namespace ROCKSDB_NAMESPACE