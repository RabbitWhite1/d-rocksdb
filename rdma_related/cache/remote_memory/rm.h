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

namespace ROCKSDB_NAMESPACE {

class RemoteMemory {
public:
  RemoteMemory(RemoteMemoryAllocator *rm_allocator, std::string server_name,
               const size_t size, const size_t shard_id);
  ~RemoteMemory();

  void print();

  RMRegion *rmalloc(size_t size);
  void rmfree(RMRegion *rm_region);
  void rmfree(RMRegion *rm_region, size_t size); // free with verify.
  int read(uint64_t rm_addr, void *buf, size_t size,
           RMAsyncRequest *rm_async_request = nullptr);
  int write(uint64_t rm_addr, void *buf, size_t size,
            RMAsyncRequest *rm_async_request = nullptr);
  void *get_read_buf();
  void *get_write_buf();

private:
  std::string server_name_;
  size_t rm_size_;
  size_t shard_id_;

  std::mutex read_mutex_;
  std::mutex write_mutex_;

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