#include "rm.h"

#include <iostream>

namespace ROCKSDB_NAMESPACE {

RemoteMemory::RemoteMemory(RemoteMemoryAllocator *rm_allocator,
                           std::string server_name, const size_t size,
                           const size_t shard_id) {
  server_name_ = server_name;
  rm_size_ = size;
  shard_id_ = shard_id;
  transport_ = new rdma::Transport(/*server=*/false, server_name_.c_str(), size,
                                   shard_id_, /*client_qp_count=*/2);
  const rdma::Context *ctx = transport_->get_context();
  allocator_ = rm_allocator;
  allocator_->init(ctx->rm_addr, ctx->rm_size);
}

RMRegion *RemoteMemory::rmalloc(size_t size) {
  RMRegion *rm_region = allocator_->rmalloc(size);
  return rm_region;
}

void RemoteMemory::print() {
  allocator_->print_size_info();
  allocator_->print(true);
}

RemoteMemory::~RemoteMemory() {
  delete allocator_;
  delete transport_;
}

void RemoteMemory::rmfree(RMRegion *rm_region) {
  allocator_->rmfree(rm_region);
}

void RemoteMemory::rmfree(RMRegion *rm_region, size_t size) {
  size_t freed_size = allocator_->rmfree(rm_region);
  if (freed_size != size) {
    throw "rmfree size mismatch";
  }
}

int RemoteMemory::read(uint64_t rm_addr, void *buf, size_t size,
                       RMAsyncRequest *rm_async_request) {
  // TODO: decide which conn_id to use
  // TODO: check whether size is valid
  while (!read_mutex_.try_lock())
    ;
  const rdma::Context *ctx = transport_->get_context();

  rdma::RDMAAsyncRequest *rdma_async_request = nullptr;
  if (rm_async_request) {
    rdma_async_request = new rdma::RDMAAsyncRequest();
    rm_async_request->rdma_async_request = rdma_async_request;
    rm_async_request->is_read = true;
    rm_async_request->rdma_buf = ctx->bufs[0];
    rm_async_request->target_buf = buf;
    rm_async_request->size = size;
    rm_async_request->mutex = &read_mutex_;
  }

  int ret =
      transport_->read_rm(ctx->conn_ids[0], ctx->bufs[0], size, ctx->buf_mrs[0],
                          rm_addr, ctx->rm_rkey, rdma_async_request);
  if (ret) {
    allocator_->print();
    throw "read from remote memory failed";
  }

  // TODO: is it possible to omit this copy?
  if (rm_async_request == nullptr && buf != nullptr) {
    memcpy(buf, ctx->bufs[0], size);
  }
  if (rm_async_request == nullptr) {
    // if async, let RMAsyncRequest unlock this mutex.
    read_mutex_.unlock();
  }
  return ret;
}

int RemoteMemory::write(uint64_t rm_addr, void *buf, size_t size,
                        RMAsyncRequest *rm_async_request) {
  // TODO: decide which conn_id to use
  // TODO: check whether size is valid
  while (!write_mutex_.try_lock())
    ;
  const rdma::Context *ctx = transport_->get_context();
  // TODO: is it possible to omit this copy?
  if (buf != nullptr) {
    memcpy(ctx->bufs[1], buf, size);
  }

  rdma::RDMAAsyncRequest *rdma_async_request = nullptr;
  if (rm_async_request) {
    rdma_async_request = new rdma::RDMAAsyncRequest();
    rm_async_request->rdma_async_request = rdma_async_request;
    rm_async_request->is_read = false;
    rm_async_request->rdma_buf = ctx->bufs[1];
    rm_async_request->target_buf = buf;
    rm_async_request->size = size;
    rm_async_request->mutex = &write_mutex_;
  }

  int ret = transport_->write_rm(ctx->conn_ids[1], ctx->bufs[1], size,
                                 ctx->buf_mrs[1], rm_addr, ctx->rm_rkey,
                                 rdma_async_request);

  if (rm_async_request == nullptr) {
    // if async, let RMAsyncRequest unlock this mutex.
    write_mutex_.unlock();
  }
  if (ret) {
    allocator_->print();
    throw "write to remote memory failed";
  }
  return ret;
}

void *RemoteMemory::get_read_buf() {
  return transport_->get_context()->bufs[0];
}

void *RemoteMemory::get_write_buf() {
  return transport_->get_context()->bufs[1];
}

RemoteMemoryServer::RemoteMemoryServer(std::string server_name) {
  server_name_ = server_name;
  transport_ = new rdma::Transport(/*server=*/true, server_name_.c_str());
  const rdma::Context *ctx = transport_->get_context();
  rm_size_ = ctx->rm_size;
}

RemoteMemoryServer::~RemoteMemoryServer() {
  delete allocator_;
  delete transport_;
}
}  // namespace ROCKSDB_NAMESPACE