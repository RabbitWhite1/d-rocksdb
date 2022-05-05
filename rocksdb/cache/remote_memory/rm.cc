#include "rm.h"

#include <iostream>

namespace ROCKSDB_NAMESPACE {

RemoteMemory::RemoteMemory(RemoteMemoryAllocator *rm_allocator,
                           std::string server_name, const size_t size) {
  server_name_ = server_name;
  rm_size_ = size;
  transport_ =
      new rdma::Transport(/*server=*/false, server_name_.c_str(), size);
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

void RemoteMemory::rmfree(RMRegion *rm_region, size_t /*size*/) {
  allocator_->rmfree(rm_region);
  // size_t freed_size = allocator_->rmfree(rm_region);
  // assert(freed_size == size);
}

int RemoteMemory::read(uint64_t rm_addr, void *buf, size_t size,
                       AsyncRequest * /*async_request*/) {
  // TODO: decide which conn_id to use
  // TODO: check whether size is valid
  std::lock_guard<std::mutex> lock(mutex_);
  const rdma::Context *ctx = transport_->get_context();
  int ret = transport_->read_rm(ctx->conn_ids[0], ctx->buf, size, ctx->buf_mr,
                                rm_addr, ctx->rm_rkey);
  if (ret) {
    allocator_->print();
    throw "read from remote memory failed";
  }
  // TODO: is it possible to omit this copy?
  if (buf != nullptr) {
    memcpy(buf, ctx->buf, size);
  }
  return ret;
}

int RemoteMemory::write(uint64_t rm_addr, void *buf, size_t size,
                        AsyncRequest * /*async_request*/) {
  // TODO: decide which conn_id to use
  // TODO: check whether size is valid
  std::lock_guard<std::mutex> lock(mutex_);
  const rdma::Context *ctx = transport_->get_context();
  // TODO: is it possible to omit this copy?
  memcpy(ctx->buf, buf, size);
  int ret = transport_->write_rm(ctx->conn_ids[0], ctx->buf, size, ctx->buf_mr,
                                 rm_addr, ctx->rm_rkey);
  if (ret) {
    allocator_->print();
    throw "write to remote memory failed";
  }
  return ret;
}

void *RemoteMemory::get_buf() { return transport_->get_context()->buf; }

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