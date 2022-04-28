#include "rm.h"
#include <iostream>


namespace ROCKSDB_NAMESPACE {

RemoteMemory::RemoteMemory(std::string server_name, const size_t size) {
  server_name_ = server_name;
  rm_size_ = size;
  transport_ =
      new rdma::Transport(/*server=*/false, server_name_.c_str(), size);
  allocator_ =
      new RemoteMemoryAllocator(transport_->get_context()->rm_addr, size);
}

uint64_t RemoteMemory::rmalloc(size_t size) {
  uint64_t rm_addr = allocator_->rmalloc(size);
  return rm_addr;
}

void RemoteMemory::print() {
  allocator_->print_size_info();
  allocator_->print(true);
}

RemoteMemory::~RemoteMemory() {
  delete allocator_;
  delete transport_;
}

void RemoteMemory::rmfree(uint64_t addr) { 
  allocator_->rmfree(addr);
}

void RemoteMemory::rmfree(uint64_t addr, size_t size) {
  size_t freed_size = allocator_->rmfree(addr);
  assert(freed_size == size);
}

int RemoteMemory::read(uint64_t rm_addr, void *buf, size_t size) {
  // TODO: decide which conn_id to use
  // TODO: check whether size is valid
  const rdma::Context *ctx = transport_->get_context();
  int ret = transport_->read_rm(ctx->conn_ids[0], ctx->buf, size,
                                ctx->buf_mr, rm_addr, ctx->rm_rkey);
  if (ret) {
    allocator_->print();
    throw "read from remote memory failed";
  }
  // TODO: is it possible to omit this copy?
  memcpy(buf, ctx->buf, size);
  return ret;
}
int RemoteMemory::write(uint64_t rm_addr, void *buf, size_t size) {
  // TODO: decide which conn_id to use
  // TODO: check whether size is valid
  const rdma::Context *ctx = transport_->get_context();
  // TODO: is it possible to omit this copy?
  memcpy(ctx->buf, buf, size);
  int ret = transport_->write_rm(ctx->conn_ids[0], ctx->buf, size,
                                 ctx->buf_mr, rm_addr, ctx->rm_rkey);
  if (ret) {
    allocator_->print();
    throw "write to remote memory failed";
  }
  return ret;
}

RemoteMemoryServer::RemoteMemoryServer(std::string server_name) {
  server_name_ = server_name;
  transport_ = new rdma::Transport(/*server=*/true, server_name_.c_str());
  const rdma::Context *ctx = transport_->get_context();
  rm_size_ = ctx->rm_size;
  allocator_ = new RemoteMemoryAllocator(ctx->rm_addr, ctx->rm_size);
}

RemoteMemoryServer::~RemoteMemoryServer() {
  delete allocator_;
  delete transport_;
}
}  // namespace ROCKSDB_NAMESPACE