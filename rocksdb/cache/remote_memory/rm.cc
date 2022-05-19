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

RemoteMemory::~RemoteMemory() {
  delete allocator_;
  delete transport_;
}

RMRegion *RemoteMemory::rmalloc(size_t size) {
  RMRegion *rm_region = allocator_->rmalloc(size);
  return rm_region;
}

void RemoteMemory::print() {
  allocator_->print_size_info();
  allocator_->print(true);
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
  // std::chrono::high_resolution_clock::time_point begin, end_lock, end_copy,
  //     end_req, end;
  // begin = std::chrono::high_resolution_clock::now();
  while (!write_mutex_.try_lock())
    ;
  // end_lock = std::chrono::high_resolution_clock::now();
  const rdma::Context *ctx = transport_->get_context();
  // TODO: is it possible to omit this copy?
  if (buf != nullptr) {
    memcpy(ctx->bufs[1], buf, size);
  }
  // end_copy = std::chrono::high_resolution_clock::now();

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
  // end_req = std::chrono::high_resolution_clock::now();

  if (rm_async_request == nullptr) {
    // if async, let RMAsyncRequest unlock this mutex.
    write_mutex_.unlock();
  }
  if (ret) {
    allocator_->print();
    throw std::runtime_error("write to remote memory failed");
  }
  // end = std::chrono::high_resolution_clock::now();
  // printf(
  //     "lock took %lu ns, copy took %lu ns, req took %lu ns, total took %lu "
  //     "ns\n",
  //     std::chrono::duration_cast<std::chrono::nanoseconds>(end_lock - begin)
  //         .count(),
  //     std::chrono::duration_cast<std::chrono::nanoseconds>(end_copy -
  //     end_lock)
  //         .count(),
  //     std::chrono::duration_cast<std::chrono::nanoseconds>(end_req -
  //     end_copy)
  //         .count(),
  //     std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
  //         .count());
  return ret;
}

void *RemoteMemory::get_read_buf() {
  return transport_->get_context()->bufs[0];
}

void *RemoteMemory::get_write_buf() {
  return transport_->get_context()->bufs[1];
}

void RemoteMemory::init_lm(const std::shared_ptr<LocalMemory> &lm) {
  if (local_memory_ == lm) {
    return;
  } else {
    auto lm_mr = lm->get_lm_mr();
    if (lm_mr) {
      transport_->reg_lm(lm->get_lm_addr(), lm->get_lm_size(), lm_mr);
    } else {
      transport_->reg_lm(lm->get_lm_addr(), lm->get_lm_size());
      const rdma::Context *ctx = transport_->get_context();
      lm->set_lm_mr(ctx->lm_mr);
    }
    local_memory_ = lm;
  }
}

// TODO: add mutex for different shard of lm
int RemoteMemory::direct_read(
    uint64_t rm_addr, void *lm_buf, size_t size,
    RMAsyncDirectRequest *rm_async_direct_request = nullptr) {
  // TODO: check whether size is valid
  assert((char *)lm_buf >= local_memory_->get_lm_addr());
  assert((char *)lm_buf + size <=
         local_memory_->get_lm_addr() + local_memory_->get_lm_size());
  assert(rm_addr >= transport_->get_context()->rm_addr);
  assert(rm_addr + size <= transport_->get_context()->rm_addr +
                               transport_->get_context()->rm_size);
  if (not local_memory_) {
    throw std::runtime_error("local memory not initialized");
  }
  while (!read_mutex_.try_lock())
    ;
  const rdma::Context *ctx = transport_->get_context();

  rdma::RDMAAsyncRequest *rdma_async_request = nullptr;
  if (rm_async_direct_request) {
    rdma_async_request = new rdma::RDMAAsyncRequest();
    rm_async_direct_request->rdma_async_request = rdma_async_request;
    rm_async_direct_request->is_read = true;
    rm_async_direct_request->lm_value = lm_buf;
    rm_async_direct_request->size = size;
    rm_async_direct_request->mutex = &read_mutex_;
  }

  int ret = transport_->read_rm(ctx->conn_ids[0], (char *)lm_buf, size,
                                local_memory_->get_lm_mr(), rm_addr,
                                ctx->rm_rkey, rdma_async_request);
  if (ret) {
    allocator_->print();
    throw "read from remote memory failed";
  }

  if (rm_async_direct_request == nullptr) {
    // if async, let RMAsyncRequest unlock this mutex.
    read_mutex_.unlock();
  }
  return ret;
}

int RemoteMemory::direct_write(
    uint64_t rm_addr, void *lm_buf, size_t size,
    RMAsyncDirectRequest *rm_async_direct_request = nullptr) {
  // TODO: decide which conn_id to use
  // TODO: check whether size is valid
  assert((char *)lm_buf >= local_memory_->get_lm_addr());
  assert((char *)lm_buf + size <=
         local_memory_->get_lm_addr() + local_memory_->get_lm_size());
  assert(rm_addr >= transport_->get_context()->rm_addr);
  assert(rm_addr + size <= transport_->get_context()->rm_addr +
                               transport_->get_context()->rm_size);
  if (not local_memory_) {
    throw std::runtime_error("local memory not initialized");
  }
  while (!write_mutex_.try_lock())
    ;
  const rdma::Context *ctx = transport_->get_context();

  rdma::RDMAAsyncRequest *rdma_async_request = nullptr;
  if (rm_async_direct_request) {
    rdma_async_request = new rdma::RDMAAsyncRequest();
    rm_async_direct_request->rdma_async_request = rdma_async_request;
    rm_async_direct_request->is_read = false;
    rm_async_direct_request->lm_value = lm_buf;
    rm_async_direct_request->size = size;
    rm_async_direct_request->mutex = &write_mutex_;
  }

  int ret = transport_->write_rm(ctx->conn_ids[1], (char *)lm_buf, size,
                                 local_memory_->get_lm_mr(), rm_addr,
                                 ctx->rm_rkey, rdma_async_request);

  if (rm_async_direct_request == nullptr) {
    // if async, let RMAsyncRequest unlock this mutex.
    write_mutex_.unlock();
  }
  if (ret) {
    allocator_->print();
    throw "write to remote memory failed";
  }
  return ret;
}

void RemoteMemory::deinit_lm() { transport_->dereg_lm(); }

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