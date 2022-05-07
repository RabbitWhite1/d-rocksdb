#include "cache/remote_memory/rdma_transport.h"
#include "cache/remote_memory/rdma_utils.h"
#include <iostream>

int main(int argc, char **argv) {
  int ret;
  char server_name[] = "10.0.0.5";
  if (argc > 1) {
    rdma::Transport(/*server=*/true, server_name);
  } else {
    rdma::Transport transport(/*server=*/false, server_name, 4096, 0, 2);
    const rdma::Context *ctx = transport.get_context();

    std::string buf = "Hello World!\0";
    rdma::RDMAAsyncRequest *rdma_async_request = new rdma::RDMAAsyncRequest;
    transport.read_rm(ctx->conn_ids[0], ctx->bufs[0], ctx->msg_length,
                      ctx->buf_mrs[0], ctx->rm_addr, ctx->rm_rkey,
                      rdma_async_request);
    rdma_async_request->wait();

    memcpy(ctx->bufs[1], buf.c_str(), buf.size());
    transport.write_rm(ctx->conn_ids[1], ctx->bufs[1], buf.size(),
                      ctx->buf_mrs[1], ctx->rm_addr, ctx->rm_rkey,
                      rdma_async_request);
    rdma_async_request->wait();
    
    transport.read_rm(ctx->conn_ids[0], ctx->bufs[0], ctx->msg_length,
                      ctx->buf_mrs[0], ctx->rm_addr, ctx->rm_rkey,
                      rdma_async_request);
    rdma_async_request->wait();
  }
}