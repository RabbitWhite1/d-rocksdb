#include <iostream>
#include "cache/remote_memory/rdma_transport.h"
#include "cache/remote_memory/rdma_utils.h"

int main(int argc, char **argv) {
  int ret;
  char server_name[] = "10.0.0.5";
  if (argc > 1) {
    rdma::Transport(/*server=*/true, server_name);
  } else {
    rdma::Transport transport(/*server=*/false, server_name);
    const rdma::Context *ctx = transport.get_context();
    for (int i = 0; i < 10; i++) {
      int qp_idx = 0;
      if (ret = transport.read_rm(ctx->conn_ids[qp_idx], ctx->buf,
                                  ctx->msg_length, ctx->buf_mr, ctx->rm_addr,
                                  ctx->rm_rkey)) {
        VERB_ERR("read_rm", ret);
        exit(ret);
      }
      memset(ctx->buf, 0, ctx->msg_length);
      sprintf(ctx->buf, "Hello Hank! %d", i);
      if (ret = transport.write_rm(ctx->conn_ids[qp_idx], ctx->buf,
                                   strlen(ctx->buf), ctx->buf_mr, ctx->rm_addr,
                                   ctx->rm_rkey)) {
        VERB_ERR("write_rm", ret);
        exit(ret);
      }
      if (ret = transport.read_rm(ctx->conn_ids[qp_idx], ctx->buf,
                                  ctx->msg_length, ctx->buf_mr, ctx->rm_addr,
                                  ctx->rm_rkey)) {
        VERB_ERR("read_rm", ret);
        exit(ret);
      }
      printf("read result: %s\n", ctx->buf);
    }
  }
}