#pragma once

#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "rdma_utils.h"

#define VERB_ERR(verb, ret) \
  fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)
// #define CHECK_RDMA_CM_ID_WORKING

namespace rdma {

constexpr char *DEFAULT_PORT = (char *)"51216";
constexpr int DEFAULT_MAX_WR = 128;
constexpr int DEFAULT_MAX_QP = 128;
constexpr int DEFAULT_MAX_SHARDS = 64;
constexpr int DEFAULT_MAX_CLIENT_QP = 2;
constexpr int DEFAULT_MSG_LENGTH = 1024;
constexpr int DEFAULT_BUF_LENGTH = 8192;

enum rdma_id_state {
  RDMA_ID_STATE_FREE = 0,
  RDMA_ID_STATE_CONNECTED = 1,
  RDMA_ID_STATE_DISCONNECTED = 2,
  RDMA_ID_STATE_TIMEWAIT_EXITED = 3,
  RDMA_ID_STATE_QP_DESTROYED = 4,
  RDMA_ID_STATE_ID_DESTROYED = 5,
  RDMA_ID_STATE_READING = 6,
  RDMA_ID_STATE_WRITING = 7,
};

/* Resources used in the example */
struct Context {
  /* User parameters */
  bool server;
  char *server_name;
  char *server_port;
  int msg_count;
  int msg_length;
  int local_buf_size;
  int qp_count;
  int max_wr;
  /* Resources */
  struct rdma_cm_id *srq_id;
  struct rdma_cm_id *listen_id;
  struct rdma_cm_id **conn_ids;
  std::atomic<enum rdma_id_state> *id_states;
  struct rdma_event_channel *event_channel;
  struct ibv_mr *send_mr;
  struct ibv_mr *recv_mr;
  struct ibv_mr **buf_mrs;
  struct ibv_srq *srq;
  struct ibv_cq *srq_cq;
  struct ibv_comp_channel *srq_cq_channel;
  char *send_buf;
  char *recv_buf;
  /*
   * For client, more bufs enbale higher concurrency, its size is `qp_count`
   * For server, it has to have a buf (i.e., rm) for each shard, its size is
   * `max_num_shards`
   */
  char **bufs;
  /* remote memory info (client only) */
  uint32_t rm_rkey;
  uint64_t rm_addr;
  size_t rm_size;

  // for all local memory if enable BlockBasedMemoryAllocator
  char *lm_buf;
  size_t lm_size;
  ibv_mr *lm_mr;

  /* client only*/
  size_t shard_id;
  /* server only*/
  size_t *num_connnected_client;  // index is `shard_id`
  size_t max_num_shards;
};

enum async_request_type {
  ASYNC_READ = 0,
  ASYNC_WRITE = 1,
};

/* async ops request*/
struct RDMAAsyncRequest {
  // TODO: don't let any other thread modify the id_state if it is held by
  // AsyncRequest (just checking whether it is reading/writing is ok)
#ifdef CHECK_RDMA_CM_ID_WORKING
  std::atomic<enum rdma_id_state> *id_state;
#endif
  enum async_request_type type;
  struct rdma_cm_id *cm_id;

  char *lm_addr;
  size_t lm_length;
  uint64_t rm_addr;

  int wait();
  int reset_cm_id_state();
};

class Transport {
  // TODO: target on 1 qp now, will extent it to qp pool.
 public:
  // server
  Transport(bool server, const char *server_name,
            size_t max_num_shards = DEFAULT_MAX_SHARDS);
  // client
  Transport(bool server, const char *server_name, size_t rm_size,
            size_t shard_id, size_t client_qp_count = DEFAULT_MAX_CLIENT_QP,
            size_t local_buf_size = DEFAULT_BUF_LENGTH);
  ~Transport();

  const struct Context *get_context() { return ctx_; }

  int read_rm(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
              struct ibv_mr *lm_mr, uint64_t rm_addr, uint32_t rm_rkey,
              RDMAAsyncRequest *rdma_async_request = nullptr);
  int write_rm(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
               struct ibv_mr *lm_mr, uint64_t rm_addr, uint32_t rm_rkey,
               RDMAAsyncRequest *rdma_async_request = nullptr);
  int send(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
           struct ibv_mr *send_mr);
  int recv(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
           struct ibv_mr *recv_mr);

  void reg_lm(char *lm_buf, size_t lm_size, struct ibv_mr *lm_mr = nullptr);
  void dereg_lm();

 private:
  struct rdma::Context *ctx_;
  struct rdma_addrinfo *rai_;

  std::mutex mutex_;
  std::unordered_map<uint32_t, size_t> qp_num_2_conn_idx;
  std::unordered_map<uint32_t, size_t> qp_num_2_shard_id;

  int init_resources(struct rdma_addrinfo *rai);
  void destroy_resources();
  int await_completion();
  int wait_send_comp();

  int run_server();
  int connect_server(struct rdma_addrinfo *rai);
  int handle_connect_request(struct rdma_cm_event *cm_event);
  int handle_disconnected(struct rdma_cm_event *cm_event);
  int handle_timewait_exit(struct rdma_cm_event *cm_event);
};

}  // namespace rdma