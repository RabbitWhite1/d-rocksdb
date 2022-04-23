#include "rdma_transport.h"
#include <iostream>

rdma::Transport::Transport(bool server, const char *server_name) {
  assert(server == true && "if not serer, specify `size`");
  int ret;
  struct rdma_addrinfo *rai, hints;

  ctx_ = (struct rdma::Context *)malloc(sizeof(struct rdma::Context));
  memset(ctx_, 0, sizeof(rdma::Context));
  memset(&hints, 0, sizeof(struct rdma_addrinfo));
  ctx_->server = server;
  ctx_->server_name = strdup(server_name);
  ctx_->server_port = DEFAULT_PORT;
  ctx_->qp_count = DEFAULT_MAX_QP;
  ctx_->max_wr = DEFAULT_MAX_WR;
  ctx_->msg_length = DEFAULT_MSG_LENGTH;

  hints.ai_port_space = RDMA_PS_TCP;
  if (ctx_->server == true)
    hints.ai_flags = RAI_PASSIVE; /* this makes it a server */
  ret = rdma_getaddrinfo(ctx_->server_name, ctx_->server_port, &hints, &rai);
  if (ret) {
    VERB_ERR("rdma_getaddrinfo", ret);
    exit(1);
  }
  ctx_->conn_ids =
      (struct rdma_cm_id **)calloc(ctx_->qp_count, sizeof(struct rdma_cm_id *));
  ctx_->send_buf = (char *)malloc(ctx_->msg_length);
  memset(ctx_->send_buf, 0, ctx_->msg_length);
  ctx_->recv_buf = (char *)malloc(ctx_->msg_length);
  memset(ctx_->recv_buf, 0, ctx_->msg_length);

  ret = init_resources(rai);
  if (ret) {
    printf("init_resources returned %d\n", ret);
    return;
  }

  ret = run_server();
}

rdma::Transport::Transport(bool server, const char *server_name, size_t rm_size,
                           size_t local_buf_size) {
  int ret;
  struct rdma_addrinfo *rai, hints;

  ctx_ = (struct rdma::Context *)malloc(sizeof(struct rdma::Context));
  memset(ctx_, 0, sizeof(rdma::Context));
  memset(&hints, 0, sizeof(struct rdma_addrinfo));
  ctx_->server = server;
  ctx_->server_name = strdup(server_name);
  ctx_->server_port = DEFAULT_PORT;
  ctx_->qp_count = DEFAULT_MAX_QP;
  ctx_->max_wr = DEFAULT_MAX_WR;
  ctx_->msg_length = DEFAULT_MSG_LENGTH;
  ctx_->local_buf_size = local_buf_size;
  ctx_->rm_size = rm_size;

  hints.ai_port_space = RDMA_PS_TCP;
  if (ctx_->server == true)
    hints.ai_flags = RAI_PASSIVE; /* this makes it a server */
  ret = rdma_getaddrinfo(ctx_->server_name, ctx_->server_port, &hints, &rai);
  if (ret) {
    VERB_ERR("rdma_getaddrinfo", ret);
    exit(1);
  }
  ctx_->conn_ids =
      (struct rdma_cm_id **)calloc(ctx_->qp_count, sizeof(struct rdma_cm_id *));
  ctx_->send_buf = (char *)malloc(ctx_->msg_length);
  memset(ctx_->send_buf, 0, ctx_->msg_length);
  ctx_->recv_buf = (char *)malloc(ctx_->msg_length);
  memset(ctx_->recv_buf, 0, ctx_->msg_length);
  ctx_->buf = (char *)malloc(ctx_->local_buf_size);
  memset(ctx_->buf, 0, ctx_->local_buf_size);

  memcpy(ctx_->buf, (char *)"Hello World", 11);

  ret = init_resources(rai);
  if (ret) {
    printf("init_resources returned %d\n", ret);
    return;
  }

  ret = connect_server(rai);
}

rdma::Transport::~Transport() {
  // need to check about memory leak
  destroy_resources();
}

/*
 * Function: init_resources
 *
 * Input:
 * rai The RDMA address info for the connection
 *
 * Description:
 * This function initializes resources that are common to both the client
 * and server functionality.
 * It creates our SRQ, registers memory regions, posts receive buffers
 * and creates a single completion queue that will be used for the receive
 * queue on each queue pair.
 */
int rdma::Transport::init_resources(struct rdma_addrinfo *rai) {
  int ret, i;
  struct rdma_cm_event *cm_event;

  /* create event channel */
  ctx_->event_channel = rdma_create_event_channel();
  if (!ctx_->event_channel) {
    VERB_ERR("rdma_create_event_channel", errno);
    return errno;
  }
  /* Create an ID used for creating/accessing our SRQ */
  ret =
      rdma_create_id(ctx_->event_channel, &ctx_->srq_id, nullptr, RDMA_PS_TCP);
  if (ret) {
    VERB_ERR("rdma_create_id", ret);
    return ret;
  }

  /* We need to bind the ID to a particular RDMA device
   * This is done by resolving the address or binding to the address */
  printf("[%-16s] Resolving/Binding address\n", "Init");
  if (ctx_->server == false) {
    ret = rdma_resolve_addr(ctx_->srq_id, NULL, rai->ai_dst_addr, 1000);
    if (ret) {
      VERB_ERR("rdma_resolve_addr", ret);
      return ret;
    }
    ret = get_cm_event(ctx_->event_channel, RDMA_CM_EVENT_ADDR_RESOLVED,
                       &cm_event);
    if (ret) {
      VERB_ERR("get_cm_event", ret);
      return ret;
    }
    if (cm_event != nullptr && (ret = rdma_ack_cm_event(cm_event))) {
      VERB_ERR("rdma_ack_cm_event", ret);
      return ret;
    }
  } else {
    ret = rdma_bind_addr(ctx_->srq_id, rai->ai_src_addr);
    if (ret) {
      VERB_ERR("rdma_bind_addr", ret);
      return ret;
    }
  }
  /* Create the memory regions being used in this example */
  printf("[%-16s] Register memory region...\n", "Init");
  ctx_->recv_mr = rdma_reg_msgs(ctx_->srq_id, ctx_->recv_buf, ctx_->msg_length);
  if (!ctx_->recv_mr) {
    VERB_ERR("rdma_reg_msgs recv_mr", -1);
    return -1;
  }
  ctx_->send_mr = rdma_reg_msgs(ctx_->srq_id, ctx_->send_buf, ctx_->msg_length);
  if (!ctx_->send_mr) {
    VERB_ERR("rdma_reg_msgs send_mr", -1);
    return -1;
  }
  if (ctx_->buf) {
    assert(ctx_->server == false && "only client will init the buf here");
    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                   IBV_ACCESS_REMOTE_READ;
    ctx_->buf_mr =
        ibv_reg_mr(ctx_->srq_id->pd, ctx_->buf, ctx_->local_buf_size, mr_flags);
    if (!ctx_->buf_mr) {
      VERB_ERR("rdma_reg_msgs buf_mr", -1);
      return -1;
    }
  }
  /* Create our shared receive queue */
  printf("[%-16s] Create shared receive queue...\n", "Init");
  struct ibv_srq_init_attr srq_attr;
  memset(&srq_attr, 0, sizeof(srq_attr));
  srq_attr.attr.max_wr = ctx_->max_wr;
  srq_attr.attr.max_sge = 1;
  ret = rdma_create_srq(ctx_->srq_id, NULL, &srq_attr);
  if (ret) {
    VERB_ERR("rdma_create_srq", ret);
    return -1;
  }
  /* Save the SRQ in our context so we can assign it to other QPs later */
  ctx_->srq = ctx_->srq_id->srq;
  /* Post our receive buffers on the SRQ */
  printf("[%-16s] Post recv to SRQ...\n", "Init");
  for (i = 0; i < ctx_->max_wr; i++) {
    ret = rdma_post_recv(ctx_->srq_id, NULL, ctx_->recv_buf, ctx_->msg_length,
                         ctx_->recv_mr);
    if (ret) {
      VERB_ERR("rdma_post_recv", ret);
      return ret;
    }
  }

  /* Create a completion channel to use with the SRQ CQ */
  printf("[%-16s] Create Completion Channel & Completion Queue for SRQ ...\n",
         "Init");
  ctx_->srq_cq_channel = ibv_create_comp_channel(ctx_->srq_id->verbs);
  if (!ctx_->srq_cq_channel) {
    VERB_ERR("ibv_create_comp_channel", -1);
    return -1;
  }
  /* Create a CQ to use for all connections (QPs) that use the SRQ */
  ctx_->srq_cq = ibv_create_cq(ctx_->srq_id->verbs, ctx_->max_wr, NULL,
                               ctx_->srq_cq_channel, 0);
  if (!ctx_->srq_cq) {
    VERB_ERR("ibv_create_cq", -1);
    return -1;
  }
  /* Make sure that we get notified on the first completion */
  printf("[%-16s] Make SRQ req notify ...\n", "Init");
  ret = ibv_req_notify_cq(ctx_->srq_cq, 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }

  return 0;
}

/*
 * Function: destroy_resources
 *
 * Description:
 * This function cleans up resources used by the application
 */
void rdma::Transport::destroy_resources() {
  printf("destroy_resources\n");
  int i;
  if (ctx_->conn_ids && not ctx_->server) {
    for (i = 0; i < ctx_->qp_count; i++) {
      uint32_t qp_num = ctx_->conn_ids[i]->qp->qp_num;
      if (ctx_->conn_ids[i] && ctx_->conn_ids[i]->qp &&
          ctx_->conn_ids[i]->qp->state == IBV_QPS_RTS) {
        rdma_disconnect(ctx_->conn_ids[i]);
      }
      // int ret = get_cm_event(ctx_->event_channel, RDMA_CM_EVENT_DISCONNECTED,
      //                    &cm_event);
      rdma_destroy_qp(ctx_->conn_ids[i]);
      rdma_destroy_id(ctx_->conn_ids[i]);
      printf("[%-16s] connection %d (qp_num=%u) destroyed\n", "Info", i,
             qp_num);
    }
    free(ctx_->conn_ids);
  }
  if (ctx_->recv_mr)
    rdma_dereg_mr(ctx_->recv_mr);
  if (ctx_->send_mr)
    rdma_dereg_mr(ctx_->send_mr);
  if (ctx_->buf_mr)
    rdma_dereg_mr(ctx_->buf_mr);
  if (ctx_->recv_buf)
    free(ctx_->recv_buf);
  if (ctx_->send_buf)
    free(ctx_->send_buf);
  if (ctx_->buf)
    free(ctx_->buf);
  if (ctx_->srq_cq)
    ibv_destroy_cq(ctx_->srq_cq);
  if (ctx_->event_channel)
    rdma_destroy_event_channel(ctx_->event_channel);
  if (ctx_->srq_cq_channel)
    ibv_destroy_comp_channel(ctx_->srq_cq_channel);
  if (ctx_->srq_id) {
    rdma_destroy_srq(ctx_->srq_id);
    rdma_destroy_id(ctx_->srq_id);
  }
  if (ctx_->server_name) {
    free(ctx_->server_name);
  }
  delete ctx_;
}

/*
 * Function: await_completion
 *
 * Description:
 * Waits for a completion on the SRQ CQ
 *
 */
int rdma::Transport::await_completion() {
  int ret;
  struct ibv_cq *ev_cq;
  void *ev_ctx;
  /* Wait for a CQ event to arrive on the channel */
  ret = ibv_get_cq_event(ctx_->srq_cq_channel, &ev_cq, &ev_ctx);
  if (ret) {
    VERB_ERR("ibv_get_cq_event", ret);
    return ret;
  }
  ibv_ack_cq_events(ev_cq, 1);
  /* Reload the event notification */
  ret = ibv_req_notify_cq(ctx_->srq_cq, 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }
  return 0;
}

/*
 * Function: run_server
 *
 */
int rdma::Transport::run_server() {
  int ret;
  struct rdma_cm_event *cm_event;

  /* Use the srq_id as the listen_id since it is already setup */
  ctx_->listen_id = ctx_->srq_id;
  ret = rdma_listen(ctx_->listen_id, 4);
  if (ret) {
    VERB_ERR("rdma_listen", ret);
    return ret;
  }
  printf("[%-16s] waiting for connection from client...\n", "Info");

  while (true) {
    // TODO: handle all kinds of event (e.g., disconnect.)
    ret = rdma_get_cm_event(ctx_->event_channel, &cm_event);
    if (ret) {
      VERB_ERR("rdma_get_cm_event", ret);
      return ret;
    }
    printf("[%-16s] cm_event: %s (%d)\n", "CM Event",
           rdma_event_str(cm_event->event), cm_event->event);
    if (cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
      handle_connect_request(cm_event);
      rdma_ack_cm_event(cm_event);
    } else if (cm_event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(cm_event);
    } else if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
      handle_disconnected(cm_event);
    }
  }
}

/*
 * Function: connect_server
 */
int rdma::Transport::connect_server(struct rdma_addrinfo *rai) {
  int ret;
  char *ptr;
  struct ibv_qp_init_attr attr;

  for (int i = 0; i < ctx_->qp_count; i++) {
    memset(&attr, 0, sizeof(attr));
    attr.qp_context = ctx_;
    attr.cap.max_send_wr = ctx_->max_wr;
    attr.cap.max_recv_wr = ctx_->max_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.recv_cq = ctx_->srq_cq;
    attr.srq = ctx_->srq;
    attr.sq_sig_all = 0;

    // create endpoint
    ret = rdma_create_ep(&ctx_->conn_ids[i], rai, NULL, &attr);
    if (ret) {
      VERB_ERR("rdma_create_ep", ret);
      return ret;
    }
    // send connect request
    printf("[%-16s] connecting... (ctx_->conn_ids[%d]=%p)\n", "Info", i,
           ctx_->conn_ids[i]);
    ret = rdma_connect(ctx_->conn_ids[i], NULL);
    if (ret) {
      VERB_ERR("rdma_connect", ret);
      return ret;
    }

    // TODO: this information exchange maybe redundant (in the loop)
    // send requested remote memory information
    ptr = ctx_->send_buf;
    memset(ptr, 0, ctx_->msg_length);
    memcpy(ptr, &ctx_->rm_size, sizeof(ctx_->rm_size));
    ret = send(ctx_->conn_ids[i], ctx_->send_buf, ctx_->msg_length,
               ctx_->send_mr);
    if (ret) {
      VERB_ERR("send", ret);
      return ret;
    }

    // recv remote memory information
    ret = recv(ctx_->conn_ids[i], ctx_->recv_buf, ctx_->msg_length,
               ctx_->recv_mr);
    if (ret) {
      VERB_ERR("recv", ret);
      return ret;
    }
    printf("[%-16s] connected! (ctx_->conn_ids[%d]=%p)\n", "Info", i,
           ctx_->conn_ids[i]);

    ptr = ctx_->recv_buf;
    memcpy(&ctx_->rm_rkey, ptr, sizeof(ctx_->rm_rkey));
    ptr += sizeof(ctx_->rm_rkey);
    memcpy(&ctx_->rm_addr, ptr, sizeof(ctx_->rm_addr));
    ptr += sizeof(ctx_->rm_addr);
    printf("[%-16s] remote memory info: rkey: 0x%x, addr: 0x%lx, size: %lu\n",
           "Info", ctx_->rm_rkey, ctx_->rm_addr, ctx_->rm_size);
  }
  
  return 0;
}

/*
 * Function: handle_connect_request
 *
 * TODO: reject if all conn_ids are connected
 */
int rdma::Transport::handle_connect_request(struct rdma_cm_event *cm_event) {
  int ret;
  struct ibv_qp_init_attr qp_attr;

  ctx_->conn_ids[next_conn_ids_idx] = cm_event->id;
  struct rdma_cm_id *conn_id = ctx_->conn_ids[next_conn_ids_idx];
  ++next_conn_ids_idx;

  /* Create the queue pair */
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_context = ctx_;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.cap.max_send_wr = ctx_->max_wr;
  qp_attr.cap.max_recv_wr = ctx_->max_wr;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.cap.max_inline_data = 0;
  qp_attr.recv_cq = ctx_->srq_cq;
  qp_attr.srq = ctx_->srq;
  qp_attr.sq_sig_all = 0;
  ret = rdma_create_qp(conn_id, NULL, &qp_attr);
  printf("[%-16s] create qp_num: %u\n", "Connection", conn_id->qp->qp_num);
  if (ret) {
    VERB_ERR("rdma_create_qp", ret);
    return ret;
  }
  /* Set the new connection to use our SRQ */
  conn_id->srq = ctx_->srq;
  ret = rdma_accept(conn_id, NULL);
  if (ret) {
    VERB_ERR("rdma_accept", ret);
    return ret;
  }

  // recv requested remote memory information
  ret = recv(conn_id, ctx_->recv_buf, ctx_->msg_length, ctx_->recv_mr);
  if (ret) {
    VERB_ERR("recv", ret);
    return ret;
  }
  memcpy(&ctx_->rm_size, ctx_->recv_buf, sizeof(ctx_->rm_size));

  // allocate remote memory
  ctx_->buf = (char *)malloc(ctx_->rm_size);
  memset(ctx_->buf, 0, ctx_->rm_size);
  int mr_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
  ctx_->buf_mr =
      ibv_reg_mr(ctx_->srq_id->pd, ctx_->buf, ctx_->rm_size, mr_flags);
  if (!ctx_->buf_mr) {
    VERB_ERR("rdma_reg_msgs buf_mr", -1);
    return -1;
  }

  // send back remote memory information
  char *ptr = ctx_->send_buf;
  memset(ptr, 0, ctx_->msg_length);
  memcpy(ptr, &ctx_->buf_mr->rkey, sizeof(ctx_->rm_rkey));
  ptr += sizeof(ctx_->rm_rkey);
  uint64_t addr = (uint64_t)ctx_->buf_mr->addr;
  memcpy(ptr, &addr, sizeof(ctx_->rm_addr));

  ret = send(conn_id, ctx_->send_buf, ctx_->msg_length, ctx_->send_mr);
  if (ret) {
    VERB_ERR("send", ret);
    return ret;
  }
  printf("[%-16s] sent rm info to %u (rkey=0x%x, rm_addr=0x%lx, rm_size=%lu)\n",
         "Info", conn_id->qp->qp_num, ctx_->buf_mr->rkey, addr, ctx_->rm_size);

  return 0;
}

/*
 * Function: handle_disconnected
 *
 */
int rdma::Transport::handle_disconnected(struct rdma_cm_event *cm_event) {
  int ret;
  struct rdma_cm_event *event;
  struct rdma_cm_id *conn_id = cm_event->id;
  rdma_ack_cm_event(cm_event);

  uint32_t qp_num = conn_id->qp->qp_num;
  if (conn_id->qp && conn_id->qp->state == IBV_QPS_RTS) {
    ret = rdma_disconnect(conn_id);
    if (ret) {
      VERB_ERR("rdma_disconnect", ret);
      return ret;
    }
    ret =
        get_cm_event(ctx_->event_channel, RDMA_CM_EVENT_TIMEWAIT_EXIT, &event);
    if (ret) {
      VERB_ERR("get_cm_event", ret);
      return ret;
    }
    if (cm_event != nullptr && (ret = rdma_ack_cm_event(cm_event))) {
      VERB_ERR("rdma_ack_cm_event", ret);
      return ret;
    }
  }
  rdma_destroy_qp(conn_id);
  rdma_destroy_id(conn_id);
  printf("[%-16s] disconnected! (qp_num=%u)\n", "Connection", qp_num);

  return 0;
}

/*
 * Function: read_rm
 *
 * Input:
 */
int rdma::Transport::read_rm(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
                             struct ibv_mr *lm_mr, uint64_t rm_addr,
                             uint32_t rm_rkey) {
  int ret, ne;
  struct ibv_wc wc = {};
  ret = rdma_post_read(cm_id, /*context=*/nullptr, lm_addr, lm_length, lm_mr,
                       /*flags=*/IBV_SEND_SIGNALED, rm_addr, rm_rkey);
  if (ret) {
    VERB_ERR("rdma_post_read", ret);
    return ret;
  }
  // printf("[%-16s] post read:  rkey=0x%x, rm_addr=0x%lx, length=%lu\n", "Info",
  //        rm_rkey, rm_addr, lm_length);

  do {
    ne = ibv_poll_cq(cm_id->send_cq, 1, &wc);
    if (ne < 0) {
      VERB_ERR("ibv_poll_cq", ne);
      return ne;
    }

    if (wc.status != IBV_WC_SUCCESS) {
      VERB_ERR("ibv_poll_cq", wc.status);
      return wc.status;
    }
  } while (ne == 0);

  return 0;
}

/*
 * Function: write_rm
 *
 * Input:
 */
int rdma::Transport::write_rm(rdma_cm_id *cm_id, char *lm_addr,
                              size_t lm_length, struct ibv_mr *lm_mr,
                              uint64_t rm_addr, uint32_t rm_rkey) {
  int ret, ne;
  struct ibv_wc wc = {};
  ret = rdma_post_write(cm_id, /*context=*/nullptr, lm_addr, lm_length, lm_mr,
                        /*flags=*/IBV_SEND_SIGNALED, rm_addr, rm_rkey);
  if (ret) {
    VERB_ERR("rdma_post_write", ret);
    return ret;
  }
  // printf("[%-16s] post write: rkey=0x%x, rm_addr=0x%lx, length=%lu\n", "Info",
  //        rm_rkey, rm_addr, lm_length);

  do {
    ne = ibv_poll_cq(cm_id->send_cq, 1, &wc);
    if (ne < 0) {
      VERB_ERR("ibv_poll_cq", ne);
      return ne;
    }

    if (wc.status != IBV_WC_SUCCESS) {
      VERB_ERR("ibv_poll_cq", wc.status);
      return wc.status;
    }
  } while (ne == 0);

  return 0;
}

/*
 * Function: send
 *
 * Input:
 */
int rdma::Transport::send(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
                          struct ibv_mr *send_mr) {
  int ret, ne;
  struct ibv_wc wc = {};

  ret = rdma_post_send(cm_id, /*context=*/nullptr, lm_addr, lm_length, send_mr,
                       IBV_SEND_SIGNALED);
  if (ret) {
    VERB_ERR("rdma_post_send", ret);
    return ret;
  }
  do {
    ne = rdma_get_send_comp(cm_id, &wc);
    if (ne < 0) {
      VERB_ERR("rdma_get_send_comp", ret);
      return ret;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      VERB_ERR("rdma_get_send_comp", wc.status);
      return wc.status;
    }
  } while (ne == 0);

  return 0;
}

/*
 * Function: recv
 *
 * Input:
 */
int rdma::Transport::recv(rdma_cm_id *cm_id, char *lm_addr, size_t lm_length,
                          struct ibv_mr *recv_mr) {
  int ret, ne;
  struct ibv_wc wc = {};

  memset(ctx_->recv_buf, 0, ctx_->msg_length);
  ret = await_completion();
  if (ret) {
    printf("await_completion %d\n", ret);
    return ret;
  }
  do {
    ne = ibv_poll_cq(ctx_->srq_cq, 1, &wc);
    if (ne < 0) {
      VERB_ERR("ibv_poll_cq", ne);
      return ne;
    } else if (ne == 0)
      break;
    if (wc.status != IBV_WC_SUCCESS) {
      printf("work completion status %s\n", ibv_wc_status_str(wc.status));
      return -1;
    }

    ret = rdma_post_recv(ctx_->srq_id, (void *)wc.wr_id, ctx_->recv_buf,
                         ctx_->msg_length, ctx_->recv_mr);
    if (ret) {
      VERB_ERR("rdma_post_recv", ret);
      return ret;
    }
  } while (ne);

  return 0;
}
