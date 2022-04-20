#include "rdma_utils.h"
#include <iostream>

#define VERB_ERR(verb, ret)                                                    \
  fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)

void rdma::print_cm_id(struct rdma_cm_id *id) {
  if (id == nullptr) {
    printf("[%-16s] id is nullptr\n", "Error");
    return;
  }
  if (id->verbs) {
    printf("verbs=(device=%s, cmd_fd=%d, async_fd=%d, num_comp_vectors=%d)\n",
           ibv_get_device_name(id->verbs->device), id->verbs->cmd_fd,
           id->verbs->async_fd, id->verbs->num_comp_vectors);
  } else {
    printf("verbs=%p\n", id->verbs);
  }
  printf("channel=%p\n", id->channel);
  printf("context=%p\n", id->context);
  if (id->qp) {
    printf("qp=(qp_num=%u, qp_type=%d)\n", id->qp->qp_num, id->qp->qp_type);
  } else {
    printf("qp=%p\n", id->qp);
  }
  printf("port_num=%d\n", id->port_num);
  printf("ps=%d\n", id->ps);
  printf("event=%p\n", id->event);
  printf("send_cq_channel=%p\n", id->send_cq_channel);
  printf("send_cq=%p\n", id->send_cq);
  printf("recv_cq_channel=%p\n", id->recv_cq_channel);
  printf("recv_cq=%p\n", id->recv_cq);
  printf("srq=%p\n", id->srq);
  printf("pd=%p\n", id->pd);
  printf("qp_type=%d\n", id->qp_type);
}

std::string rdma::char_array_to_string(char *array, int size) {
  char *char_str = new char[size + 1];
  memset(char_str, 0, size+1);
  memcpy(char_str, array, size);
  std::string str = std::string(char_str);
  return str;
}

void print_string_as_hex(char *str, int len) {
  for (int i = 0; i < len; i++) {
    printf("%02x ", (uint8_t)str[i]);
  }
}

/*
 * Function: get_cm_event
 *
 * Input:
 * channel The event channel
 * type The event type that is expected
 *
 * Output:
 * out_ev The event will be passed back to the caller, if desired
 * Set this to NULL and the event will be acked automatically
 * Otherwise the caller must ack the event using rdma_ack_cm_event
 *
 * Returns:
 * 0 on success, non-zero on failure
 *
 * Description:
 * Waits for the next CM event and check that is matches the expected
 * type.
 */
int rdma::get_cm_event(struct rdma_event_channel *channel,
                       enum rdma_cm_event_type type,
                       struct rdma_cm_event **out_ev) {
  int ret = 0;
  struct rdma_cm_event *event = NULL;
  ret = rdma_get_cm_event(channel, &event);
  if (ret) {
    VERB_ERR("rdma_resolve_addr", ret);
    return -1;
  }
  /* Verify the event is the expected type */
  if (event->event != type) {
    printf("event: %s, status: %d\n", rdma_event_str(event->event),
           event->status);
    ret = -1;
  }
  /* Pass the event back to the user if requested */
  if (!out_ev) {
    rdma_ack_cm_event(event);
  } else {
    *out_ev = event;
  }
  return ret;
}