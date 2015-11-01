#ifndef __LIBUV_WRAP_H_INCLUDED__
#define __LIBUV_WRAP_H_INCLUDED__

#include <stdio.h>
#include <uv.h>
#include <uthash.h>

#include "request_response.h"
#include "utils.h"

#define HEADER_MAGIC (0xDEADBEEF)

#define DBG_ERR(fmt...) printf(fmt)
#define DBG_INFO(fmt...) printf(fmt)
#define DBG_PRINT(fmt...) printf(fmt)
#define DBG_FUNC_ENTER()  printf("Enter %s:%d\n", __FUNCTION__, __LINE__)
#define DBG_FUNC_EXIT()  printf("Exit %s:%d\n", __FUNCTION__, __LINE__)
#define DBG_VERBOSE(fmt...) printf(fmt)

typedef enum {
    ON_CONNECT_CB = 1,
}cb_id_t;

typedef enum {
    IN_PROGRESS = 1,
    DONE = 2,
}call_status_t;

typedef struct {
    cb_id_t cb_id;
    int err;
}error_info_t;

typedef struct {
    uv_write_t req;
    uv_buf_t   buf;
    uv_mutex_t mutex;
    uv_cond_t cond;
    int status;
    call_status_t progress;
}write_req_t;

typedef struct {
    uint32_t  header_len;
    pkt_hdr_t hdr;
    uint8_t * buf;
}response_t;

typedef struct {
    int id;
    call_status_t progress;
    uv_mutex_t mutex;
    uv_cond_t  cond;
    response_t * resp;
    UT_hash_handle hh;
}response_header_t;

typedef struct {
    char * buf;
    ssize_t len;
    ssize_t offset;
}res_buf_t;

typedef enum { ACTIVE = 1, CLOSED = 2} connect_status_t;

typedef struct {
    uv_connect_t req;
    connect_status_t status;
    unsigned magic;
    uv_stream_t * handle;
    uv_thread_t tid_io_loop;
    uv_thread_t tid_resp_split;
    uv_thread_t tid_res_mapper;
    uv_mutex_t  mutex;
    uv_cond_t   cond;
    uv_tcp_t    tcp_client;
    uv_loop_t   * loop;
    error_info_t error;
    uv_sem_t    sem;
    call_status_t progress;
    int req_id;
    queue_t    * buf_q;
    queue_t    * res_q;
    response_header_t * hash;
    uv_idle_t idle;
}client_info_t;

typedef void * handle_t;
int libuv_connect(const char * addr, int port, handle_t * handle);
int libuv_send(handle_t handle, const uint8_t * data, uint32_t len, int * req_id);
int libuv_recv(handle_t handle, int req_id, uint8_t * data, uint32_t nread, int * more);
int libuv_disconnect(handle_t handle);

#endif /*__LIBUV_WRAP_H_INCLUDED__*/
