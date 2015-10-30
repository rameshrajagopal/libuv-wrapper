#ifndef __LIBUV_WRAP_H_INCLUDED__
#define __LIBUV_WRAP_H_INCLUDED__

#include <stdio.h>
#include <uv.h>

#define HEADER_MAGIC (0xDEADBEEF)

#define DBG_ERR(fmt...) printf(fmt)
#define DBG_PRINT(fmt...) printf(fmt)
#define DBG_FUNC_ENTER()  printf("Enter %s:%d\n", __FUNCTION__, __LINE__)
#define DBG_FUNC_EXIT()  printf("Exit %s:%d\n", __FUNCTION__, __LINE__)

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
    uv_mutex_t cond;
}write_req_t;

typedef struct {
    uv_connect_t req;
    unsigned magic;
    uv_thread_t thread_id;
    uv_mutex_t  mutex;
    uv_cond_t   cond;
    uv_tcp_t    tcp_client;
    uv_loop_t   * loop;
    error_info_t error;
    uv_sem_t    sem;
    call_status_t status;
}client_info_t;

typedef void * handle_t;
int libuv_connect(const char * addr, int port, handle_t * handle);
int libuv_send(handle_t handle, const uint8_t * data, uint32_t len);
int libuv_recv(handle_t handle, uint8_t * data, uint32_t nread);
int libuv_disconnect(handle_t handle);

#endif /*__LIBUV_WRAP_H_INCLUDED__*/
