#ifndef _UTILS_H_INCLUDED_
#define _UTILS_H_INCLUDED_

#include <uv.h>
#define  WORKER_ADDRESS  "192.168.0.241"
#define  WORKER_PORT  7000
#define  SERVER_ADDRESS  "192.168.0.241"
#define  SERVER_PORT  7000

#define DBG()  //printf("%s:%d\n", __FUNCTION__, __LINE__)
#define DBG_PRINT(fmt...) printf(fmt)
#define DBG_PRINT_ERR(fmt...) printf(fmt)
#define DBG_PRINT_INFO(fmt...) printf(fmt)
#define DBG_LOG(fmt...) printf(fmt)

typedef void (*on_connection_callback)(uv_stream_t *, int);

typedef int bool;

struct request 
{
    uv_work_t req;
    int client_req_num;
    uv_handle_t * handle;
    const uv_buf_t * buf;
    ssize_t nread;
    int nrequests;
};

typedef struct queue_data_s
{
    const void * data;
    struct queue_data_s * next;
}queue_data_t;

typedef struct queue_s
{
    int cnt;
    struct queue_data_s * head;
    struct queue_data_s * tail;
    uv_mutex_t mutex;
    uv_cond_t cond;
}queue_t;

queue_t * queue_init(void);
void queue_push(queue_t * q, const void * data);
const void * queue_pop(queue_t * q);
void queue_push_front(queue_t * q, const void * data);
bool is_empty(queue_t * q);
void queue_deinit(queue_t * q);

typedef struct worker_data_s
{
    uv_tcp_t client;
    uv_stream_t * handle;
    queue_t * q;
}worker_data_t;

typedef struct work_request_s
{
    uv_work_t req;
    worker_data_t * wdata;
}work_request_t;


#endif /*_UTILS_H_INCLUDED_*/
