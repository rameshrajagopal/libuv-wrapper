#ifndef _LIBUV_SERVER_H_INCLUDED_
#define _LIBUV_SERVER_H_INCLUDED_ 

#include <uv.h>
#include <request_response.h>

#define MAX_NUM_WORKER_THREADS (10)

typedef struct 
{
    uv_tcp_t  tcp_server;
    uv_mutex_t mutex;
    uv_cond_t  cond;
    uint32_t cid;
    uv_thread_t tid;
    queue_t * req_q;
    uv_thread_t tids[MAX_NUM_WORKER_THREADS];
}server_info_t;

typedef struct 
{
    uv_tcp_t client;
    uint32_t cid;
    queue_t * buf_q;
}connection_info_t;

typedef struct 
{
    uv_work_t req;
    connection_info_t * cinfo;
}req_work_t;

#define DO_WORK()  usleep(100 * 1000)

#endif /*_LIBUV_SERVER_H_INCLUDED_ */
