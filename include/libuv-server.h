#ifndef _LIBUV_SERVER_H_INCLUDED_
#define _LIBUV_SERVER_H_INCLUDED_ 

#include <uv.h>
#include <request_response.h>

#define MAX_NUM_WORKER_THREADS (10)

typedef struct 
{
    int id;
    client_info_t * client;
    UT_hash_handle hh;
}proxy_slave_t;

typedef struct 
{
    uv_tcp_t  tcp_server;
    uv_mutex_t mutex;
    uv_cond_t  cond;
    uint32_t cid;
    uv_thread_t tid;
    queue_t * req_q;
    queue_t * res_q;
    uv_thread_t tids[MAX_NUM_WORKER_THREADS];
    uint32_t client_req_num;
    int num_slaves;
    proxy_slave_t * slave_hash;
}server_info_t;

typedef struct 
{
   uv_work_t req;
   server_info_t * server;
}resp_work_t;

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
int is_connection_active(client_info_t * client);
client_info_t * proxy_slave_init(const char * addr, int port, uint32_t slave_num, uv_handle_t * server);

#endif /*_LIBUV_SERVER_H_INCLUDED_ */
