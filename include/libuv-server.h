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
    uint32_t id;
    uint32_t client_req_id;
    uv_handle_t * client;
    int num_replies;
    UT_hash_handle hh;
}request_mapper_t;

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
    uint32_t client_req_id;
    int num_slaves;
    proxy_slave_t * slave_hash;
    request_mapper_t * req_hash;
    uv_async_t resp_async;
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
    connect_status_t status;
}connection_info_t;

typedef struct 
{
    uv_work_t req;
    connection_info_t * cinfo;
}req_work_t;

#define DO_WORK()  usleep(100 * 1000)
int is_connection_active(client_info_t * client);
client_info_t * proxy_slave_init(const char * addr, int port, uint32_t slave_num, uv_handle_t * server);
int request_mapper_reply_dec(server_info_t * server, uint32_t req_id);
void proxy_slave_send(client_info_t * client, const uint8_t * req, uint32_t len);
void wakeup_response_async_cb(server_info_t * server);

#endif /*_LIBUV_SERVER_H_INCLUDED_ */
