#ifndef _LIBUV_SERVER_H_INCLUDED_
#define _LIBUV_SERVER_H_INCLUDED_ 

#include <uv.h>

typedef struct 
{
    uv_tcp_t  tcp_server;
    uv_mutex_t mutex;
    uv_cond_t  cond;
    uint32_t cid;
    uv_thread_t tid;
}server_info_t;

typedef struct 
{
    uv_tcp_t client;
    uint32_t cid;
}connection_info_t;

#endif /*_LIBUV_SERVER_H_INCLUDED_ */
