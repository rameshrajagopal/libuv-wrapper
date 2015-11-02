#include "libuv-wrap.h"
#include "libuv-server.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <request_response.h>

static void req_async_send_cb(uv_async_t * async);
static void libuv_idle_cb(uv_idle_t * handle)
{
    /* do nothing here, if possible, we can include some general check like
     * heartbeat or anything that sort
     */
}

static void on_connect_cb(uv_connect_t * req, int status)
{
    client_info_t * client = (client_info_t *) req;

    DBG_FUNC_ENTER();
    DBG_PRINT("%s client: %p\n", __FUNCTION__, client);
    if (status < 0) {
        DBG_ERR("%s: listen err %s\n", __FUNCTION__, uv_strerror(status));
    } else {
        DBG_VERBOSE("connection is established\n");
        client->status = ACTIVE;
        client->handle = (uv_stream_t *)client->req.handle;
    }
    DBG_PRINT("%s: handle: %p %p\n", __FUNCTION__, client->handle, &client->tcp_client);
    DBG_FUNC_EXIT();
}

static void close_cb(uv_handle_t * handle)
{
    uv_key_t * key = (uv_key_t *)((uint8_t *)handle + sizeof(uv_tcp_t));
    client_info_t * client = uv_key_get(key);
    DBG_FUNC_ENTER();
    uv_mutex_lock(&client->mutex);
    client->status = CLOSED;
    uv_mutex_unlock(&client->mutex);
    /* need to check how to go about freeing this info 
     */
    DBG_FUNC_EXIT();
}

static void data_read_cb(uv_stream_t * stream, ssize_t nread, const uv_buf_t * buf)
{
    DBG_FUNC_ENTER();
    /* FIXME replace with offset macro 
     *  ((uint8_t *)stream - OFFSET(tcp_client)) + OFFSET(client_key)
     */
    uv_key_t * key = (uv_key_t *)((uint8_t *)stream + sizeof(uv_tcp_t));
    client_info_t * client = uv_key_get(key);

    assert(client != NULL);
    if (nread < 0) {
        DBG_ERR("error on read\n");
        free(buf->base);
        uv_close((uv_handle_t *)stream, close_cb);
        return;
    }
    DBG_VERBOSE("stream: %p nread: %ld buf: %p\n", stream, nread, buf);
    txn_buf_t * res = txn_buf_init(buf->base, nread);
    assert(res != NULL);
    queue_push(client->buf_q, (void *)res);
    DBG_FUNC_EXIT();
}

static void alloc_buffer_cb(uv_handle_t * handle, size_t size, uv_buf_t * buf)
{
    DBG_FUNC_ENTER();
    *buf = uv_buf_init((char *) malloc(size), size);
    DBG_VERBOSE("handle: %p buf %p \n", handle, buf);
    DBG_FUNC_EXIT();
}

static void write_end_cb(uv_write_t * req, int status)
{
    write_req_t * wr = (write_req_t *) req;
    DBG_FUNC_ENTER();
    if (status < 0) {
        DBG_ERR("write err %s\n", uv_strerror(status));
    }
    uv_read_start(wr->req.handle, alloc_buffer_cb, data_read_cb);
    /* free the memory */
    DBG_ALLOC("FREE : wr : %p\n", wr);
    free(wr);
    DBG_FUNC_EXIT();
}


static void client_io_loop(void * arg)
{
    DBG_FUNC_ENTER();
    client_info_t * client = arg;
    DBG_PRINT("%s: client: %p\n", __FUNCTION__, client);
    uv_key_set(&client->client_key, arg);
    /* increment the counter to keep the loop alive */
    uv_run(client->loop, UV_RUN_DEFAULT);
    DBG_FUNC_EXIT();
}

static int tcp_client_init(const char * master_addr, int port, client_info_t * client)
{
    struct sockaddr_in addr;
    int r;

    DBG_FUNC_ENTER();
    uv_ip4_addr(master_addr, port, &addr);
    /* initlize loop */
    client->loop = malloc(sizeof(uv_loop_t));
    assert(client->loop != NULL);
    uv_loop_init(client->loop);
    /* register async cb */
    r = uv_async_init(client->loop, &client->req_async, req_async_send_cb);
    assert(r == 0);
    /* connect to the server */
    uv_tcp_init(client->loop, &client->tcp_client);
    r = uv_tcp_connect(&client->req, &client->tcp_client, 
                       (struct sockaddr *) &addr, on_connect_cb);
    if (r) {
        DBG_ERR("listen err %s\n", uv_strerror(r));
        return -1;
    }
    DBG_FUNC_EXIT();
    return 0;
}

client_info_t * client_info_init(uint32_t slave_num, uv_handle_t * server)
{
    int r;
    DBG_FUNC_ENTER();
    client_info_t * client = malloc(sizeof(client_info_t));
    assert(client != NULL);

    client->magic = HEADER_MAGIC;
    r = uv_mutex_init(&client->mutex);
    assert(r == 0);

    r = uv_cond_init(&client->cond);
    assert(r == 0);

    client->buf_q = queue_init();
    assert(client->buf_q != NULL);

    client->res_q = queue_init();
    assert(client->res_q != NULL);

    client->req_buf_q = queue_init();
    assert(client->req_buf_q != NULL);


    client->hash = NULL;
    client->req_id = 0;
    client->server = server;
    client->slave_num = 0;

    r = uv_key_create(&client->client_key);
    assert(r == 0);

    DBG_FUNC_EXIT();
    return client;
}

static int libuv_idle_start(client_info_t * client)
{
    int r = uv_idle_init(client->loop, &client->idle);
    assert(r == 0);

    r = uv_idle_start(&client->idle, libuv_idle_cb);
    assert(r == 0);
    return r;
}

void response_mapper_task(void * arg)
{
    client_info_t * client = arg;

    DBG_FUNC_ENTER();
    DBG_VERBOSE("%s: client: %p\n", __FUNCTION__, client);
    while (1) {
        response_t * resp = (response_t *)queue_pop(client->res_q);
        /* take out the resp id */
        int cnt = request_mapper_reply_dec((server_info_t *)client->server, resp->hdr.id);
        DBG_VERBOSE("Pop response : %p num_replies: %d\n", resp, cnt);
        if (cnt == 0) {
            /* merge out, do whatever you want to do with the responses you 
             * have got, merge or join or etc
             */
            queue_push(((server_info_t *)((client_info_t *)client)->server)->res_q, resp);
            wakeup_response_async_cb((server_info_t *)((client_info_t *)client)->server);
        }
    }
    DBG_FUNC_EXIT();
}

int is_connection_active(client_info_t * client)
{
    uv_mutex_lock(&client->mutex);
    int status = (client->status == ACTIVE) ? 1 : 0;
    uv_mutex_unlock(&client->mutex);
    return status;
}

client_info_t * proxy_slave_init(const char * addr, int port, uint32_t slave_num, uv_handle_t * server)
{
    client_info_t * client = NULL; 
    int ret;

    DBG_FUNC_ENTER();
    client = client_info_init(slave_num, server);
    assert(client != NULL);

    ret = tcp_client_init(addr, port, client);
    assert(ret == 0);
    /* create a io thread */
    ret = uv_thread_create(&client->tid_io_loop, client_io_loop, (void *)client);
    assert(ret == 0);
    /* thread for splitting response */
    ret = uv_thread_create(&client->tid_resp_split, response_split_task, (void *)client);
    assert(ret == 0);
    /* thread for mapping req->response */
    ret = uv_thread_create(&client->tid_res_mapper, response_mapper_task, (void *)client);
    assert(ret == 0);
    /* idle loop start */
    ret = libuv_idle_start(client);
    assert(ret == 0);

    DBG_FUNC_EXIT();
    return client;
}

static inline write_req_t * write_req_init(void)
{
    write_req_t * wr = malloc(sizeof(write_req_t));
    assert(wr != NULL);
    return wr;
}

static void req_async_send_cb(uv_async_t * async)
{
    client_info_t * client = (client_info_t *) async->data;
    while (!is_empty(client->req_buf_q)) {
        uv_buf_t * buf = (uv_buf_t *) queue_pop(client->req_buf_q);
        if (client->status != CLOSED) {
            /* write the data on to the tcp client req */
            write_req_t * wr = write_req_init();
            assert(wr != NULL);
            DBG_ALLOC("ALLOC wr: %p\n", wr);
            wr->buf.base = buf->base;
            wr->buf.len  = buf->len;

            int ret = uv_write(&wr->req, client->handle, &wr->buf, 1, write_end_cb);
            assert(ret == 0);
        }
        DBG_ALLOC("FREE req_buf: %p\n", buf);
        free(buf);
    }
}

void proxy_slave_send(client_info_t * client, const uint8_t * req, uint32_t len)
{
    DBG_FUNC_ENTER();
    uv_buf_t * buf = malloc(sizeof(uv_buf_t));
    assert(buf != NULL);
    buf->base = (char *)req;
    buf->len  = len;
    queue_push(client->req_buf_q, (void *)buf);
    /* wake up req async send callback */
    client->req_async.data = (void *)client;
    uv_async_send(&client->req_async);
    DBG_FUNC_EXIT();
}

int libuv_disconnect(handle_t handle) 
{
   /* if the status of client->status is closed, then just deinitilizes the
    * client handle 
    * send uv_close, once you receive the close cb and do the below */
   /* close idle handler, so io loop returns, so no more callbacks */
   /* put a dummy entry into buf_q and set flag to exit */
   /* put a dummy entry into res_q and set flag to exit */
   /* delete the entries into the queue */
   /* deinit all handle related ds */
   return 0;        
}

