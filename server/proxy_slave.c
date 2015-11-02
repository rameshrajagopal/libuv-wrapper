#include "libuv-wrap.h"
#include "libuv-server.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

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

void response_split_task(void * arg)
{
    client_info_t * client = arg;

    DBG_FUNC_ENTER();
    DBG_VERBOSE("%s: client: %p\n", __FUNCTION__, client);
    /* variables for handling response split */
    char temp[1024];
    uint32_t temp_len = 0;
    read_stages_t stage = HEADER_LEN_READ;
    uint32_t payload_offset = 0;
    response_t * resp = NULL;
    uint32_t rem_size = 0;
    txn_buf_t * cur_buf = NULL;
    while (1) {
        if (cur_buf != NULL) {
            free(cur_buf->buf);
            free(cur_buf);
            cur_buf = NULL;
        }
        cur_buf = (txn_buf_t *)queue_pop(client->buf_q);
        DBG_VERBOSE("%s: buf: %p len: %ld offset: %ld\n", __FUNCTION__, cur_buf->buf, cur_buf->len, cur_buf->offset);
        /* write a response split using fixed len */
        rem_size = cur_buf->len - cur_buf->offset + temp_len;
        DBG_VERBOSE("rem_size: %u\n", rem_size);
        if (temp_len > 0) {
            if (rem_size > 0) {
                switch(stage) {
                    case HEADER_LEN_READ:
                    {
                        uint32_t hdr_len = 0;
                        memcpy((uint8_t *)&hdr_len, temp, temp_len);
                        memcpy(((uint8_t *)&hdr_len) + temp_len, cur_buf->buf + cur_buf->offset, HEADER_SIZE_LEN - temp_len);
                        DBG_VERBOSE("HEADER length: %u\n", hdr_len);
                        resp->header_len = hdr_len;
                        temp_len = 0;
                        rem_size -= HEADER_SIZE_LEN;
                        stage = HEADER_READ;
                        break;
                    }
                    case HEADER_READ:
                    {
                        memcpy((uint8_t *)&resp->hdr, temp, temp_len);
                        memcpy(((uint8_t *)&resp->hdr) + temp_len, cur_buf->buf + cur_buf->offset, resp->header_len - temp_len);
                        DBG_VERBOSE("REQUEST id: %u\n", resp->hdr.id);
                        temp_len = 0;
                        rem_size -= resp->header_len;
                        stage = PAYLOAD_READ;
                        break;
                    }
                    default:
                        DBG_VERBOSE("INVALID case needs to be find out");
                        assert(0);
                        break;
                }
            }
        }
        while (rem_size > 0) {
            switch(stage) {
                case HEADER_LEN_READ:
                    if (rem_size < HEADER_SIZE_LEN){
                        memcpy(temp, cur_buf->buf + cur_buf->offset, rem_size);
                        temp_len = rem_size;
                        cur_buf->offset += rem_size;
                        rem_size = 0;
                    } else {
                        resp = malloc(sizeof(response_t));
                        assert(resp != NULL);
                        DBG_PRINT("Allocated resp: %p\n", resp);
                        resp->buf = NULL;

                        read_uint32_t((uint8_t *)cur_buf->buf + cur_buf->offset, HEADER_SIZE_LEN, &resp->header_len);
                        cur_buf->offset += HEADER_SIZE_LEN;
                        rem_size -= HEADER_SIZE_LEN;
                        DBG_VERBOSE("HEADER: %u\n", resp->header_len);
                        stage = HEADER_READ;
                    }
                    break;
                case HEADER_READ:
                    if (rem_size < resp->header_len) {
                        memcpy(temp, cur_buf->buf + cur_buf->offset, rem_size);
                        temp_len = rem_size;
                        cur_buf->offset += rem_size;
                        rem_size = 0;
                    } else {
                        read_pkt_hdr((uint8_t *)cur_buf->buf + cur_buf->offset, resp->header_len, &resp->hdr);
                        DBG_VERBOSE("HEADER: %x %x %x %x\n", resp->hdr.magic, resp->hdr.len, resp->hdr.id, resp->hdr.future);
                        rem_size -= resp->header_len;
                        cur_buf->offset += resp->header_len;
                        stage = PAYLOAD_READ;
                    }
                    break;
                case PAYLOAD_READ:
                    if (payload_offset > 0) {
                        size_t len_to_read = resp->hdr.len - payload_offset;
                        if (rem_size < len_to_read) {
                            memcpy(resp->buf + payload_offset, cur_buf->buf + cur_buf->offset, rem_size);
                            payload_offset += rem_size;
                            cur_buf->offset += rem_size;
                            rem_size = 0;
                        } else {
                            memcpy(resp->buf + payload_offset, cur_buf->buf + cur_buf->offset, len_to_read);
                            payload_offset = 0;
                            rem_size -= len_to_read;
                            cur_buf->offset += len_to_read;
                        }
                    } else {
                        resp->buf = malloc(resp->hdr.len);
                        assert(resp->buf != NULL);

                        if (rem_size < resp->hdr.len) {
                            memcpy(resp->buf, cur_buf->buf + cur_buf->offset, rem_size);
                            payload_offset = rem_size;
                            rem_size = 0;
                            cur_buf->offset += rem_size;
                        } else {
                            memcpy(resp->buf, cur_buf->buf + cur_buf->offset, resp->hdr.len);
                            rem_size -= resp->hdr.len;
                            cur_buf->offset += resp->hdr.len;
                        }
                    }
                    /* response is done, push it to the queue */
                    DBG_VERBOSE("GOT THE RESPONSE for ID: %u\n", resp->hdr.id);
                    queue_push(client->res_q, (void *)resp);
                    DBG_VERBOSE("Pushed response %p to Queue\n", resp);
                    resp = NULL;
                    stage = HEADER_LEN_READ;
                    break;
                default:
                    DBG_ERR("Invalid stage\n");
                    assert(0);
            }
        }
    }
    DBG_FUNC_EXIT();
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

