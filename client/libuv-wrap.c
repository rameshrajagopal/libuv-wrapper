#include "libuv-wrap.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <uthash.h>

static void find_response_header(int id, response_header_t **res, response_header_t ** hash);
static write_req_t * write_req_init(void);

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
    client->error.cb_id = ON_CONNECT_CB;
    client->error.err = status;
    if (status < 0) {
        DBG_ERR("listen err %s\n", uv_strerror(status));
    } else {
        client->status = ACTIVE;
    }
    /* notify the connect caller */
    client->progress = DONE;
    client->handle = (uv_stream_t *)client->req.handle;
    uv_cond_signal(&client->cond);
    DBG_PRINT("%s: handle: %p %p\n", __FUNCTION__, client->handle, &client->tcp_client);
    DBG_FUNC_EXIT();
}

static void close_cb(uv_handle_t * handle)
{
    DBG_FUNC_ENTER();
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
        DBG_VERBOSE("error on read\n");
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
    DBG_PRINT("%s write req: %p\n", __FUNCTION__, wr);
    if (status < 0) {
        DBG_ERR("write err %s\n", uv_strerror(status));
    }
#if 0
    uv_mutex_lock(&wr->mutex);
    wr->status = status;
    wr->progress = DONE;
    uv_mutex_unlock(&wr->mutex);
    uv_cond_signal(&wr->cond);
#endif
    /* register the callback for reading */
    uv_read_start(wr->req.handle, alloc_buffer_cb, data_read_cb);
    /* free the memory */
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

client_info_t * client_info_init(void)
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

    r = uv_key_create(&client->client_key);
    assert(r == 0);

    DBG_FUNC_EXIT();
    return client;
}

static void wait_for_response(client_info_t * client)
{
    DBG_FUNC_ENTER();
    uv_mutex_lock(&client->mutex);
    while (client->progress == IN_PROGRESS) {
        uv_cond_wait(&client->cond, &client->mutex);
    }
    uv_mutex_unlock(&client->mutex);
    DBG_FUNC_EXIT();
}

static void clear_error(error_info_t * error)
{
    memset(error, '\0', sizeof(error_info_t));
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
        DBG_VERBOSE("Pop response : %p\n", resp);
        /* get the info from hash map using req_id */
        response_header_t * resp_header = NULL;
        find_response_header(resp->hdr.id, &resp_header, &client->hash);
        assert(resp_header != NULL);
        /* notify the waiter */
        uv_mutex_lock(&resp_header->mutex);
        resp_header->resp = resp;
        resp_header->progress = DONE;
        uv_mutex_unlock(&resp_header->mutex);
        uv_cond_signal(&resp_header->cond);
    }
    DBG_FUNC_EXIT();
}

int libuv_connect(const char * addr, int port, handle_t * handle)
{
    client_info_t * client; 
    int ret;

    DBG_FUNC_ENTER();
    if ((addr == NULL) || (handle == NULL)) {
        return -1;
    }


    client = client_info_init();
    assert(client != NULL);
    /*tcp init */
    client->progress = IN_PROGRESS;
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

    wait_for_response(client);
    ret = client->error.err;
    if (ret == 0) {
        DBG_PRINT("handle: %p\n", client);
        *handle = (handle_t) client;
    }
    clear_error(&client->error);
    DBG_FUNC_EXIT();
    return ret;
}

static write_req_t * write_req_init(void)
{
    write_req_t * wr = malloc(sizeof(write_req_t));
    assert(wr != NULL);

    int r = uv_mutex_init(&wr->mutex);
    assert(r == 0);

    wr->status = 0;
    wr->progress = IN_PROGRESS;

    r = uv_cond_init(&wr->cond);
    assert(r == 0);

    return wr;
}

static void add_response_header(int id, response_header_t * res, response_header_t ** hash) 
{
    HASH_ADD_INT(*hash, id, res);
}
static void find_response_header(int id, response_header_t **res, response_header_t ** hash)
{
    HASH_FIND_INT(*hash, &id, *res);
}

static response_header_t * response_init(void)
{
    response_header_t * res = malloc(sizeof(response_header_t));
    assert(res != NULL);

    int r = uv_mutex_init(&res->mutex);
    assert(r == 0);

    r = uv_cond_init(&res->cond);
    assert(r == 0);

    res->progress = IN_PROGRESS;
    res->id = 0;
    res->resp = NULL;
    return res;
}

static void client_send(client_info_t * client, const uint8_t * req, uint32_t len)
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
    /* wait for response */
#if 0
    uv_mutex_lock(&wr->mutex);
    while (wr->progress == IN_PROGRESS) {
        uv_cond_wait(&wr->cond, &wr->mutex);
    }
    ret = wr->status;
#endif
    DBG_FUNC_EXIT();
}

int libuv_send(handle_t handle, const uint8_t * data, uint32_t len, int * req_id)
{
    client_info_t * client = handle;

    DBG_FUNC_ENTER();
    assert((data != NULL) || (len != 0));
    assert((client != NULL) && (client->magic == HEADER_MAGIC));
    assert(client->status != CLOSED);
    DBG_VERBOSE("%s: wr: %p\n", __FUNCTION__, wr);
    /* allocate response for this request */
    response_header_t * res = response_init();
    assert(res != NULL);

    uv_mutex_lock(&client->mutex);
    DBG_VERBOSE("%s: client: %p handle: %p\n", __FUNCTION__, client->handle, &client->tcp_client);
    res->id = ++client->req_id;
    *req_id = res->id;
    uv_buf_t buf = create_request(data, len, res->id);
    add_response_header(res->id, res, &client->hash);
    client_send(client, (const uint8_t *)buf.base, (uint32_t)buf.len);
    uv_mutex_unlock(&client->mutex);
    DBG_FUNC_EXIT();
    return 0;
}

int libuv_recv(handle_t handle, int req_id, uint8_t * data, uint32_t nread, int * more)
{
    client_info_t * client = handle;

    DBG_FUNC_ENTER();
    assert((data != NULL) || (nread != 0));
    assert((client != NULL) && (client->magic == HEADER_MAGIC));
    assert(client->status != CLOSED);
    /* allocate response for this request */
    response_header_t * res_hdr = NULL;
    find_response_header(req_id, &res_hdr, &client->hash);
    assert(res_hdr != NULL);

    DBG_VERBOSE("%s: waiting for response\n", __FUNCTION__);
    uv_mutex_lock(&res_hdr->mutex);
    while (res_hdr->progress == IN_PROGRESS) {
        uv_cond_wait(&res_hdr->cond, &res_hdr->mutex);
    }
    uv_mutex_unlock(&res_hdr->mutex);
    if (res_hdr->resp->hdr.len > nread) {
        memcpy(data, res_hdr->resp->buf, nread);
        *more = 1;
    } else {
        nread = res_hdr->resp->hdr.len;
        memcpy(data, res_hdr->resp->buf, nread);
        *more = 0;
        /* delete the response from hash table */
    }
    DBG_FUNC_EXIT();
    return nread;
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

