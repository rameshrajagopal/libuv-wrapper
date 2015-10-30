#include "libuv-wrap.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

uv_key_t client_key;
uv_idle_t idle;

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
    } 
    /* notify the connect caller */
    client->status = DONE;
    client->handle = (uv_stream_t *)client->req.handle;
    uv_cond_signal(&client->cond);
    DBG_PRINT("%s: handle: %p %p\n", __FUNCTION__, client->handle, &client->tcp_client);
    DBG_FUNC_EXIT();
}

static void data_read_cb(uv_stream_t * stream, ssize_t nread, const uv_buf_t * buf)
{
    DBG_FUNC_ENTER();
    if (nread < 0) {
        DBG_ERR("error on read");
        return;
    }
    DBG_VERBOSE("stream: %p nread: %ld buf: %p\n", stream, nread, buf);
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
    uv_mutex_lock(&wr->mutex);
    wr->status = status;
    wr->progress = DONE;
    uv_mutex_unlock(&wr->mutex);
    uv_cond_signal(&wr->cond);
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
    uv_key_set(&client_key, arg);
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

    DBG_FUNC_EXIT();
    return client;
}

static void wait_for_response(client_info_t * client)
{
    DBG_FUNC_ENTER();
    uv_mutex_lock(&client->mutex);
    while (client->status == IN_PROGRESS) {
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
    int r = uv_idle_init(client->loop, &idle);
    assert(r == 0);

    r = uv_idle_start(&idle, libuv_idle_cb);
    assert(r == 0);
    return r;
}

int libuv_connect(const char * addr, int port, handle_t * handle)
{
    client_info_t * client; 
    int ret;

    DBG_FUNC_ENTER();
    if ((addr == NULL) || (handle == NULL)) {
        return -1;
    }

    ret = uv_key_create(&client_key);
    assert(ret == 0);

    client = client_info_init();
    /*tcp init */
    client->status = IN_PROGRESS;
    ret = tcp_client_init(addr, port, client);
    assert(ret == 0);
    /* create a io thread */
    ret = uv_thread_create(&client->thread_id, client_io_loop, (void *)client);
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

int libuv_send(handle_t handle, const uint8_t * data, uint32_t len)
{
    client_info_t * client = handle;

    DBG_FUNC_ENTER();
    assert((data != NULL) || (len != 0));
    assert((client != NULL) && (client->magic == HEADER_MAGIC));
    /* write the data on to the tcp client req */
    write_req_t * wr = write_req_init();
    assert(wr != NULL);

    DBG_VERBOSE("%s: wr: %p\n", __FUNCTION__, wr);
    uv_buf_t buf = uv_buf_init((char *)data, len);

    uv_mutex_lock(&client->mutex);
    DBG_VERBOSE("%s: client: %p handle: %p\n", __FUNCTION__, client->handle, &client->tcp_client);
    int ret = uv_write(&wr->req, client->handle, &buf, 1, write_end_cb);
    uv_mutex_unlock(&client->mutex);
    /* wait for response */
    uv_mutex_lock(&wr->mutex);
    while (wr->progress == IN_PROGRESS) {
        uv_cond_wait(&wr->cond, &wr->mutex);
    }
    ret = wr->status;
    DBG_FUNC_EXIT();
    return ret;
}
