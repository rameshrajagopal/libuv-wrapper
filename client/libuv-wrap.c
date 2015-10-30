#include "libuv-wrap.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

uv_key_t client_key;

static void on_connect_cb(uv_connect_t * req, int status)
{
    client_info_t * client = uv_key_get(&client_key);

    DBG_FUNC_ENTER();
    DBG_PRINT("%s client: %p\n", __FUNCTION__, client);
    client->error.cb_id = ON_CONNECT_CB;
    client->error.err = status;
    if (status < 0) {
        DBG_ERR("listen err %s\n", uv_strerror(status));
    } 
    /* notify the connect caller */
    client->status = DONE;
    uv_ref((uv_handle_t *)req->handle);
    uv_cond_signal(&client->cond);
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
    /* create a io thread */
    ret = uv_thread_create(&client->thread_id, client_io_loop, (void *)client);
    assert(ret == 0);
    /*tcp init */
    client->status = IN_PROGRESS;
    ret = tcp_client_init(addr, port, client);
    assert(ret == 0);
    wait_for_response(client);
    ret = client->error.err;
    if (ret == 0) {
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

    int w = uv_cond_init(&wr->cond);
    assert(r == 0);

    return wr;
}

int libuv_send(handle_t handle, const uint8_t * data, uint32_t len)
{
    client_info_t * client = handle;

    DBG_FUNC_ENTER();
    assert((data == NULL) || (len == 0));
    assert((client == NULL) || (client->magic != HEADER_MAGIC));
    /* write the data on to the tcp client req */
    write_req_t * wr = write_req_init();
    assert(write_req != NULL);

    uv_buf_t buf = uv_buf_init(data, len);
    uv_mutex_lock(&client->mutex);
    int ret = uv_write(&wr->req, (uv_stream_t *)client->req.handle, &buf, 1, write_end_cb);
    uv_mutex_unlock(&client->mutex);
    return ret;
}
