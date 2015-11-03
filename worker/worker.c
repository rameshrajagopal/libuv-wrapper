#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>

#include <libuv-wrap.h>
#include <libuv-server.h>

uv_key_t server_key;
typedef void (*on_connection_callback)(uv_stream_t *, int);

static void response_send(request_t * req)
{
  DBG_FUNC_ENTER();

  write_req_t * wr = (write_req_t *) malloc(sizeof(*wr));
  assert(wr != NULL);
  DBG_ALLOC("ALLOC: wr %p\n", wr);


  wr->buf = create_response(malloc(16 * 1024 * sizeof(char)), 16 * 1024, req->hdr.id);
  DBG_INFO("res_id: %d\n", req->hdr.id);
  int r = uv_write(&wr->req, (uv_stream_t *)&(((connection_info_t *)req->cinfo)->client), &wr->buf, 1, after_write_cb);
  if (r != 0) DBG_VERBOSE("rvalue: %s\n", uv_strerror(r));
  //assert(r == 0);
  DBG_FUNC_EXIT();
}

static void on_connection_cb(uv_stream_t * stream, int status)
{
  server_info_t * server = (server_info_t *) stream;
  connection_info_t * cinfo = NULL;
  int r;

  DBG_FUNC_ENTER();
  assert(status == 0);

  cinfo = connection_info_init();
  assert(cinfo != NULL);

  r = uv_tcp_init(uv_default_loop(), &cinfo->client);
  assert(r == 0);

  cinfo->client.data = server;
  DBG_VERBOSE("server: %p\n", cinfo->client.data);

  r = uv_accept((uv_stream_t *)&server->tcp_server, (uv_stream_t*)&cinfo->client);
  assert(r == 0);
  /* cid */
  uv_mutex_lock(&server->mutex); 
  cinfo->cid = ++server->cid;
  uv_mutex_unlock(&server->mutex);
  DBG_INFO("%s: cinfo: %p client: %p id: %u\n", __FUNCTION__, cinfo, &cinfo->client, cinfo->cid);
  r = uv_read_start((uv_stream_t*)&cinfo->client, alloc_cb, after_read_cb);
  assert(r == 0);
  DBG_FUNC_EXIT();
}

static int tcp_server_init(server_info_t * server, const char * serv_addr, 
                           int port, on_connection_callback  connection)
{
  struct sockaddr_in addr;
  int r;

  r = uv_ip4_addr(serv_addr, port, &addr);
  assert(r == 0);

  r = uv_tcp_init(uv_default_loop(), &server->tcp_server);
  assert(r == 0);

  r = uv_tcp_bind(&server->tcp_server, (const struct sockaddr*)&addr, 0);
  assert(r == 0);

  r = uv_listen((uv_stream_t*)&server->tcp_server, SOMAXCONN, connection);
  assert(r == 0);

  return 0;
}

void server_io_loop(void * arg)
{
    server_info_t * server = arg;
    DBG_FUNC_ENTER();
    DBG_VERBOSE("%s: server: %p\n", __FUNCTION__, server);
    uv_key_set(&server_key, server);
    int r = uv_run(uv_default_loop(), UV_RUN_DEFAULT);
    assert(r == 0);
    DBG_FUNC_EXIT();
}

static int is_connection_exist()
{
    return 1;
}

static void resp_async_cb(uv_async_t * async)
{
    server_info_t  * server = (server_info_t *)async->data;

    while (!is_empty(server->res_q)) {
        request_t * req = (request_t *) queue_pop(server->res_q);
        /* find out cid still available in the hash table
         * if it is there proceed to send response, otherwise, just 
         * leave it 
         */
        if (is_connection_exist()) {
            response_send(req);
        }
        /* free the request here */
        DBG_ALLOC("FREE: req->buf: %p req %p\n", req->buf, req);
        free(req->buf);
        free(req);
    }
}

static void wakeup_async_cb(server_info_t * server)
{
   server->resp_async.data = (void *) server; 
   uv_async_send(&server->resp_async);
}

static void request_worker_task(void * arg)
{
    server_info_t * server = arg;

    DBG_FUNC_ENTER();
    while (1) {
        request_t * req = (request_t *)queue_pop(server->req_q);
        DBG_VERBOSE("Got request: %p req->hdr.len: %u buf: %p\n", req, req->hdr.len, req->buf);
        /* do the actual work */
        DO_WORK();
        /* form the response */
        queue_push(server->res_q, (void *)req);
        wakeup_async_cb(server);
    }
    DBG_FUNC_EXIT();
}


server_info_t * server_info_init(void)
{
    server_info_t * server = malloc(sizeof(server_info_t));
    assert(server != NULL);

    int r = uv_mutex_init(&server->mutex);
    assert(r == 0);

    r = uv_cond_init(&server->cond);
    assert(r == 0);

    server->req_q = queue_init();
    assert(server->req_q != NULL);

    server->res_q = queue_init();
    assert(server->res_q != NULL);

    r = uv_async_init(uv_default_loop(), &server->resp_async, resp_async_cb);
    assert(r == 0);

    for (int num = 0; num < MAX_NUM_WORKER_THREADS; ++num) {
        uv_thread_create(&server->tids[num], request_worker_task, (void *)server);  
    }

    server->cid = 0;

    return server;
}

int main(int argc, char * argv[]) 
{
  if (argc != 3) {
      printf("Usage:\n");
      printf("%s ip port\n", argv[1]);
      return -1;
  }
  server_info_t * server = server_info_init();
  assert(server != NULL);

  int r = uv_key_create(&server_key);
  assert(r == 0);

  r = tcp_server_init(server, argv[1], atoi(argv[2]), on_connection_cb);
  assert(r == 0);

  r = uv_thread_create(&server->tid, server_io_loop, (void *)server);
  assert( r == 0);

  getchar();

  return 0;
}
