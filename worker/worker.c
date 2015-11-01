#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>

#include <libuv-wrap.h>
#include <libuv-server.h>

typedef void (*on_connection_callback)(uv_stream_t *, int);

#if 0
static void on_close_cb(uv_handle_t* handle)
{
  DBG();
  free(handle);
}

static void after_shutdown_cb(uv_shutdown_t* req, int status) {
  /*assert(status == 0);*/
  if (status < 0)
    DBG_ERR("err: %s\n", uv_strerror(status));
  uv_close((uv_handle_t*)req->handle, on_close_cb);
  free(req);
}


static void after_write_cb(uv_write_t* req, int status) 
{
  write_req_t * wr = (write_req_t*)req;

  DBG();
#if 0
  if (wr->buf.base != NULL)
    free(wr->buf.base);
#endif
  free(wr);
  if (status == 0) {
    DBG_PRINT_INFO("status: %d\n", status);
    return;
  }
  DBG_ERR("uv_write : %s\n", uv_strerror(status));
  if (status == UV_ECANCELED)
    return;
  assert(status == UV_EPIPE);
  //uv_close((uv_handle_t*)req->handle, on_close_cb);
}
#endif

#if 0
void work_request_cb(uv_work_t * req)
{
    struct request * c_req = (struct request *)req->data;

    DBG_PRINT_INFO("%s: Received work: crn: %d datalength: %ld\n",
                     __FUNCTION__, c_req->client_req_num, c_req->nread);
    /* do actual work */
    usleep(100 * 1000);
    write_req_t * wr = (write_req_t *) malloc(sizeof(*wr));
    assert(wr != NULL);
    DBG_PRINT_INFO("%s: nread: %ld\n", __FUNCTION__, c_req->nread);
    wr->buf = uv_buf_init(c_req->buf->base, c_req->nread);
    int r = uv_write(&wr->req, (uv_stream_t *)c_req->handle, &wr->buf, 1, after_write_cb);
    assert(r == 0);
}

void work_request_cleanup_cb(uv_work_t * req, int status)
{
    struct request * c_req = (struct request *)req->data;
#if 0
    write_req_t * wr;

    DBG();
    wr = (write_req_t *) malloc(sizeof(*wr));
    assert(wr != NULL);

    DBG_PRINT_INFO("%s: nread: %ld\n", __FUNCTION__, c_req->nread);
    wr->buf = uv_buf_init(c_req->buf->base, c_req->nread);
    int r = uv_write(&wr->req, (uv_stream_t *)c_req->handle, &wr->buf, 1, after_write_cb);
    assert(r == 0);
#endif
    free(c_req);
}
#endif

#ifdef ENABLE_READ_CB
static void after_read_cb(uv_stream_t * handle,
                       ssize_t nread,
                       const uv_buf_t * buf) 
{
  int r;
  uv_shutdown_t* req;
//  static int client_req_num = 0;/* make it atomic */

  DBG();
  if (nread < 0) {
      /*assert(nread == UV_EOF);*/
      if (buf->base != NULL) free(buf->base);
      DBG_ERR("err: %s\n", uv_strerror(nread));

      req = (uv_shutdown_t*) malloc(sizeof(*req));
      assert(req != NULL);

      r = uv_shutdown(req, handle, after_shutdown_cb);
      assert(r == 0);
      return;
  }
  if (nread == 0) {
      DBG_PRINT("%s:%d length: %ld\n", __FUNCTION__, __LINE__, nread);
     return;
  }

    DBG();
    write_req_t * wr = (write_req_t *) malloc(sizeof(*wr));
    assert(wr != NULL);

    wr->buf = uv_buf_init(buf->base, nread);
    r = uv_write(&wr->req, (uv_stream_t *)handle, &wr->buf, 1, after_write_cb);
    assert(r == 0);
}

static void alloc_cb(uv_handle_t* handle,
                     size_t suggested_size,
                     uv_buf_t* buf) 
{
  DBG();
  buf->base = malloc(suggested_size);
  assert(buf->base != NULL);
  DBG_PRINT("%s:%d handle: %p base: %p %ld\n", __FUNCTION__, __LINE__, handle, buf->base, suggested_size);
  buf->len = suggested_size;
}
#endif

static connection_info_t * connection_info_init(void)
{
    connection_info_t * cinfo = malloc(sizeof(connection_info_t));
    assert(cinfo != NULL);
    return cinfo;
}

static void on_connection_cb(uv_stream_t * stream, int status)
{
  server_info_t * server = (server_info_t *) stream;
  connection_info_t * cinfo = NULL;
  int r;

  assert(status == 0);

  cinfo = connection_info_init();
  assert(cinfo != NULL);

  r = uv_tcp_init(uv_default_loop(), &cinfo->client);
  assert(r == 0);

  cinfo->client.data = server;

  r = uv_accept((uv_stream_t *)&server->tcp_server, (uv_stream_t*)&cinfo->client);
  assert(r == 0);
  /* cid */
  uv_mutex_lock(&server->mutex); 
  cinfo->cid = ++server->cid;
  uv_mutex_unlock(&server->mutex);
  DBG_INFO("%s: client: %p id: %u\n", __FUNCTION__, &cinfo->client, cinfo->cid);
#if 0
  r = uv_read_start((uv_stream_t*)client, alloc_cb, after_read_cb);
  assert(r == 0);
#endif
}

static int tcp_server_init(server_info_t * server, const char * serv_addr, int port, on_connection_callback  connection)
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
    int r = uv_run(uv_default_loop(), UV_RUN_DEFAULT);
    assert(r == 0);
    DBG_FUNC_EXIT();
}

server_info_t * server_info_init(void)
{
    server_info_t * server = malloc(sizeof(server_info_t));
    assert(server != NULL);

    int r = uv_mutex_init(&server->mutex);
    assert(r == 0);

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

  int r = tcp_server_init(server, argv[1], atoi(argv[2]), on_connection_cb);
  assert(r == 0);

  r = uv_thread_create(&server->tid, server_io_loop, (void *)server);
  assert( r == 0);

  getchar();

  return 0;
}
