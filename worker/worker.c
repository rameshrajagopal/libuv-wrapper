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
static void after_write_cb(uv_write_t * req, int status); 

static void response_send(request_t * req)
{
  DBG_FUNC_ENTER();

  write_req_t * wr = (write_req_t *) malloc(sizeof(*wr));
  assert(wr != NULL);
  DBG_ALLOC("ALLOC: wr %p\n", wr);

  wr->buf = create_response(req->buf, req->hdr.len, req->hdr.id);

  int r = uv_write(&wr->req, (uv_stream_t *)&(((connection_info_t *)req->cinfo)->client), &wr->buf, 1, after_write_cb);
  if (r != 0) DBG_VERBOSE("rvalue: %s\n", uv_strerror(r));
  assert(r == 0);
  DBG_FUNC_EXIT();
}

static int is_connection_exist()
{
    return 1;
}

static void response_send_task(uv_work_t * work)
{
    resp_work_t * resp_work = (resp_work_t *) work->data;
    server_info_t * server  = resp_work->server;

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

static void response_send_cleanup(uv_work_t * resp_work, int status)
{
    DBG_FUNC_ENTER();
    DBG_ALLOC("FREE resp_work: %p status: %d\n", resp_work, status);
    free(resp_work);
    DBG_FUNC_EXIT();
}

#if 0
static int libuv_idle_start(server_info_t * server)
{
    DBG_FUNC_ENTER();
    idle_task_t * idle_task = malloc(sizeof(idle_task_t));
    assert(idle_task != NULL);
    DBG_ALLOC("ALLOC idle_task: %p\n", idle_task);

    int r = uv_idle_init(uv_default_loop(), &idle_task->idle);
    assert(r == 0);

    r = uv_idle_start(&idle_task->idle, libuv_idle_cb);
    assert(r == 0);
    DBG_FUNC_EXIT();
    return r;
}
#endif

static void on_close_cb(uv_handle_t* handle)
{
  connection_info_t * cinfo = (connection_info_t *) handle;
  DBG_FUNC_ENTER();
  queue_deinit(cinfo->buf_q);
  free(handle);
  DBG_FUNC_EXIT();
}
static void after_shutdown_cb(uv_shutdown_t* req, int status) {
  /*assert(status == 0);*/
  DBG_FUNC_ENTER();
  if (status < 0)
    DBG_ERR("err: %s\n", uv_strerror(status));
  uv_close((uv_handle_t*)req->handle, on_close_cb);
  free(req);
  DBG_FUNC_EXIT();
}


static void after_write_cb(uv_write_t * req, int status) 
{
  write_req_t * wr = (write_req_t*)req;

  DBG_FUNC_ENTER();
  if (wr->buf.base != NULL) {
    DBG_ALLOC("FREE: wr->buf.base %p\n", wr->buf.base);
    free(wr->buf.base);
  }
  DBG_ALLOC("FREE: wr: %p\n", wr);
  free(wr);
  if (status == 0) {
    DBG_PRINT_INFO("status: %d\n", status);
    return;
  }
  DBG_ERR("uv_write : %s\n", uv_strerror(status));
  if (status == UV_ECANCELED) {
    return;
  }
  assert(status == UV_EPIPE);
  DBG_VERBOSE("req->handle: %p\n", req->handle);
  uv_close((uv_handle_t*)req->handle, on_close_cb);
}

void request_split_task(uv_work_t * work_req)
{
    req_work_t * work = (req_work_t *) work_req->data;
    DBG_FUNC_ENTER();
    /* variables for handling request split */
    char temp[1024];
    uint32_t temp_len = 0;
    read_stages_t stage = HEADER_LEN_READ;
    uint32_t payload_offset = 0;
    request_t * req = NULL;
    uint32_t rem_size = 0;
    req_buf_t * cur_buf = NULL;
    connection_info_t * cinfo = work->cinfo;

    while (!is_empty(cinfo->buf_q)) {
        if (cur_buf != NULL) {
            DBG_ALLOC("%s: FREE cur_buf->buf: %p cur_buf: %p\n", __FUNCTION__, cur_buf->buf, cur_buf);
            free(cur_buf->buf);
            free(cur_buf);
            cur_buf = NULL;
        }
        cur_buf = (req_buf_t *)queue_pop(cinfo->buf_q);
        /* this is to handle the previous request */
        if (cur_buf->stage != INVALID_STAGE) stage = cur_buf->stage;
        if (cur_buf->req) req  = cur_buf->req;
        DBG_VERBOSE("buf: %p len: %ld offset: %ld req: %p stage: %d\n", cur_buf->buf, cur_buf->len, 
                cur_buf->offset, cur_buf->req, cur_buf->stage);
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
                        printf("HEADER length: %u\n", hdr_len);
                        req->header_len = hdr_len;
                        temp_len = 0;
                        rem_size -= HEADER_SIZE_LEN;
                        stage = HEADER_READ;
                        break;
                    }
                    case HEADER_READ:
                    {
                        memcpy((uint8_t *)&req->hdr, temp, temp_len);
                        memcpy(((uint8_t *)&req->hdr) + temp_len, cur_buf->buf + cur_buf->offset, req->header_len - temp_len);
                        printf("REQUEST id: %u\n", req->hdr.id);
                        temp_len = 0;
                        rem_size -= req->header_len;
                        stage = PAYLOAD_READ;
                        break;
                    }
                    default:
                        printf("INVALID case needs to be find out");
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
                        req = malloc(sizeof(request_t));
                        assert(req != NULL);
                        DBG_ALLOC("%s: ALLOC request: %p\n", __FUNCTION__, req);
                        req->buf = NULL;

                        read_uint32_t((uint8_t *)cur_buf->buf + cur_buf->offset, HEADER_SIZE_LEN, &req->header_len);
                        cur_buf->offset += HEADER_SIZE_LEN;
                        rem_size -= HEADER_SIZE_LEN;
                        printf("HEADER: %u\n", req->header_len);
                        stage = HEADER_READ;
                    }
                    break;
                case HEADER_READ:
                    if (rem_size < req->header_len) {
                        memcpy(temp, cur_buf->buf + cur_buf->offset, rem_size);
                        temp_len = rem_size;
                        cur_buf->offset += rem_size;
                        rem_size = 0;
                    } else {
                        read_pkt_hdr((uint8_t *)cur_buf->buf + cur_buf->offset, req->header_len, &req->hdr);
                        printf("HEADER: %x %x %x %x\n", req->hdr.magic, req->hdr.len, req->hdr.id, req->hdr.future);
                        rem_size -= req->header_len;
                        cur_buf->offset += req->header_len;
                        stage = PAYLOAD_READ;
                    }
                    break;
                case PAYLOAD_READ:
                    if (payload_offset > 0) {
                        size_t len_to_read = req->hdr.len - payload_offset;
                        if (rem_size < len_to_read) {
                            memcpy(req->buf + payload_offset, cur_buf->buf + cur_buf->offset, rem_size);
                            payload_offset += rem_size;
                            cur_buf->offset += rem_size;
                            rem_size = 0;
                        } else {
                            memcpy(req->buf + payload_offset, cur_buf->buf + cur_buf->offset, len_to_read);
                            payload_offset = 0;
                            rem_size -= len_to_read;
                            cur_buf->offset += len_to_read;
                        }
                    } else {
                        req->buf = malloc(req->hdr.len);
                        assert(req->buf != NULL);
                        DBG_ALLOC("%s: ALLOC req->buf: %p\n", __FUNCTION__, req->buf);

                        if (rem_size < req->hdr.len) {
                            memcpy(req->buf, cur_buf->buf + cur_buf->offset, rem_size);
                            payload_offset = rem_size;
                            rem_size = 0;
                            cur_buf->offset += rem_size;
                        } else {
                            memcpy(req->buf, cur_buf->buf + cur_buf->offset, req->hdr.len);
                            rem_size -= req->hdr.len;
                            cur_buf->offset += req->hdr.len;
                        }
                    }
                    /* request is done, push it to the queue */
                    printf("GOT THE RESPONSE for ID: %u\n", req->hdr.id);
                    DBG_VERBOSE("Pushed request %p to Queue\n", req);
                    req->cinfo = (uv_handle_t *)cinfo;
                    queue_push(((server_info_t *)(cinfo->client.data))->req_q, (void *)req);
                    req = NULL;
                    stage = HEADER_LEN_READ;
                    break;
                default:
                    DBG_ERR("invalid stage\n");
                    assert(0);
                    break;
            }
        }
    }
    if (temp_len > 0) {
        cur_buf->req = req;
        cur_buf->stage = stage;
        cur_buf->offset -= temp_len;
        queue_push_front(cinfo->buf_q, (void *)cur_buf);
    }
      
    DBG_FUNC_EXIT();
}

static void request_split_cleanup(uv_work_t * req, int status)
{
    req_work_t * work = (req_work_t *) req->data;
    DBG_FUNC_ENTER();
    DBG_ALLOC("FREE: work: %p\n", work);
    free(work);
    DBG_FUNC_EXIT();
}

static void schedule_work(connection_info_t * cinfo)
{
    DBG_FUNC_ENTER();
    req_work_t * work = malloc(sizeof(req_work_t));
    assert(work != NULL);
    DBG_ALLOC("ALLOC work: %p\n", work);

    work->req.data = (void *) work;
    work->cinfo = cinfo;
    /* change the below to corresponding io loop if exists there one */
    uv_queue_work(uv_default_loop(), &work->req, request_split_task, request_split_cleanup);
    DBG_FUNC_EXIT();
}

static void after_read_cb(uv_stream_t * handle,
                       ssize_t nread,
                       const uv_buf_t * buf) 
{
  connection_info_t * cinfo = (connection_info_t *) handle;
  int r;
  uv_shutdown_t* req;

  DBG_FUNC_ENTER();
  if (nread < 0) {
      DBG_VERBOSE("%s: nread: %ld\n", __FUNCTION__, nread);
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
  DBG_VERBOSE("%s: handle: %p buf->base: %p len: %ld\n", __FUNCTION__, handle, buf->base, nread);
  req_buf_t * req_buf = req_buf_init(buf->base, nread);
  assert(req_buf != NULL);
  /* put the buf into buf_q */
  DBG_VERBOSE("queue push: queue %p\n", cinfo->buf_q);
  queue_push(cinfo->buf_q, (void *) req_buf);
  DBG_VERBOSE("queue push exit\n");
  /* allocate work queue info */
  schedule_work(cinfo);
}

static void alloc_cb(uv_handle_t * handle,
                     size_t suggested_size,
                     uv_buf_t * buf) 
{
  DBG_FUNC_ENTER();
  buf->base = malloc(suggested_size);
  assert(buf->base != NULL);
  DBG_PRINT("%s:%d handle: %p base: %p %ld\n", __FUNCTION__, __LINE__, handle, buf->base, suggested_size);
  buf->len = suggested_size;
  DBG_FUNC_EXIT();
}

static connection_info_t * connection_info_init(void)
{
    DBG_FUNC_ENTER();
    connection_info_t * cinfo = malloc(sizeof(connection_info_t));
    assert(cinfo != NULL);

    cinfo->buf_q = queue_init();
    assert(cinfo->buf_q != NULL);
    DBG_VERBOSE("cinfo->buf_q : %p\n", cinfo->buf_q);

    DBG_FUNC_EXIT();
    return cinfo;
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
    uv_key_set(&server_key, server);
    int r = uv_run(uv_default_loop(), UV_RUN_DEFAULT);
    assert(r == 0);
    DBG_FUNC_EXIT();
}

void schedule_response_route(server_info_t * server)
{
    resp_work_t * resp_work = malloc(sizeof(resp_work_t));
    assert(resp_work != NULL);
    DBG_ALLOC("ALLOC resp_work: %p\n", resp_work);

    resp_work->req.data = (void *) resp_work;
    resp_work->server = server;
    uv_queue_work(uv_default_loop(), &resp_work->req, response_send_task, response_send_cleanup);
    DBG_FUNC_EXIT();
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
        schedule_response_route(server);
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

#if 0
  r = libuv_idle_start(server);
  assert(r == 0);
#endif

  r = tcp_server_init(server, argv[1], atoi(argv[2]), on_connection_cb);
  assert(r == 0);

  r = uv_thread_create(&server->tid, server_io_loop, (void *)server);
  assert( r == 0);

  getchar();

  return 0;
}
