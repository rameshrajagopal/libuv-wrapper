#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>

#include <libuv-wrap.h>
#include <libuv-server.h>

typedef struct {
    const char * addr;
    int port;
}slave_conf_t;
slave_conf_t slaves_conf[] = {
    {"127.0.0.1", 7000},
    {"127.0.0.1", 9000},
};
#define MAX_NUM_SLAVES  (sizeof(slaves_conf)/sizeof(slaves_conf[0]))

uv_key_t server_key;
typedef void (*on_connection_callback)(uv_stream_t *, int);
static void after_write_cb(uv_write_t * req, int status); 
static void request_mapper_get_info(server_info_t * server, uint32_t id, uint32_t * creq_num, uv_handle_t ** client);

/*
 * create another hashing b/w connection info to active or inactive
 */
static void response_send(server_info_t * server, response_t * res)
{
  DBG_FUNC_ENTER();

  write_req_t * wr = (write_req_t *) malloc(sizeof(*wr));
  assert(wr != NULL);
  DBG_ALLOC("ALLOC: wr %p\n", wr);

  connection_info_t * cinfo = NULL;
  uint32_t client_req_id = 0;

  wr->server_req_id = res->hdr.id;
  request_mapper_get_info(server, res->hdr.id, &client_req_id, (uv_handle_t **)&cinfo);
  assert(cinfo != NULL);
  wr->buf = create_response(res->buf, res->hdr.len, client_req_id);

  DBG_INFO("res: %d\n", client_req_id);
  int r = uv_write(&wr->req, (uv_stream_t *)&cinfo->client , &wr->buf, 1, after_write_cb);
  if (r != 0) DBG_VERBOSE("rvalue: %s\n", uv_strerror(r));
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
        response_t * res = (response_t *) queue_pop(server->res_q);
        /* find out cid still available in the hash table
         * if it is there proceed to send response, otherwise, just 
         * leave it 
         */
        if (is_connection_exist()) {
            response_send(server, res);
        }
        /* free the request here */
        DBG_ALLOC("FREE: req->buf: %p req %p\n", req->buf, req);
        free(res->buf);
        free(res);
    }
}

static void on_close_cb(uv_handle_t* handle)
{
  connection_info_t * cinfo = (connection_info_t *) handle;
  DBG_FUNC_ENTER();
  cinfo->status = CLOSED;
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
    DBG_VERBOSE("status: %d\n", status);
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

static inline void add_request_mapper(uint32_t id, request_mapper_t * req, request_mapper_t ** hash)
{
    HASH_ADD_INT(*hash, id, req);
}
static void find_request_mapper(uint32_t id, request_mapper_t ** req, request_mapper_t ** hash) 
{
    HASH_FIND_INT(*hash, &id, *req);
}

static void request_mapper_get_info(server_info_t * server, uint32_t id, uint32_t * creq_num, uv_handle_t ** client)
{
    request_mapper_t * req_map = NULL;
    
    uv_mutex_lock(&server->mutex);
    find_request_mapper(id, &req_map, &server->req_hash);
    assert(req_map != NULL);
    *creq_num = req_map->client_req_id;
    *client   = req_map->client;
    uv_mutex_unlock(&server->mutex);
}

int request_mapper_reply_dec(server_info_t * server, uint32_t req_id)
{
    request_mapper_t * req_map = NULL;
    
    uv_mutex_lock(&server->mutex);
    find_request_mapper(req_id, &req_map, &server->req_hash);
    assert(req_map != NULL);
    int cnt = --req_map->num_replies;
    uv_mutex_unlock(&server->mutex);
    return cnt;
}

static void update_request_mapper(server_info_t * server, uint32_t req_id, int num_requests)
{
    request_mapper_t * req_map = NULL;
    
    find_request_mapper(req_id, &req_map, &server->req_hash);
    assert(req_map != NULL);
    req_map->num_replies = num_requests;
}

uint32_t add_client_request(server_info_t * server, uv_handle_t * client, uint32_t client_req_id)
{
    request_mapper_t * req_map = malloc(sizeof(request_mapper_t));
    assert(req_map != NULL);

    uv_mutex_lock(&server->mutex);
    req_map->id = ++server->client_req_id; /* unique number for server */
    uint32_t ret = req_map->id;
    /* create a mapping here b/w client_req_num => client_req_id, ids to
     * slaves
     */
    req_map->client_req_id = client_req_id;
    req_map->num_replies = 0;
    req_map->client = client;
    add_request_mapper(ret, req_map, &server->req_hash);
    uv_mutex_unlock(&server->mutex);
    return ret;
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
                        DBG_VERBOSE("HEADER length: %u\n", hdr_len);
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
                        DBG_VERBOSE("REQUEST id: %u\n", req->hdr.id);
                        temp_len = 0;
                        rem_size -= req->header_len;
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
                        req = malloc(sizeof(request_t));
                        assert(req != NULL);
                        DBG_ALLOC("%s: ALLOC request: %p\n", __FUNCTION__, req);
                        req->buf = NULL;

                        read_uint32_t((uint8_t *)cur_buf->buf + cur_buf->offset, HEADER_SIZE_LEN, &req->header_len);
                        cur_buf->offset += HEADER_SIZE_LEN;
                        rem_size -= HEADER_SIZE_LEN;
                        DBG_VERBOSE("HEADER: %u\n", req->header_len);
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
                        DBG_VERBOSE("HEADER: %x %x %x %x\n", req->hdr.magic, req->hdr.len, req->hdr.id, req->hdr.future);
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
                    DBG_VERBOSE("req_id : %u\n", req->hdr.id);
                    int client_req_id = add_client_request((server_info_t *)(cinfo->client.data), (uv_handle_t *) cinfo, req->hdr.id);
                    req->hdr.id = client_req_id;
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
  cinfo->status = ACTIVE;
  uv_mutex_unlock(&server->mutex);
  DBG_INFO("cinfo: %p client: %p id: %u\n", cinfo, &cinfo->client, cinfo->cid);
  r = uv_read_start((uv_stream_t*)&cinfo->client, alloc_cb, after_read_cb);
  assert(r == 0);
  DBG_FUNC_EXIT();
}

static int tcp_server_init(server_info_t * server, const char * serv_addr, int port, 
                           on_connection_callback  connection)
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

void wakeup_response_async_cb(server_info_t * server)
{
    DBG_FUNC_ENTER(); 
    server->resp_async.data = (void *) server; 
    uv_async_send(&server->resp_async);
    DBG_FUNC_EXIT();
}

void add_proxy_slave(uint32_t id, proxy_slave_t * slave, proxy_slave_t ** hash)
{
  HASH_ADD_INT(*hash, id, slave);
}

static void find_proxy_slave(uint32_t id, proxy_slave_t **slave, proxy_slave_t ** hash)
{
    HASH_FIND_INT(*hash, &id, *slave);
}

static void request_worker_task(void * arg)
{
    server_info_t * server = arg;

    DBG_FUNC_ENTER();
    while (1) {
        request_t * req = (request_t *)queue_pop(server->req_q);
        DBG_VERBOSE("Got request: %p req->hdr.len: %u buf: %p\n", req, req->hdr.len, req->buf);
        /* do the actual work */
        uv_buf_t buf = create_request(req->buf, req->hdr.len, req->hdr.id);
        int num_requests = 0; 
        uv_mutex_lock(&server->mutex);
        for (uint32_t num = 0; num < server->num_slaves; ++num) {
            proxy_slave_t * slave = NULL;
            find_proxy_slave(num, &slave, &server->slave_hash); 
            assert(slave != NULL);
            DBG_VERBOSE("sening to slave: %d CONNECTION STATUS: %d\n", num, is_connection_active(slave->client));
            if (is_connection_active(slave->client)) {
                proxy_slave_send(slave->client, (const uint8_t *)buf.base, buf.len);
                ++num_requests;
            }
        }
        update_request_mapper(server, req->hdr.id, num_requests);
        uv_mutex_unlock(&server->mutex);
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
  
  for (int num = 0; num < MAX_NUM_SLAVES; ++num) {
     proxy_slave_t * slave = malloc(sizeof(proxy_slave_t));
     assert(slave != NULL);
     slave->client = proxy_slave_init(slaves_conf[num].addr, slaves_conf[num].port, num, (uv_handle_t *) server);
     assert(slave->client != NULL);
     slave->id = num;
     ++server->num_slaves;
     add_proxy_slave(slave->id, slave, &server->slave_hash);
  }

  int r = uv_key_create(&server_key);
  assert(r == 0);

  r = tcp_server_init(server, argv[1], atoi(argv[2]), on_connection_cb);
  assert(r == 0);

  r = uv_thread_create(&server->tid, server_io_loop, (void *)server);
  assert( r == 0);

  getchar();

  return 0;
}
