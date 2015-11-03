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
    {"192.168.0.12", 7000},
};
#define MAX_NUM_SLAVES  (sizeof(slaves_conf)/sizeof(slaves_conf[0]))

uv_key_t server_key;
typedef void (*on_connection_callback)(uv_stream_t *, int);
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
        DBG_ALLOC("FREE: req->buf: %p req %p\n", res->buf, res);
        free(res->buf);
        free(res);
    }
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
