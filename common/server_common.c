#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <libuv-wrap.h>
#include <libuv-server.h>

#ifdef MASTER
uint32_t add_client_request(server_info_t * server, uv_handle_t * client, uint32_t client_req_id);
#endif

void request_split_push(queue_t * q, queue_t * push_q, connection_info_t * cinfo)
{
    char temp[1024];
    uint32_t temp_len = 0;
    read_stages_t stage = HEADER_LEN_READ;
    uint32_t payload_offset = 0;
    request_t * req = NULL;
    uint32_t rem_size = 0;
    req_buf_t * cur_buf = NULL;

    uv_mutex_lock(&cinfo->mutex);
    while (!is_empty(q)) {
        if (cur_buf != NULL) {
            DBG_ALLOC("%s: FREE cur_buf->buf: %p cur_buf: %p\n", __FUNCTION__, cur_buf->buf, cur_buf);
            free(cur_buf->buf);
            free(cur_buf);
            cur_buf = NULL;
        }
        cur_buf = (req_buf_t *)queue_pop(q);
        /* this is to handle the previous request */
        if (cur_buf->stage != INVALID_STAGE) stage = cur_buf->stage;
        if (cur_buf->req) { 
            req  = cur_buf->req;
            payload_offset = req->offset;
        }
        DBG_VERBOSE("%x buf: %p len: %ld offset: %ld req: %p stage: %d %d\n", 
		(unsigned)pthread_self(), cur_buf->buf, cur_buf->len, 
                cur_buf->offset, cur_buf->req, cur_buf->stage, payload_offset);
        /* write a response split using fixed len */
        rem_size = cur_buf->len - cur_buf->offset;
        DBG_VERBOSE("rem_size: %u\n", rem_size);
        while ((temp_len > 0) && (rem_size > 0)) {
            switch(stage) {
                case HEADER_LEN_READ:
                    {
                        uint32_t hdr_len = 0;
                        if ((rem_size + temp_len) < HEADER_SIZE) {
                            memcpy(temp + temp_len, cur_buf->buf + cur_buf->offset, rem_size);
                            temp_len += rem_size;
                            rem_size = 0;
                            cur_buf->offset += rem_size;
                        } else {
                            req = malloc(sizeof(request_t));
                            assert(req != NULL);
                            DBG_ALLOC("%s: ALLOC request: %p\n", __FUNCTION__, req);
                            req->buf = NULL;
                            req->offset = 0;
                            if (temp_len < HEADER_SIZE) {
                                memcpy((uint8_t *)&hdr_len, temp, temp_len);
                                memcpy(((uint8_t *)&hdr_len) + temp_len, cur_buf->buf + cur_buf->offset, HEADER_SIZE - temp_len);
                                temp_len = 0;
                                rem_size -= HEADER_SIZE + temp_len;
                                cur_buf->offset += (HEADER_SIZE - temp_len);
                            } else {
                                memcpy((uint8_t *)&hdr_len, temp, HEADER_SIZE);
                                temp_len -= HEADER_SIZE;
                            }
                            DBG_VERBOSE("%d HEADER length: %u\n", __LINE__, hdr_len);
                            req->header_len = hdr_len;
                            stage = HEADER_READ;
                        }
                        break;
                    }
                case HEADER_READ:
                    {
                        if ((rem_size + temp_len) < req->header_len) {
                            memcpy(temp + temp_len, cur_buf->buf + cur_buf->offset, rem_size);
                            temp_len += rem_size;
                            rem_size = 0;
                            cur_buf->offset += rem_size;
                        } else  {
                            if (temp_len < req->header_len) {
                                memcpy((uint8_t *)&req->hdr, temp, temp_len);
                                memcpy(((uint8_t *)&req->hdr) + temp_len, cur_buf->buf + cur_buf->offset, req->header_len - temp_len);
                                temp_len = 0;
                                rem_size -= (req->header_len - temp_len);
                                cur_buf->offset += (req->header_len - temp_len);
                            } else {
                                memcpy((uint8_t *)&req->hdr, temp, req->header_len);
                                temp_len -= req->header_len;
                            }
                            DBG_VERBOSE("%d REQUEST id: %u\n", __LINE__, req->hdr.id);
                            stage = PAYLOAD_READ;
                            req->buf = malloc(req->hdr.len);
                            assert(req->buf != NULL);
                        }
                        break;
                    }
                case PAYLOAD_READ:
                    {
                        if (temp_len < req->hdr.len) {
                            memcpy(req->buf, temp, temp_len);
                            temp_len = 0;
                        } 
                        break;
                    }
                default:
                    DBG_ERR("INVALID case needs to be find out");
                    assert(0);
                    break;
            }
        }
        while (rem_size > 0) {
            switch(stage) {
                case HEADER_LEN_READ:
                    if (rem_size < HEADER_SIZE){
                        memcpy(temp, cur_buf->buf + cur_buf->offset, rem_size);
                        temp_len = rem_size;
                        cur_buf->offset += rem_size;
                        rem_size = 0;
                    } else {
                        req = malloc(sizeof(request_t));
                        assert(req != NULL);
                        DBG_ALLOC("%s: ALLOC request: %p\n", __FUNCTION__, req);
                        req->buf = NULL;	
                        req->offset = 0;

                        read_uint32_t((uint8_t *)cur_buf->buf + cur_buf->offset, HEADER_SIZE, &req->header_len);
                        cur_buf->offset += HEADER_SIZE;
                        rem_size -= HEADER_SIZE;
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
                        req->buf = malloc(req->hdr.len);
                        assert(req->buf != NULL);
                        DBG_ALLOC("ALLOC req->buf: %p\n", req->buf);
                        stage = PAYLOAD_READ;
                    }
                    break;
                case PAYLOAD_READ:
                    {
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
                            /* request is done, lets push it into the queue */
                            DBG_VERBOSE("req_id: %u\n", req->hdr.id);
#ifdef MASTER
                            int client_req_id = add_client_request(
                                    (server_info_t *)(((connection_info_t *)cinfo)->client.data), 
                                    (uv_handle_t *) cinfo, req->hdr.id);
                            req->hdr.id = client_req_id;
#endif
                            req->cinfo = (uv_handle_t *)cinfo;
                            queue_push(push_q, (void *)req);
                            req = NULL;
                            stage = HEADER_LEN_READ;
                        }
                    }
                    break;
                default:
                    DBG_ERR("invalid stage\n");
                    assert(0);
                    break;
            }
        }
    }
    if ((temp_len > 0) || (payload_offset > 0)) {
        DBG_VERBOSE("temp_len: %d payload_offset: %d stage: %d cur_buf %p\n", 
                           temp_len, payload_offset, stage, cur_buf);
        cur_buf->req = req;
        cur_buf->stage = stage;
        cur_buf->offset -= temp_len;
        if (cur_buf->req != NULL) {
            cur_buf->req->offset = payload_offset;
        }
        queue_push_front(q, (void *)cur_buf);
    }
    uv_mutex_unlock(&(cinfo->mutex));
}

void request_split_task(uv_work_t * work_req)
{
    req_work_t * work = (req_work_t *) work_req->data;
    connection_info_t * cinfo = work->cinfo;
    DBG_FUNC_ENTER();
    request_split_push(cinfo->buf_q, ((server_info_t *)(cinfo->client.data))->req_q, cinfo);
    DBG_FUNC_EXIT();
}

void schedule_work(connection_info_t * cinfo)
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

void after_read_cb(uv_stream_t * handle,
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

void alloc_cb(uv_handle_t * handle,
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

connection_info_t * connection_info_init(void)
{
    DBG_FUNC_ENTER();
    connection_info_t * cinfo = malloc(sizeof(connection_info_t));
    assert(cinfo != NULL);

    cinfo->buf_q = queue_init();
    assert(cinfo->buf_q != NULL);
    DBG_VERBOSE("cinfo->buf_q : %p\n", cinfo->buf_q);
  
    assert (uv_mutex_init(&cinfo->mutex) == 0);

    DBG_FUNC_EXIT();
    return cinfo;
}
void request_split_cleanup(uv_work_t * req, int status)
{
    req_work_t * work = (req_work_t *) req->data;
    DBG_FUNC_ENTER();
    DBG_ALLOC("FREE: work: %p\n", work);
    free(work);
    DBG_FUNC_EXIT();
}

void after_write_cb(uv_write_t * req, int status) 
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

void on_close_cb(uv_handle_t* handle)
{
  connection_info_t * cinfo = (connection_info_t *) handle;
  DBG_FUNC_ENTER();
  cinfo->status = CLOSED;
  queue_deinit(cinfo->buf_q);
  free(handle);
  DBG_FUNC_EXIT();
}

void after_shutdown_cb(uv_shutdown_t* req, int status) {
  /*assert(status == 0);*/
  DBG_FUNC_ENTER();
  if (status < 0)
    DBG_ERR("err: %s\n", uv_strerror(status));
  uv_close((uv_handle_t*)req->handle, on_close_cb);
  free(req);
  DBG_FUNC_EXIT();
}

