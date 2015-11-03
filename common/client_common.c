#include "libuv-wrap.h"
#include <assert.h>

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
            DBG_ALLOC("%s: FREE cur_buf->buf: %p cur_buf: %p\n", __FUNCTION__, cur_buf->buf, cur_buf);
            free(cur_buf->buf);
            free(cur_buf);
            cur_buf = NULL;
        }
        cur_buf = (txn_buf_t *)queue_pop(client->buf_q);
        DBG_VERBOSE("%s: buf: %p len: %ld offset: %ld\n", __FUNCTION__, cur_buf->buf, cur_buf->len, cur_buf->offset);
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
                        } else {
                            resp = malloc(sizeof(response_t));
                            assert(resp != NULL);
                            DBG_ALLOC("%s: ALLOC request: %p\n", __FUNCTION__, resp);
                            resp->buf = NULL;
                            if (temp_len < HEADER_SIZE) {
                                memcpy((uint8_t *)&hdr_len, temp, temp_len);
                                memcpy(((uint8_t *)&hdr_len) + temp_len, cur_buf->buf + cur_buf->offset, HEADER_SIZE - temp_len);
                                DBG_VERBOSE("HEADER length: %u\n", hdr_len);
                                temp_len = 0;
                                rem_size -= HEADER_SIZE + temp_len;
                            } else {
                                memcpy((uint8_t *)&hdr_len, temp, HEADER_SIZE);
                                temp_len -= HEADER_SIZE;
                            }
                            resp->header_len = hdr_len;
                            stage = HEADER_READ;
                        }
                        break;
                    }
                case HEADER_READ:
                    {
                        if ((rem_size + temp_len) < resp->header_len) {
                            memcpy(temp + temp_len, cur_buf->buf + cur_buf->offset, rem_size);
                            temp_len += rem_size;
                            rem_size = 0;
                        } else  {
                            if (temp_len < resp->header_len) {
                                memcpy((uint8_t *)&resp->hdr, temp, temp_len);
                                memcpy(((uint8_t *)&resp->hdr) + temp_len, cur_buf->buf + cur_buf->offset, resp->header_len - temp_len);
                                temp_len = 0;
                                rem_size -= (resp->header_len - temp_len);
                            } else {
                                memcpy((uint8_t *)&resp->hdr, temp, resp->header_len);
                                DBG_VERBOSE("REQUEST id: %u\n", resp->hdr.id);
                                temp_len -= resp->header_len;
                            }
                            stage = PAYLOAD_READ;
                            resp->buf = malloc(resp->hdr.len);
                            assert(resp->buf != NULL);
                        }
                        break;
                    }
                case PAYLOAD_READ:
                    {
                        if (temp_len < resp->hdr.len) {
                            memcpy(resp->buf, temp, temp_len);
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
                        resp = malloc(sizeof(response_t));
                        assert(resp != NULL);
                        DBG_PRINT("Allocated resp: %p\n", resp);
                        resp->buf = NULL;

                        read_uint32_t((uint8_t *)cur_buf->buf + cur_buf->offset, HEADER_SIZE, &resp->header_len);
                        cur_buf->offset += HEADER_SIZE;
                        rem_size -= HEADER_SIZE;
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
                        resp->buf = malloc(resp->hdr.len);
                        assert(resp->buf != NULL);
                        DBG_ALLOC("ALLOC resp->buf: %p\n", resp->buf);
                        cur_buf->offset += resp->header_len;
                        stage = PAYLOAD_READ;
                    }
                    break;
                case PAYLOAD_READ:
                    {
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
                            /* request is done, lets push it into the queue */
                            DBG_VERBOSE("GOT THE RESPONSE for ID: %u\n", resp->hdr.id);
                            queue_push(client->res_q, (void *)resp);
                            resp = NULL;
                            stage = HEADER_LEN_READ;
                        }
                    }
                    break;
                default:
                    DBG_ERR("Invalid stage\n");
                    assert(0);
            }
        }
    }
    DBG_FUNC_EXIT();
}
