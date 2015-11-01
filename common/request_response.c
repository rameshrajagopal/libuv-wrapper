#include "request_response.h"
#include <assert.h>
#include <string.h>
#include <stdlib.h>

void read_uint32_t(uint8_t * buf, int len, uint32_t * value)
{
   /* need to take of endianess and network byte order */
   memcpy((uint8_t *)value, buf, len); 
}

void read_pkt_hdr(uint8_t * buf, int len, pkt_hdr_t * hdr)
{
    /* needs to be fixed 
     * structure padding 
     * network byte order
     * if structure changes in the future, how to take care of it 
     */
    size_t offset = 0;
    memcpy((uint8_t *)&hdr->magic, buf, sizeof(hdr->magic));
    offset += sizeof(hdr->magic);
    memcpy((uint8_t *)&hdr->len, buf + offset, sizeof(hdr->len));
    offset += sizeof(hdr->len);
    memcpy((uint8_t *)&hdr->id, buf + offset, sizeof(hdr->id));
    offset += sizeof(hdr->id);
    memcpy((uint8_t *)&hdr->future, buf + offset, sizeof(hdr->future));
}

void write_pkt_hdr(pkt_hdr_t * hdr, uint8_t * buf)
{
    /* needs to be fixed 
     * structure padding 
     * network byte order
     * if structure changes in the future, how to take care of it 
     */
    size_t offset = 0;
    memcpy(buf, (uint8_t *)&hdr->magic, sizeof(hdr->magic));
    offset += sizeof(hdr->magic);
    memcpy( buf + offset,(uint8_t *)&hdr->len, sizeof(hdr->len));
    offset += sizeof(hdr->len);
    memcpy(buf + offset, (uint8_t *)&hdr->id, sizeof(hdr->id));
    offset += sizeof(hdr->id);
    memcpy(buf + offset, (uint8_t *)&hdr->future, sizeof(hdr->future));
}

uv_buf_t  create_request(const uint8_t * req, uint32_t len, uint32_t id)
{
    pkt_hdr_t hdr = {0};
    uint32_t header_len = sizeof(hdr);
    uint32_t total_len =  HEADER_SIZE_LEN + header_len + len;
    uint32_t offset = 0;

    uv_buf_t buf;
    buf.base = malloc(total_len * sizeof(char));
    assert(buf.base != NULL);
    buf.len  = total_len;
    memcpy(buf.base, &header_len, HEADER_SIZE_LEN);
    offset += HEADER_SIZE_LEN;
    hdr.magic = HEADER_MAGIC;
    hdr.len   = len;
    hdr.id    = id;
    hdr.future = 0;
    printf("REQ HEADER: %x %x %x\n", hdr.magic, hdr.len, hdr.id);
    write_pkt_hdr(&hdr, (uint8_t *)buf.base + offset);
    offset += sizeof(hdr);
    memcpy(buf.base + offset, req, len);
    return buf;
}

uv_buf_t  create_response(const uint8_t * res, uint32_t len, uint32_t id)
{
    pkt_hdr_t hdr = {0};
    uint32_t header_len = sizeof(hdr);
    uint32_t total_len =  HEADER_SIZE_LEN + header_len + len;
    uint32_t offset = 0;

    uv_buf_t buf;
    buf.base = malloc(total_len * sizeof(char));
    assert(buf.base != NULL);
    buf.len  = total_len;
    memcpy(buf.base, &header_len, HEADER_SIZE_LEN);
    offset += HEADER_SIZE_LEN;
    hdr.magic = HEADER_MAGIC; 
    hdr.len   = len;
    hdr.id    = id;
    hdr.future = 0;
    printf("RES HEADER: %x %x %x\n", hdr.magic, hdr.len, hdr.id);
    write_pkt_hdr(&hdr, (uint8_t *)buf.base + offset);
    offset += sizeof(hdr);
    memcpy(buf.base + offset, res, len);
    return buf;
}

txn_buf_t * txn_buf_init(char * base, ssize_t nread)
{
    txn_buf_t * txn = malloc(sizeof(txn_buf_t));
    assert(txn != NULL);
    txn->buf = base;
    txn->len = nread;
    txn->offset = 0;
    return txn;
}

req_buf_t * req_buf_init(char * base, ssize_t nread)
{
    DBG_FUNC_ENTER();
    req_buf_t * buf = malloc(sizeof(req_buf_t));
    assert(buf != NULL);
    buf->buf = base;
    buf->len = nread;
    buf->offset = 0;
    buf->req = NULL;
    buf->stage = INVALID_STAGE;
    DBG_ALLOC("ALLOC req_buf: %p\n", buf);
    DBG_FUNC_EXIT();
    return buf;
}

