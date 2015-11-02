#ifndef _REQUEST_RESPONSE_H_INCLUDED_
#define _REQUEST_RESPONSE_H_INCLUDED_

#include <stdio.h>
#include <uv.h>
#include <utils.h>

#define HEADER_SIZE_LEN   (4) /*fixed, should not be changed */
#define HEADER_MAGIC      (0xDEADBEEF)

typedef enum { ACTIVE = 1, CLOSED = 2} connect_status_t;

typedef struct pkt_hdr_s
{
    uint32_t magic;
    uint32_t len;
    uint32_t id;
    uint32_t future;
}pkt_hdr_t;

typedef struct {
    uint32_t  header_len;
    pkt_hdr_t hdr;
    uint8_t * buf;
}response_t;

typedef struct {
    uint32_t  header_len;
    pkt_hdr_t hdr;
    uint8_t * buf;
    uv_handle_t * cinfo;
}request_t;

typedef enum {INVALID_STAGE = 0, HEADER_LEN_READ = 1, HEADER_READ = 2, PAYLOAD_READ = 3} read_stages_t;

typedef struct {
    char * buf;
    ssize_t len;
    ssize_t offset;
    request_t * req;
    read_stages_t stage;
}req_buf_t;

typedef struct {
    char * buf;
    ssize_t len;
    ssize_t offset;
}txn_buf_t;


/*packet format */
//<headerlen> <header> <payload>
void read_uint32_t(uint8_t * buf, int len, uint32_t * value);
void read_pkt_hdr(uint8_t * buf, int len, pkt_hdr_t * hdr);
uv_buf_t  create_request(const uint8_t * req, uint32_t len, uint32_t id);
uv_buf_t  create_response(const uint8_t * res, uint32_t len, uint32_t id);
req_buf_t * req_buf_init(char * base, ssize_t nread);
txn_buf_t * txn_buf_init(char * base, ssize_t nread);



#endif /*_REQUEST_RESPONSE_H_INCLUDED_ */
