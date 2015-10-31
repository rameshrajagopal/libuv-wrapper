#ifndef _REQUEST_RESPONSE_H_INCLUDED_
#define _REQUEST_RESPONSE_H_INCLUDED_

#include <stdio.h>
#include <uv.h>

#define HEADER_SIZE_LEN   (4) /*fixed, should not be changed */
#define HEADER_MAGIC      (0xDEADBEEF)

typedef struct pkt_hdr_s
{
    uint32_t magic;
    uint32_t len;
    uint32_t id;
    uint32_t future;
}pkt_hdr_t;

typedef enum {HEADER_LEN_READ = 1, HEADER_READ = 2, PAYLOAD_READ = 3} read_stages_t;

/*packet format */
//<headerlen> <header> <payload>
void read_uint32_t(uint8_t * buf, int len, uint32_t * value);
void read_pkt_hdr(uint8_t * buf, int len, pkt_hdr_t * hdr);
uv_buf_t  create_request(const uint8_t * req, uint32_t len, uint32_t id);


#endif /*_REQUEST_RESPONSE_H_INCLUDED_ */
