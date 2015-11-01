#include "libuv-wrap.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>

typedef struct 
{
    int num;
    handle_t handle;
}test_task_t;

#define MAX_NUM_THREADS  (10)
#define MAX_NUM_REQUESTS (5)

void client_test_task(void * arg)
{
    uint32_t th_num = (uint64_t)arg;
    char buf[1004] = {0};
    int ret = -1;
    int req_id;
    char recv_buf[1024] = {0};
    int more = 0;
    handle_t handle;

    ret = libuv_connect("127.0.0.1", 8000, &handle);
    printf("test: connect status: %d\n", ret);
    if (ret < 0) {
        return;
    }
    for (int i = 0; i < MAX_NUM_REQUESTS; ++i) {
        memset(buf, 'a' + th_num, sizeof(buf));
        ret = libuv_send(handle, (const uint8_t *)buf, sizeof(buf), &req_id);
        printf("test:%d write status: %d req_id: %d\n", th_num, ret, req_id);
        usleep(200 * 1000);
        ret = libuv_recv(handle, req_id, (uint8_t *)recv_buf, sizeof(recv_buf), &more);
        printf("********************test: %d read status: %d more: %d\n", th_num, ret, more);
        printf("%d: %.*s\n", th_num, ret, recv_buf);
        usleep(200 * 1000);
    }
}

int main(void)
{
    int ret;
    uv_thread_t ids[MAX_NUM_THREADS];
    for (int num = 0; num < MAX_NUM_THREADS; ++num) {
        ret = uv_thread_create(&ids[num], client_test_task, (void *)(uint64_t)num);
        assert(ret == 0);
    }
    getchar();
    return 0;
}
