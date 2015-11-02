#include "libuv-wrap.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>

typedef struct 
{
    int num;
    handle_t handle;
    int num_requests;
}test_task_t;

#define MAX_NUM_THREADS  (5)

static int num_failures = 0;

void client_test_task(void * arg)
{
    test_task_t * task = (test_task_t *)arg;
    char buf[1004] = {0};
    int ret = -1;
    int req_id;
    char recv_buf[1024] = {0};
    int more = 0;

    for (int i = 0; i < task->num_requests; ++i) {
        memset(buf, 'a' + task->num, sizeof(buf));
        ret = libuv_send(task->handle, (const uint8_t *)buf, sizeof(buf), &req_id);
        if (ret == 0) {
            ret = libuv_recv(task->handle, req_id, (uint8_t *)recv_buf, sizeof(recv_buf), &more);
            if (ret < 0) {
                ++num_failures;
            }
        } else {
            ++num_failures;
        }
        usleep(300 * 1000);
    }
}

int main(int argc, char * argv[])
{
    handle_t handle;
    if (argc != 5) {
        printf("Usage:\n");
        return -1;
    }
    const char * master_addr = argv[1];
    int port = atoi(argv[2]);
    int concurrency = atoi(argv[3]);
    int repeat = atoi(argv[4]);

    int ret = libuv_connect(master_addr, port, &handle);
    printf("test: connect status: %d\n", ret);
    if (ret < 0) {
        return -1;
    }
    uv_thread_t ids[concurrency];
    test_task_t tasks[concurrency];
    for (int num = 0; num < concurrency; ++num) {
        tasks[num].num = num;
        tasks[num].handle = handle;
        tasks[num].num_requests = repeat;
        ret = uv_thread_create(&ids[num], client_test_task, (void *)&tasks[num]);
        assert(ret == 0);
    }
    getchar();
    return 0;
}
