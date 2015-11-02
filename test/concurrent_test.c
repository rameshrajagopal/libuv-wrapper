#include "libuv-wrap.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>

typedef struct 
{
    int num;
    handle_t handle;
    int max_requests;
    const char * master_addr;
    int port;
}test_task_t;

void client_test_task(void * arg)
{
    test_task_t * task = arg;
    char buf[1004] = {0};
    int ret = -1;
    int req_id;
    char recv_buf[1024] = {0};
    int more = 0;
    handle_t handle;
    int other_failures = 0;

    ret = libuv_connect(task->master_addr, task->port, &handle);
    if (ret < 0) {
        printf("failed to connect to the master\n");
        return;
    }
    int success = 0;
    for (int i = 0; i < task->max_requests; ++i) {
        memset(buf, 'a' + task->num, sizeof(buf));
        ret = libuv_send(handle, (const uint8_t *)buf, sizeof(buf), &req_id);
        if (ret == 0) {
            ret = libuv_recv(handle, req_id, (uint8_t *)recv_buf, sizeof(recv_buf), &more);
            if (ret < 0) {
                ++other_failures;
            } else {
                printf("success: %d\n", ++success);
            }
        } else {
            ++other_failures;
        }
        usleep(300 * 1000);
    }
    printf("failures: %d\n", other_failures);
}

int main(int argc, char * argv[])
{
    if (argc != 5) {
        printf("Usage:\n");
        printf("%s master_ip port concurrency repeat\n", argv[0]);
        return -1;
    }
    int port = atoi(argv[2]);
    int concurrency = atoi(argv[3]);
    int repeat = atoi(argv[4]);

    uv_thread_t ids[concurrency];
    test_task_t tasks[concurrency];
    for (int num = 0; num < concurrency; ++num) {
        tasks[num].num = num;
        tasks[num].max_requests = repeat;
        tasks[num].master_addr = argv[1];
        tasks[num].port = port;
        int ret = uv_thread_create(&ids[num], client_test_task, (void *)&tasks[num]);
        assert(ret == 0);
    }
    getchar();
    return 0;
}

