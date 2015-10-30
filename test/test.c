#include "libuv-wrap.h"
#include <assert.h>
#include <stdio.h>

typedef struct 
{
    int num;
    handle_t handle;
}test_task_t;

#define MAX_NUM_THREADS  (5)

void client_test_task(void * arg)
{
    test_task_t * task = (test_task_t *)arg;
    char buf[1024] = {0};
    int ret = -1;
    int req_id;

    ret = libuv_send(task->handle, (const uint8_t *)buf, sizeof(buf), &req_id);
    printf("test:%d write status: %d req_id: %d\n", task->num, ret, req_id);
}

int main(void)
{
    handle_t handle;
    int ret = libuv_connect("127.0.0.1", 8000, &handle);
    printf("test: connect status: %d\n", ret);
    if (ret < 0) {
        return -1;
    }
    uv_thread_t ids[MAX_NUM_THREADS];
    test_task_t tasks[MAX_NUM_THREADS];
    for (int num = 0; num < MAX_NUM_THREADS; ++num) {
        tasks[num].num = num;
        tasks[num].handle = handle;
        ret = uv_thread_create(&ids[num], client_test_task, (void *)&tasks[num]);
        assert(ret == 0);
    }
    getchar();
    return 0;
}
