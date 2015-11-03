#include "libuv-wrap.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <limits.h>

#define within(num) (int) ((float) num * random() / (RAND_MAX + 1.0))

typedef struct 
{
    int num;
    handle_t handle;
    int max_requests;
    const char * master_addr;
    int port;
    uint32_t total_msec;
    uint32_t min_value;
    uint32_t max_value;
    uint32_t failures;
}test_task_t;

uint32_t get_diff_time(struct timeval tstart, struct timeval tend)
{
    struct timeval tdiff;

    //cout << "v " << tstart.tv_sec << " " << tstart.tv_usec << endl;
    if (tend.tv_usec < tstart.tv_usec) {
        tdiff.tv_sec = tend.tv_sec - tstart.tv_sec - 1;
        tdiff.tv_usec = 1000000 + tend.tv_usec - tstart.tv_usec;
    } else {
        tdiff.tv_sec = tend.tv_sec - tstart.tv_sec;
        tdiff.tv_usec = tend.tv_usec - tstart.tv_usec;
    }
    return (tdiff.tv_sec * 1000) + (tdiff.tv_usec/1000);
}

void client_test_task(void * arg)
{
    test_task_t * task = arg;
 //   char buf[1004] = {0};
    int ret = -1;
    int req_id;
//    char recv_buf[1024] = {0};
    int more = 0;
    handle_t handle;
    uint32_t total_msec = 0;
    struct timeval starttime, endtime;
    uint32_t time_taken;

    char * buf = malloc((sizeof(char) * 16 * 1024)- 20);
    assert(buf != NULL);
    char * recv_buf = malloc(sizeof(char) * 16 * 1024);
    assert(recv_buf != NULL);

    ret = libuv_connect(task->master_addr, task->port, &handle);
    if (ret < 0) {
        printf("failed to connect to the master\n");
        return;
    }
    for (int i = 0; i < task->max_requests; ++i) {
        gettimeofday(&starttime, NULL);
        more = 0;
        ret = libuv_send(handle, (const uint8_t *)buf, 16 * 1024 - 20, &req_id);
        if (ret == 0) {
            do {
                ret = libuv_recv(handle, req_id, (uint8_t *)recv_buf, 16 * 1024, &more);
                if (ret < 0) {
                    ++task->failures;
                } 
            } while(more == 1);
            gettimeofday(&endtime, NULL);
            time_taken = get_diff_time(starttime, endtime);
            total_msec += time_taken;
            if (time_taken < task->min_value) task->min_value = time_taken;
            if (time_taken > task->max_value) task->max_value = time_taken;
        } else {
            ++task->failures;
        }
        usleep(within(400 * 1000));
    }
    task->total_msec += total_msec;
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
    srandom((unsigned) time(NULL));
    for (int num = 0; num < concurrency; ++num) {
        tasks[num].num = num;
        tasks[num].max_requests = repeat;
        tasks[num].master_addr = argv[1];
        tasks[num].port = port;
        tasks[num].total_msec = 0;
        tasks[num].min_value = INT_MAX;
        tasks[num].max_value = INT_MIN;
        tasks[num].failures = 0;
        int ret = uv_thread_create(&ids[num], client_test_task, (void *)&tasks[num]);
        assert(ret == 0);
    }
    getchar();
    uint64_t total_msec = 0;
    uint64_t min_value = INT_MAX;
    uint64_t max_value = INT_MIN;
    uint32_t failures = 0;
    for (int num = 0; num < concurrency; ++num) {
        total_msec += tasks[num].total_msec;
        if (min_value > tasks[num].min_value) {
            min_value = tasks[num].min_value;
        } 
        if (max_value < tasks[num].max_value) {
            max_value = tasks[num].max_value;
        }
        failures += tasks[num].failures;
    }
    printf("Total requests: %d concurrency: %d\n", repeat * concurrency, concurrency);
    printf("Total millisec: %lu\n", total_msec);
    printf("Min value: %lu\n", min_value);
    printf("Max value: %lu\n", max_value);
    printf("Failures: %u\n", failures); 
    return 0;
}

