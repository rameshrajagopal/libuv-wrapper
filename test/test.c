#include "libuv-wrap.h"
#include <stdio.h>

int main(void)
{
    handle_t handle;
    int ret = libuv_connect("127.0.0.1", 8000, &handle);
    printf("test: connect status: %d\n", ret);
    if (ret < 0) {
        return -1;
    }
    char buf[1024] = {0};
    ret = libuv_send(handle, (const uint8_t *)buf, sizeof(buf));
    printf("test: write status: %d\n", ret);
    getchar();
    return 0;
}
