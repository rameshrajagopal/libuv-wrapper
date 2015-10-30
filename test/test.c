#include "libuv-wrap.h"
#include <stdio.h>


int main(void)
{
    handle_t handle;
    int ret = libuv_connect("127.0.0.1", 8000, &handle);
    printf("test: connect status: %d\n", ret);
    getchar();
}
