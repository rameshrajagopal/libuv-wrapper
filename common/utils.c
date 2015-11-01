#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <uv.h>
#include <utils.h>

queue_t * queue_init(void)
{
    queue_t * q = malloc(sizeof(queue_t));
    assert(q != NULL);
    q->cnt = 0;
    q->tail = q->head = NULL;
    uv_mutex_init(&q->mutex);
    uv_cond_init(&q->cond);
    return q;
}

bool is_empty(queue_t * q)
{
    uv_mutex_lock(&q->mutex);
    bool empty = (q->cnt == 0);
    uv_mutex_unlock(&q->mutex);
    return empty;
}

void queue_push_front(queue_t * q, const void * data)
{
    printf("%s:\n", __FUNCTION__);
    queue_data_t * dnode = malloc(sizeof(queue_data_t));
    assert(dnode != NULL);
    dnode->data = data;
    dnode->next = NULL;
    uv_mutex_lock(&q->mutex);
    if (q->head == NULL) {
        q->head = q->tail = dnode;
    } else {
        dnode->next = q->head;
        q->head = dnode;
    }
    ++q->cnt;
    if (q->tail == NULL) q->tail = q->head;
    uv_mutex_unlock(&q->mutex);
}

void queue_push(queue_t * q, const void * data)
{
    queue_data_t * dnode = malloc(sizeof(queue_data_t));
    assert(dnode != NULL);
    dnode->data = data;
    dnode->next = NULL;
    uv_mutex_lock(&q->mutex);
    if (q->tail) {
        q->tail->next = dnode;
    }
    q->tail = dnode;
    if (q->head == NULL) q->head = q->tail;
    ++q->cnt;
    uv_mutex_unlock(&q->mutex);
    uv_cond_signal(&q->cond);
}

const void * queue_pop(queue_t * q)
{
    queue_data_t * data;
    const void * ret;
    uv_mutex_lock(&q->mutex);
    while (q->cnt == 0) {
        uv_cond_wait(&q->cond, &q->mutex);
    }
    data = q->head;
    q->head = q->head->next;
    --q->cnt;
    if (q->cnt == 0) q->tail = q->head;
    uv_mutex_unlock(&q->mutex);
    ret = data->data;
    free(data);
    return ret;
}

void queue_deinit(queue_t * q)
{
    printf("queue_deinit implement it\n");
}
