// This file is part of Emonk released under the MIT license.
// See the LICENSE file for more information.

/* Adapted by: Maas-Maarten Zeeman <mmzeeman@xs4all.nl */
// Modified by Sergej Jurecko

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "queue.h"
#define BLOCK_SIZE 32


// void*
// queue_get_item_data(void* item)
// {
//     qitem* it = (qitem*)item;
//     return it->data;
// }
//
// void
// queue_set_item_data(void* item, void *data)
// {
//     qitem* it = (qitem*)item;
//     it->data = data;
// }

queue *queue_create()
{
    queue *ret;
    // int i = 0;
    // qitem *item;

    ret = (queue *) enif_alloc(sizeof(struct queue_t));
    if(ret == NULL) goto error;

    // ret->freeitem = freecb;
    ret->lock = NULL;
    ret->cond = NULL;
    ret->head = NULL;
    ret->tail = NULL;
    ret->length = 0;

    ret->reuseq = NULL;

    ret->lock = enif_mutex_create("queue_lock");
    if(ret->lock == NULL) goto error;

    ret->cond = enif_cond_create("queue_cond");
    if(ret->cond == NULL) goto error;

    return ret;

error:
    if(ret->lock != NULL)
        enif_mutex_destroy(ret->lock);
    if(ret->cond != NULL)
        enif_cond_destroy(ret->cond);
    if(ret != NULL)
        enif_free(ret);
    return NULL;
}

void
queue_destroy(queue *queue)
{
    ErlNifMutex *lock;
    ErlNifCond *cond;
    int length;
    qitem *blocks = NULL;

    enif_mutex_lock(queue->lock);
    lock = queue->lock;
    cond = queue->cond;
    length = queue->length;

    queue->lock = NULL;
    queue->cond = NULL;
    queue->head = NULL;
    queue->tail = NULL;
    queue->length = -1;
    while(queue->reuseq != NULL)
    {
        qitem *tmp = queue->reuseq->next;
        if(tmp != NULL && tmp->env != NULL)
           enif_free_env(tmp->env);
        if (tmp != NULL && tmp->cmd != NULL)
            enif_free(tmp->cmd);
        if (queue->reuseq->blockStart)
        {
          queue->reuseq->next = blocks;
          blocks = queue->reuseq;
        }
        queue->reuseq = tmp;
    }
    while (blocks != NULL)
    {
      qitem *tmp = blocks->next;
      enif_free(blocks);
      blocks = tmp;
    }
    enif_mutex_unlock(lock);

    assert(length == 0 && "Attempting to destroy a non-empty queue.");
    enif_cond_destroy(cond);
    enif_mutex_destroy(lock);
    enif_free(queue);
}


int
queue_push(queue *queue, qitem *entry)
{
    enif_mutex_lock(queue->lock);

    assert(queue->length >= 0 && "Invalid queue size at push");

    if(queue->tail != NULL)
        queue->tail->next = entry;

    queue->tail = entry;

    if(queue->head == NULL)
        queue->head = queue->tail;

    queue->length += 1;

    enif_cond_signal(queue->cond);
    enif_mutex_unlock(queue->lock);

    return 1;
}

int queue_size(queue *queue)
{
    int r = 0;
    enif_mutex_lock(queue->lock);
    r = queue->length;
    enif_mutex_unlock(queue->lock);
    return r;
}

qitem*
queue_pop(queue *queue)
{
    qitem *entry;

    enif_mutex_lock(queue->lock);

    /* Wait for an item to become available.
     */
    while(queue->head == NULL)
        enif_cond_wait(queue->cond, queue->lock);

    assert(queue->length >= 0 && "Invalid queue size at pop.");

    /* Woke up because queue->head != NULL
     * Remove the entry and return the payload.
     */
    entry = queue->head;
    queue->head = entry->next;
    entry->next = NULL;

    if(queue->head == NULL) {
        assert(queue->tail == entry && "Invalid queue state: Bad tail.");
        queue->tail = NULL;
    }

    queue->length -= 1;

    enif_mutex_unlock(queue->lock);

    return entry;
}

void
queue_recycle(queue *queue,qitem *entry)
{
    enif_mutex_lock(queue->lock);
    entry->next = queue->reuseq;
    queue->reuseq = entry;
    enif_mutex_unlock(queue->lock);
}

qitem*
queue_get_item(queue *queue)
{
    qitem *entry = NULL;
    int i;
    enif_mutex_lock(queue->lock);
    if (queue->reuseq != NULL)
    {
        entry = queue->reuseq;
        queue->reuseq = queue->reuseq->next;
        entry->next = NULL;
    }
    else
    {
        entry = enif_alloc(sizeof(qitem)*BLOCK_SIZE);
        memset(entry,0,sizeof(qitem)*BLOCK_SIZE);

        for (i = 1; i < BLOCK_SIZE; i++)
        {
          entry[i].env = enif_alloc_env();
          entry[i].next = queue->reuseq;
          queue->reuseq = &entry[i];
        }
        entry->env = enif_alloc_env();
        entry->blockStart = 1;
    }
    enif_mutex_unlock(queue->lock);
    return entry;
}

