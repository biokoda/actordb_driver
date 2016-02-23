// This file is part of Emonk released under the MIT license.
// See the LICENSE file for more information.

/* Adapted by: Maas-Maarten Zeeman <mmzeeman@xs4all.nl */
// Modified by Sergej Jurecko

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "queue.h"
#define BLOCK_SIZE 128


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

#ifndef _TESTAPP_
    #if USE_SEM
        static void qwait(queue *q)
        {
            while (q->head == NULL)
            {
                enif_mutex_unlock(q->lock);
                cond_wait(q->cond, q->lock);
                while (enif_mutex_trylock(q->lock) != 0)
                {
                }
            }
        }
    #else
        static void qwait(queue *q)
        {
            while (q->head == NULL)
                cond_wait(q->cond, q->lock);
        }
    #endif
#endif

queue *queue_create()
{
    queue *ret;
    // int i = 0;
    // qitem *item;

    ret = (queue *) enif_alloc(sizeof(struct queue_t));
    if(ret == NULL) goto error;
    memset(ret, 0, sizeof(struct queue_t));

    ret->lock = enif_mutex_create("queue_lock");
    if(ret->lock == NULL) goto error;

    cond_init(ret->cond);

    return ret;

error:
    if(ret)
    {
        enif_mutex_destroy(ret->lock);
        cond_destroy(ret->cond);
        enif_free(ret);
    }
    return NULL;
}

void
queue_destroy(queue *queue)
{
    ErlNifMutex *lock;
    COND_T cond;
    qitem *blocks = NULL;

    enif_mutex_lock(queue->lock);
    lock = queue->lock;
    cond = queue->cond;

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

    cond_destroy(cond);
    enif_mutex_destroy(lock);
    enif_free(queue);
}


int
queue_push(queue *queue, qitem *entry)
{
    while (enif_mutex_trylock(queue->lock) != 0)
    {
    }

    assert(queue->length >= 0 && "Invalid queue size at push");

    if(queue->tail != NULL)
        queue->tail->next = entry;

    queue->tail = entry;

    if(queue->head == NULL)
        queue->head = queue->tail;

    queue->length += 1;
    cond_signal(queue->cond);
    enif_mutex_unlock(queue->lock);

    return 1;
}

int queue_size(queue *queue)
{
    int r = 0;
    while (enif_mutex_trylock(queue->lock) != 0)
    {
    }
    r = queue->length;
    enif_mutex_unlock(queue->lock);
    return r;
}

qitem*
queue_pop(queue *queue)
{
    qitem *entry;

    while (enif_mutex_trylock(queue->lock) != 0)
    {
    }
    qwait(queue);

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

    queue->length--;

    enif_mutex_unlock(queue->lock);

    return entry;
}

void
queue_recycle(queue *queue,qitem *entry)
{
    while (enif_mutex_trylock(queue->lock) != 0)
    {
    }
    entry->next = queue->reuseq;
    queue->reuseq = entry;
    enif_mutex_unlock(queue->lock);
}

qitem*
queue_get_item(queue *queue)
{
    qitem *entry = NULL;
    int i;
    while (enif_mutex_trylock(queue->lock) != 0)
    {
    }
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

