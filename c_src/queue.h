// This file is part of Emonk released under the MIT license. 
// See the LICENSE file for more information.

/* adapted by: Maas-Maarten Zeeman <mmzeeman@xs4all.nl */

#ifndef QUEUE_H
#define QUEUE_H

#ifndef _TESTAPP_
#include "erl_nif.h"
#endif

#ifdef _WIN32
#define USE_SEM 0
#else
#define USE_SEM 0
#endif

#if USE_SEM
    #if defined(__APPLE__)
        #include <dispatch/dispatch.h>
        #define COND_T dispatch_semaphore_t
        #define cond_destroy(X) dispatch_release(X)
        #define cond_init(X)  X = dispatch_semaphore_create(0)
        #define cond_signal(X) dispatch_semaphore_signal(X)
        #define cond_wait(X,MTX) dispatch_semaphore_wait(X, DISPATCH_TIME_FOREVER)
    #else
        #include <semaphore.h>
        #define COND_T sem_t
        #define cond_destroy(X) sem_destroy(&X)
        #define cond_init(X) sem_init(&X, 0, 0)
        #define cond_signal(X) sem_post(&X)
        #define cond_wait(X,MTX) sem_wait(&X)
    #endif
#else
    #define COND_T ErlNifCond*
    #define cond_destroy enif_cond_destroy
    #define cond_init(X) X = enif_cond_create("queue_cond")
    #define cond_signal(X) enif_cond_signal(X)
    #define cond_wait(C,MTX) enif_cond_wait(C, MTX)
#endif

typedef struct queue_t queue;
typedef struct qitem_t qitem;

struct qitem_t
{
	qitem* next;
	void *cmd;
	#ifndef _TESTAPP_
	ErlNifEnv *env;
	#endif
	char blockStart;
};

struct queue_t
{
#ifndef _TESTAPP_
    ErlNifMutex *lock;
    COND_T cond;
#endif
    qitem *head;
    qitem *tail;
    qitem *reuseq;
    // void (*freeitem)(db_command);
    int length;
};

queue *queue_create(void);
void queue_destroy(queue *queue);

// int queue_has_item(queue *queue);

int queue_push(queue *queue, qitem* item);
qitem* queue_pop(queue *queue);

void queue_recycle(queue *queue, qitem* item);

qitem* queue_get_item(queue *queue);
int queue_size(queue *queue);

// int queue_send(queue *queue, void* item);
// void* queue_receive(queue *);

#endif 
