// This file is part of Emonk released under the MIT license. 
// See the LICENSE file for more information.

/* adapted by: Maas-Maarten Zeeman <mmzeeman@xs4all.nl */

#ifndef QUEUE_H
#define QUEUE_H

#ifndef _TESTAPP_
#include "erl_nif.h"
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
    ErlNifCond *cond;
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
