#ifndef LFQUEUE_H
#define LFQUEUE_H

#ifndef _TESTAPP_
#include "erl_nif.h"
#endif
#include "platform.h"

typedef struct queue_t queue;
typedef struct qitem_t qitem;
typedef struct intq intq;

struct qitem_t
{
	_Atomic (struct qitem_t*) next;
	void *cmd;
	#ifndef _TESTAPP_
	ErlNifEnv *env;
	#endif
	char blockStart;
	// Every pair of producer-consumer has a reuse queue.  
	// This way we're not constantly doing allocations.
	// Home is a queue that is attached to every producer 
	// (scheduler) thread.
	intq *home;
};

struct intq
{
	_Atomic (qitem*) head;
	qitem* tail;
};

struct queue_t
{
	struct intq q;
	SEMAPHORE sem;
	size_t length;
};

queue *queue_create(void);
void queue_destroy(queue *queue);

int queue_push(queue *queue, qitem* item);
qitem* queue_pop(queue *queue);
qitem* queue_trypop(queue *queue);
qitem* queue_timepop(queue *queue, uint32_t miliseconds);

void queue_recycle(qitem* item);
qitem* queue_get_item(void);
void queue_intq_destroy(intq *q);

#endif 
