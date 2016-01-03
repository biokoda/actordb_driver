#ifndef _LFQUEUE_H_
#define _LFQUEUE_H_

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#ifndef _TESTAPP_
#include "erl_nif.h"
#endif

typedef struct qitem
{
	void         *cmd;
	#ifndef _TESTAPP_
	ErlNifEnv    *env;
	#endif
	struct qitem *next;
} qitem;

typedef struct queue
{
	char       *buf;
	atomic_int size;
	atomic_int ct;
	int        last_map_pos;
	long long int visited;
	int        npages;
	int        map_elements;
	int        bufbytes;
	int        mapbytes;
} queue;

queue *queue_create(const int npages);
void queue_destroy(queue *queue);
qitem* queue_get_item(queue *queue);
int queue_push(queue *queue, qitem* item);
qitem* queue_pop(queue *queue);
void queue_recycle(queue *queue, qitem* item);
int queue_size(queue *queue);
int queue_getct(queue *q);

#endif
