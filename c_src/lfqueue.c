#include "lfqueue.h"
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
// usleep value
#define DELAY_BY 1
#define PGSZ sizeof(qitem)
#define ffz(x) ffsll(~(x))

// Lock free queue
// Uses a single buffer divided into 3 parts
// [ReserveBMap,UsedBMap,Items]
// The two bitmaps are used for thread sync.
// - ReserveBMap is used to reserve space for producer threads.
// - UsedBMap is used to signal consumer thread that data available to use. 

// !!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!!
// -> DOES NOT ENSURE A CONSISTENT SEQUENCE OF EVENTS
//   ActorDB does not need it, a single actor always waits for event to complete before
//   issuing a new one.

// Not used as it is actually slower than mutex based queue.
// It does appear to work fine however.
// Normal queue.c does not have enough contention for lock free to be a benefit.

queue *queue_create(const int npages)
{
	int i;
	atomic_llong *map;
	queue *q;
	qitem *buf;

	if (npages % 64)
		return NULL;

	q = malloc(sizeof(queue));
	q->npages = npages;
	q->map_elements = (npages/(sizeof(long long int)*8));
	q->mapbytes = q->map_elements*sizeof(long long int);
	q->bufbytes = PGSZ*npages;
	q->buf = malloc(PGSZ*npages + 2*npages*sizeof(long long int));
	q->last_map_pos = 0;
	q->visited = ((long long int)0);
	map = (atomic_llong*)q->buf;
	buf = (qitem*)(q->buf + q->mapbytes*2);

	memset(q->buf+q->mapbytes*2, 0, q->bufbytes);
	for (i = 0; i < q->map_elements*2; i++)
	{
		atomic_init(&map[i],0);
	}
	atomic_init(&q->size,0);
	atomic_init(&q->ct,0);
	#ifndef _TESTAPP_
	for (i = 0; i < q->npages; i++)
	{
		buf[i].env = enif_alloc_env();
		buf[i].next = NULL;
	}
	#endif
	return q;
}

void queue_destroy(queue *q)
{
	int i;
	qitem *buf = (qitem*)(q->buf + q->mapbytes*2);

	#ifndef _TESTAPP_
	for (i = 0; i < q->npages; i++)
	{
		if (buf[i].env != NULL)
			enif_free_env(buf[i].env);
		if (buf[i].cmd != NULL)
			enif_free(buf[i].cmd);
	}
	#endif
	free(q->buf);
	free(q);
}

// Sets UsedBMap to signal consumer it can be used.
int queue_push(queue *q, qitem* item)
{
	qitem *buf        = (qitem*)(q->buf + q->mapbytes*2);
	atomic_llong *map = (atomic_llong*)(q->buf + q->mapbytes);
	const int diff    = (item - buf);
	const int i       = diff / 64;
	const int zbit    = diff % 64;

	while (1)
	{
		long long int mval = atomic_load(&map[i]);
		long long int nval;

		nval = mval | (((long long int)1) << zbit);

		if (atomic_compare_exchange_strong(&map[i], &mval, nval))
		{
			// printf("PUSHING ON POS i=%d zbit=%d diff=%d rdiff=%lld\n",i, zbit, diff, item-buf);
			break;
		}
		else
			atomic_fetch_add(&q->ct, 1);

		usleep(DELAY_BY);
	}
	return 1;
}

// Reserves a free slot in ReserveBMap
qitem* queue_get_item(queue *q)
{
	int i;
	qitem *buf        = (qitem*)(q->buf + q->mapbytes*2);
	atomic_llong *map = (atomic_llong*)q->buf;
	int zbit;
	qitem *r;

	while(1)
	{
		// Reserve space
		for (i = 0; i < q->map_elements; i++)
		{
			long long int mval = atomic_load(&map[i]);
			long long int nval;

			// if no zero bit go to next element
			if (!(zbit = ffz(mval)))
			{
				continue;
			}

			nval = mval | (((long long int)1) << (--zbit));

			// update map val with bit set. 
			// If successful we are done.
			if (atomic_compare_exchange_strong(&map[i], &mval, nval))
			{
				// printf("ZBIT %d %lld %lld\n",zbit,mval, sizeof(mval));
				atomic_fetch_add(&q->size, 1);
				break;
			}
			// Unable to exchange, go again for same index. 
			else
			{
				atomic_fetch_add(&q->ct, 1);
				i--;
			}
		}

		if (i < q->map_elements)
		{
			r = &buf[i*64+zbit];
			break;
		}
		else
		{
			usleep(100);
		}
	}
	return r;
}

// get/wait for next item for consumer.
// Does not set any bmap bits. Item must remain in buffer
// until we are done with it.
qitem* queue_pop(queue *q)
{
	qitem *buf        = (qitem*)(q->buf + q->mapbytes*2);
	atomic_llong *map = (atomic_llong*)(q->buf + q->mapbytes);
	int sbit;
	int next;

	// printf("POP %lld, %lld %d\n",(long long int)q->buf, (long long int)map, q->mapbytes);

	while (1)
	{
		long long int mval = atomic_load(&map[q->last_map_pos]);

		if ((sbit = ffsll(mval & (~q->visited))))
		{
			// printf("SET BIT ! %d %d %lld\n",sbit,q->last_map_pos, mval);
			--sbit;
			q->visited |= (((long long int)1) << sbit);
			atomic_fetch_sub(&q->size, 1);
			return (qitem*)&buf[q->last_map_pos*64 + sbit];
		}

		if (q->last_map_pos == q->map_elements-1)
		{
			next = 0;
		}
		else
		{
			next = q->last_map_pos+1;
		}

		q->last_map_pos = next;
		q->visited = (long long int)0;
		mval = atomic_load(&map[next]);
		if ((sbit = ffsll(mval)))
		{
			--sbit;
			q->visited |= (((long long int)1) << sbit);
			atomic_fetch_sub(&q->size, 1);
			return (qitem*)&buf[next*64 + sbit];
		}
		else
		{
			q->last_map_pos = 0;
		}

		usleep(DELAY_BY);
	}
}

// Item is no longer being used. Clear bits.
// First we must clear used bmap, then reserve bmap.
void queue_recycle(queue *q, qitem* item)
{
	qitem *buf         = (qitem*)(q->buf + q->mapbytes*2);
	atomic_llong *rmap = (atomic_llong*)(q->buf);
	atomic_llong *umap = (atomic_llong*)(q->buf + q->mapbytes);
	atomic_llong *map = umap;
	const int diff    = (item - buf);
	const int i       = diff / 64;
	const int zbit    = diff % 64;

	// printf("Recycle diff=%d, i=%d, zbit=%d %lld %lld\n",diff, i, zbit, (long long int)buf, (long long int)item);
	while (1)
	{
		long long int mval = atomic_load(&map[i]);
		long long int nval;

		nval = mval & (~(((long long int)1) << zbit));

		if (atomic_compare_exchange_strong(&map[i], &mval, nval))
		{
			if (map == umap)
			{
				map = rmap;
			}
			else
			{
				break;
			}
			continue;
		}
		else
			atomic_fetch_add(&q->ct, 1);

		usleep(DELAY_BY);
	}
}

int queue_size(queue *q)
{
	return atomic_load(&q->size);
}

int queue_getct(queue *q)
{
	return atomic_load(&q->ct);
}

// 
// 
//  TEST APP
// 
// 
// gcc c_src/lfqueue.c -DTEST_LQUEUE -D_TESTAPP_ -DSQLITE_DEFAULT_PAGE_SIZE=4096 -lpthread -o lfq
#ifdef TEST_LQUEUE

typedef struct item
{
	int thread;
	int n;
}item;

typedef struct threadinf
{
	int thread;
	queue *q;
}threadinf;

#define ITERATIONS 1000000
#define NUM_THREADS 6
#define NPAGES 256

static void *producer(void *arg)
{
	threadinf *inf = (threadinf*)arg;
	char *op;
	int i;
	long long int me = (long long int)pthread_self();

	for (i = 0; i < ITERATIONS; i++)
	{
		// printf("PULL! %lld\n",me);
		item *it;
		qitem *qi = queue_get_item(inf->q);
		if (qi->cmd == NULL)
			qi->cmd = malloc(sizeof(item));
		it = (item*)qi->cmd;
		it->n = i;
		it->thread = inf->thread;
		queue_push(inf->q,qi);
		// if (tries > 10)
			// printf("%lld, i=%d, tries=%d, index=%d\n",me, i, tries, index);

		// if ((i % 10000) == 0)
		// 	printf("pthr=%lld, i=%d\n",me, i);
	}

	return NULL;
}


int main(int argc, const char* argv[])
{
	queue* q;
	int i;
	pthread_t threads[NUM_THREADS];
	threadinf infos[NUM_THREADS];
	int thrnums[NUM_THREADS];

	// less available space than threads
	q = queue_create(NPAGES);
	for (i = 0; i < NUM_THREADS; i++)
	{
		thrnums[i] = 0;
		infos[i].thread = i;
		infos[i].q = q;
		pthread_create(&threads[i], NULL, producer, (void *)&infos[i]);
	}

	for (i = 0; i < ITERATIONS*NUM_THREADS; i++)
	{
		qitem *qi = queue_pop(q);
		item *it = (item*)qi->cmd;
		// if (thrnums[it->thread] != it->n)
		// {
		// 	printf("Items not sequential thread=%d, n=%d, shouldbe=%d!!\n", it->thread, it->n, thrnums[it->thread]);
		// 	return 0;
		// }
		thrnums[it->thread]++;
		queue_recycle(q,qi);
		// printf("Recycle %d\n",i);
	}

	// for (i = 0; i < NUM_THREADS; i++)
	// 	pthread_join(threads[i],NULL);

	// printf("No thread sync errors!\n");
	return 0;
}

#endif