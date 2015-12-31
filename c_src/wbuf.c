#include "wbuf.h"
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#define PGSZ SQLITE_DEFAULT_PAGE_SIZE
#define ffz(x) ffsll(~(x))

#define WBUF_MAP_ELEMENTS(NP) (NP/(sizeof(long long int)*sizeof(long long int)))
#define WBUF_MAPBYTES(NP) (NP*sizeof(long long int))
#define WBUF_SIZE(NP) (SQLITE_DEFAULT_PAGE_SIZE*NP)

char* wbuf_init(const int npages)
{
	int i;
	char *buf; 
	atomic_llong *map;

	if (npages % 64)
		return NULL;

	buf = malloc(WBUF_MAPBYTES(npages)+WBUF_SIZE(npages));
	map = (atomic_llong*)buf;

	memset(buf+WBUF_MAPBYTES(npages), 0, WBUF_SIZE(npages));

	for (i = 0; i < WBUF_MAP_ELEMENTS(npages); i++)
	{
		atomic_init(&map[i],0);
	}
	return buf;
}

int wbuf_put(const int npages, char *buf1, char *data, int *tries)
{
	int i,t = 0;
	char *buf = buf1+WBUF_MAPBYTES(npages);
	atomic_llong *map = (atomic_llong*)buf1;
	int zbit;

	while(1)
	{
		// Reserve space
		for (i = 0; i < WBUF_MAP_ELEMENTS(npages); i++)
		{
			long long int mval = atomic_load(&map[i]);
			long long int nval;

			// if no zero bit go to next element
			if (!(zbit = ffz(mval)))
				continue;

			nval = mval | (1 << (--zbit));

			// update map val with bit set. 
			// If successful we are done.
			if (atomic_compare_exchange_strong(&map[i], &mval, nval))
				break;
			// Unable to exchange, go again for same index. 
			else
			{
				i--;
				t++;
			}
		}

		if (i < WBUF_MAP_ELEMENTS(npages))
		{
			// Copy data
			memcpy(buf + i*64*PGSZ + zbit*PGSZ, data, PGSZ);
			break;
		}
		else
		{
			usleep(100);
		}
	}

	if (tries != NULL)
		*tries = t;

	return i*64 + zbit;
}

char* wbuf_get(const int npages, char *buf, int index)
{
	return buf+WBUF_MAPBYTES(npages) + index*PGSZ;
}

void wbuf_release(char *buf1, int index)
{
	atomic_llong *map = (atomic_llong*)buf1;
	int i = index / 64;
	int bit = index % 64;

	while (1)
	{
		long long int mval = atomic_load(&map[i]);
		long long int nval = mval & (~(1 << bit));
		if (atomic_compare_exchange_strong(&map[i], &mval, nval))
			break;
	}
}

// 
// 
//  TEST APP
// 
// 
// gcc c_src/wbuf.c -DTEST_WBUF -DSQLITE_DEFAULT_PAGE_SIZE=4096 -lpthread -o wb
#ifdef TEST_WBUF

#define NUM_THREADS 70
#define NPAGES 64

static void *perform(void *arg)
{
	char *wb = (char*)arg;
	char bufin[4096];
	char bufout[4096];
	char *op;
	int i;
	long long int me = (long long int)pthread_self();

	for (i = 0; i < sizeof(bufin); i++)
		bufin[i] = i;
	memcpy(bufin, &me, sizeof(long long int));
	memcpy(bufout, bufin, sizeof(bufin));

	for (i = 0; i < 1000000; i++)
	{
		int tries = 0;
		int index = wbuf_put(NPAGES, wb, bufin, &tries);
		op = wbuf_get(NPAGES, wb, index);
		memcpy(bufout, op, sizeof(bufout));
		wbuf_release(wb, index);

		if (memcmp(bufin, bufout, sizeof(bufin)) != 0)
		{
			printf("ERROR! %d\n", i);
			exit(1);
		}
		// if (tries > 10)
			// printf("%lld, i=%d, tries=%d, index=%d\n",me, i, tries, index);

		if ((i % 10000) == 0)
			printf("%lld, i=%d, tries=%d, index=%d\n",me, i, tries, index);
	}

	return NULL;
}


int main(int argc, const char* argv[])
{
	char* w;
	int i;
	pthread_t threads[NUM_THREADS];

	// less available space than threads
	w = wbuf_init(NPAGES);
	for (i = 0; i < NUM_THREADS; i++)
	{
		pthread_create(&threads[i], NULL, perform, (void *)w);
		usleep(100);
	}

	perform(w);
	for (i = 0; i < NUM_THREADS; i++)
		pthread_join(threads[i],NULL);

	printf("No thread sync errors!\n");
	return 0;
}

#endif