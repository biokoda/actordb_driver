#include "platform.h"

#ifdef _WIN32

int clock_gettime(int X, struct timespec* tp)
{
    FILETIME ft;
    uint64_t nanos;
    GetSystemTimeAsFileTime(&ft);
    nanos = ((((uint64_t)ft.dwHighDateTime) << 32) | ft.dwLowDateTime) * 100;
    tp->tv_sec = nanos / 1000000000ul;
    tp->tv_nsec = nanos % 1000000000ul;
    return 1;
}
#endif

#if !defined(__APPLE__) && !defined(_WIN32)
int SEM_TIMEDWAIT(sem_t s, uint32_t milis)
{
	TIME stv;
	clock_gettime(CLOCK_REALTIME, &stv);
	stv.tv_nsec += MS(milis); 
	return sem_timedwait(&s, &stv);
}
#endif
