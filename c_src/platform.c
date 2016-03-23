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
#include <errno.h>
int SEM_TIMEDWAIT(sem_t s, uint32_t milis)
{
    struct timespec ts;
    struct timespec dts;
    struct timespec sts;
    int r;

    if (clock_gettime(CLOCK_REALTIME, &ts) == -1)
        return -1;

    dts.tv_sec = milis / 1000;
    dts.tv_nsec = (milis % 1000) * 1000000;
    sts.tv_sec = ts.tv_sec + dts.tv_sec + (dts.tv_nsec + ts.tv_nsec) / 1000000000;
    sts.tv_nsec = (dts.tv_nsec + ts.tv_nsec) % 1000000000;

    while ((r = sem_timedwait(&s, &sts)) == -1 && errno == EINTR)
        continue;
    return r;
}
#endif
