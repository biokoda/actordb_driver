#ifndef _PLATFORM_H_
#define _PLATFORM_H_

#include <stdio.h>
#include <stdint.h>

#ifndef _WIN32
#include <pthread.h>
#endif

// #define u8 uint8_t
// #define i64 int64_t
// #define u64 uint64_t
// #define u32 uint32_t
// #define i32 int32_t

// #if ATOMIC
#ifndef _WIN32
#include <stdatomic.h>
#endif

#define SEC(X) (1000000000*X)
#define MS(X)  (1000000*X)
#define US(X)  (1000*X)
#define QSIZE 1024

#if defined(_WIN32)
#include <windows.h>

typedef struct timespec
{
	long tv_sec;
	long tv_nsec;
};
int clock_gettime(int X, struct timespec *tv);
#endif

#if defined(_WIN32)
	#define IOV WSABUF
	#define IOV_SET(IOV, DATA, LEN) IOV.buf = DATA; IOV.len = LEN
	#define IOV_SEND(RT, FD, IOV, IOVSIZE) WSASend(FD,IOV,IOVSIZE, &RT, 0, NULL, NULL)
#else
	#define IOV struct iovec
	#define IOV_SET(IOV, DATA, LEN) IOV.iov_base = DATA; IOV.iov_len = LEN
	#define IOV_SEND(RT, FD, IOV, IOVSIZE) RT = writev(FD,IOV,IOVSIZE)
#endif

#if defined(__APPLE__)
	#include <mach/mach_time.h>
	#include <libkern/OSAtomic.h>
	#define MemoryBarrier OSMemoryBarrier
	#include <dispatch/dispatch.h>
	#define SEMAPHORE dispatch_semaphore_t
	#define TIME uint64_t
	#define SEM_INIT_SET(X) (X = dispatch_semaphore_create(1)) == NULL
	#define SEM_INIT(X) (X = dispatch_semaphore_create(0)) == NULL
	#define SEM_WAIT(X) dispatch_semaphore_wait(X, DISPATCH_TIME_FOREVER)
	#define SEM_TIMEDWAIT(X,T) dispatch_semaphore_wait(X, dispatch_time(DISPATCH_TIME_NOW, (uint64_t)MS(T)))
	#define SEM_POST(X) dispatch_semaphore_signal(X)
	#define SEM_DESTROY(X) dispatch_release(X)
	#define GETTIME(X) X = mach_absolute_time()
	#define INITTIME mach_timebase_info_data_t timeinfo; mach_timebase_info(&timeinfo)
	#define NANODIFF(STOP,START,DIFF)	DIFF = ((STOP-START)*timeinfo.numer)/timeinfo.denom
#elif defined(_WIN32)
	#define _Atomic(X) X volatile
	#define SEMAPHORE HANDLE
	#define TIME struct timespec
	#define memory_order_relaxed 1
	#define sched_yield SwitchToThread
	#define atomic_exchange InterlockedExchangePointer
	#define atomic_store(X,V) *X = V
	#define atomic_init(X,V) *X = V
	#define atomic_fetch_add InterlockedAdd64
	#define atomic_fetch_sub InterlockedAdd64
	#define atomic_fetch_add_explicit(X,Y,Z) InterlockedAdd64(X,Z)
	#define atomic_fetch_sub_explicit(X,Y,Z) InterlockedAdd64(X,Z) 
	#define atomic_load_explicit(X,Z) *X 
	#define SEM_INIT_SET(X) (X = CreateSemaphore(NULL,1,1,NULL)) == NULL
	#define SEM_INIT(X) (X = CreateSemaphore(NULL,0,1,NULL)) == NULL
	#define SEM_WAIT(X) WaitForSingleObject(X, INFINITE)
	#define SEM_TIMEDWAIT(X,T) (WaitForSingleObject(X,T) == WAIT_TIMEOUT)
	#define SEM_POST(X) ReleaseSemaphore(X,1,NULL)
	#define SEM_DESTROY(X) CloseHandle(X)
	#define GETTIME(X) clock_gettime(1,&X)
	#define INITTIME 
	#define NANODIFF(STOP,START,DIFF) \
	 DIFF = ((STOP.tv_sec * 1000000000UL) + STOP.tv_nsec) - \
	 ((START.tv_sec * 1000000000UL) + START.tv_nsec)
#else
	// #define _POSIX_C_SOURCE 199309L
	#include <semaphore.h>
	#include <time.h>
	#define SEMAPHORE sem_t
	#define TIME struct timespec
	#define SEM_INIT_SET(X) sem_init(&X, 0, 1) != 0
	#define SEM_INIT(X) sem_init(&X, 0, 0) != 0
	#define SEM_WAIT(X) sem_wait(&X)
	int SEM_TIMEDWAIT(sem_t s, uint32_t time);
	#define SEM_POST(X) sem_post(&X)
	#define SEM_DESTROY(X) sem_destroy(&X)
	#define GETTIME(X) clock_gettime(CLOCK_MONOTONIC, &X)
	#define INITTIME
	#define NANODIFF(STOP,START,DIFF) \
	 DIFF = ((STOP.tv_sec * 1000000000UL) + STOP.tv_nsec) - \
	 ((START.tv_sec * 1000000000UL) + START.tv_nsec)
#endif

#endif