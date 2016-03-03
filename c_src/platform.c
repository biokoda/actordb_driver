#include "platform.h"

#ifndef __APPLE__
int SEM_TIMEDWAIT(sem_t s, uint32_t milis)
{
	TIME stv;
	clock_gettime(CLOCK_REALTIME, &stv);
	stv.tv_nsec += MS(milis); 
	return sem_timedwait(&s, &stv);
}
#endif
