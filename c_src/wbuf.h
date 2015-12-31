#ifndef _WBUF_H_
#define _WBUF_H_

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
// #include "erl_nif.h"


char* wbuf_init(const int npages);
int  wbuf_put(const int npages, char *buf, char *data, int *tries);
char* wbuf_get(const int npages, char *buf, int index);
void wbuf_release(char *buf, int index);

#endif
