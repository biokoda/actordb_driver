// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
#define _TESTAPP_ 1
#ifdef __linux__
#define _GNU_SOURCE 1
#include <sys/mman.h>
#include <dlfcn.h>
#endif

#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdlib.h>

#ifndef  _WIN32
#include <sys/time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif


// Directly include sqlite3.c
// This way we are sure the included version of sqlite3 is actually used.
// If we were to just include "sqlite3.h" OSX would actually use /usr/lib/libsqlite3.dylib
#define SQLITE_API static 
#define SQLITE_EXTERN static 
#include "sqlite3.c" 


#include "actordb_driver_nif.h"

// wal.c code has been taken out of sqlite3.c and placed in wal.c file.
// Every wal interface function is changed, but the wal-index code remains unchanged.
#include "wal.c"
#include "tool_do.c"


void do_print(db_thread *thread)
{
    wal_file *wal = thread->walFile;
    wal_file *tmpWal;
    int rc;
    i64 nSize = 0;
    u32 iOffset;
    u8 buf[1024*10];
    char filename[512];
    u64 curEvnum, curTerm;
    u32 threadWriteNum;
    u32 prevFrameOffset;
    u32 pgno;
    u32 nTruncate;
    u32 actorIndex;
    const int szFrame = SQLITE_DEFAULT_PAGE_SIZE+WAL_FRAME_HDRSIZE;
    u32 aSalt[2];

    memset(buf,0,sizeof(buf));
    memset(filename,0,sizeof(filename));

    aSalt[0] = 123456789;
    aSalt[1] = 987654321;

    while (wal->prev)
        wal = wal->prev;


    while (1)
    {
        rc = sqlite3OsFileSize(wal->pWalFd, &nSize);
        for(iOffset = WAL_HDRSIZE; (iOffset+szFrame)<=nSize; iOffset+=szFrame)
        {
            rc = sqlite3OsRead(wal->pWalFd, buf, WAL_FRAME_HDRSIZE+SQLITE_DEFAULT_PAGE_SIZE, iOffset);
            if( rc!=SQLITE_OK )
            {
                printf("Error reading file %d\n",rc);
                return;
            }
            rc = walDecodeFrame(aSalt,SQLITE_BIGENDIAN, &pgno, &nTruncate,filename,&actorIndex, 
                            &curEvnum,&curTerm,&threadWriteNum,&prevFrameOffset, buf+WAL_FRAME_HDRSIZE, buf);
            if(!rc)
            {
                printf("Error (%d) decoding frame at=%d\n",rc,iOffset);
                continue;
            }
            if (!pgno)
            {
                printf("Zeroed out frame\n");
                continue;
            }
            printf("%u Frame for %s evnum=%llu evterm=%llu dbpgno=%u threadwnum=%u commit=%u actorindex=%u\n",
                iOffset,filename,curEvnum,curTerm,pgno,threadWriteNum,nTruncate,actorIndex);
        }

        if (thread->walFile == wal)
            break;
        // Move to next wal. Start with first and move back until previous wal is current one.
        tmpWal = wal;
        while (tmpWal->prev != wal)
            tmpWal = tmpWal->prev;
    }
}


int main(int argc, char* argv[])
{
	db_thread thread;
	db_command clcmd;

	memset(&clcmd,0,sizeof(db_command));
    memset(&thread,0,sizeof(thread));

    if (argc != 3)
    {
        printf("Call: tool print /path/to/wal/folder\n");
        return 1;
    }

	// INIT THREAD
    reset(&thread, argv[2]);
	if (strcmp(argv[1],"print") == 0)
    {
        do_print(&thread);
    }

    
    close_conns(&thread);
    free(thread.conns);
    cleanup(&thread);

	return 1;
}
