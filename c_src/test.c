// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
#define _TESTAPP_ 1
#define _TESTDBG_ 1
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

#define NINSERTS 100


void check_large(db_thread *thread, db_command *clcmd, char *buf, char* buf1)
{
    int i,j,rc;
    for (i = 0; thread->conns[i].db != NULL; i++)
    {
        thread->curConn = clcmd->conn = &thread->conns[i];
        for (j = 0; j < NINSERTS; j++)
        {
            // printf("check_large %d %d\n",i,j);
            char str[10];
            char *res[] = {str,buf1};
            
            sprintf(str,"%d",j+10);
            sprintf(buf,"select * from tab where id=%d;",j+10);
            rc = do_exec(buf,clcmd,thread,res); 
            assert(SQLITE_OK == rc);
        }
    }
}


int main()
{
	db_thread thread;
	db_command clcmd;
    Wal *pWal;
	int i = 0,j = 0;
    int rc;
    int ndbs = 3;
    char buf[1024*10];
    char buf1[1024*10];
    char pgBuf[4096+WAL_FRAME_HDRSIZE];
    char pgDone[2] = {0,0}, pgLast = 0;
    iterate_resource iter[2];
    u32 wnum;

    g_wal_size_limit = 100;
    g_log = stdout;

    memset(&iter,0,sizeof(iter));
    memset(buf,0,sizeof(buf));
    memset(buf1,0,sizeof(buf1));

    char* dbnames[] = {"my1.db","my2.db","my3.db"};
    char* initvals[4][2] = {{"1","db1 text"},
                            {"1","db2 text"},
                            {"1","db3 text"},
                            {"2","db1 second"}};
	
	memset(&thread,0,sizeof(db_thread));
	memset(&clcmd,0,sizeof(db_command));

	// INIT THREAD
	sprintf(thread.path,".");
	thread.pathlen = strlen(thread.path);

	thread.conns = malloc(100*sizeof(db_connection));
    memset(thread.conns,0,sizeof(db_connection)*100);
	thread.nconns = 100;
	read_thread_wal(&thread);

	// OPEN DBS and insert
    for (i = 0; i < ndbs; i++)
    {
        do_open(dbnames[i],&clcmd,&thread);
        thread.curConn = clcmd.conn = &thread.conns[i];
        thread.threadNum++;
        do_exec("CREATE TABLE tab (id INTEGER PRIMARY KEY, val TEXT);",&clcmd,&thread,NULL);
        sprintf(buf,"INSERT INTO tab VALUES (%s,'%s');",initvals[i][0],initvals[i][1]);
        do_exec(buf,&clcmd,&thread,NULL);
    }
	
    // Read
	for (i = 0; i < ndbs; i++)
    {
        thread.curConn = clcmd.conn = &thread.conns[i];
        do_exec("SELECT * from tab;",&clcmd,&thread,initvals[i]);
    }

	reset(&thread,".");

    printf("STARTING READ\n");

    // read written data, will assert if not correct
    for (i = 0; i < ndbs; i++)
    {
        thread.curConn = clcmd.conn = &thread.conns[i];
        rc = do_exec("SELECT * from tab;",&clcmd,&thread,initvals[i]);
        assert(rc == SQLITE_OK);
    }

    printf("TRY FAILED SAVEPOINT\n");
    // start savepoint
    thread.curConn = clcmd.conn = &thread.conns[0];
    thread.threadNum++;
    rc = do_exec("SAVEPOINT 'adb';",&clcmd,&thread,NULL);
    assert(SQLITE_OK == rc);
    
    memset(buf1,'a',4096*2);
    // write more than a pages worth of valid insert (which should force some data to disk)
    for (i = 2; i < 1000; i++)
    {
        sprintf(buf,"insert into tab values (%d,'%s');",i+10,buf1);
        rc = do_exec(buf,&clcmd,&thread,NULL); 
        assert(SQLITE_OK == rc);
    }
    
    // Now insert on an existing id which must fail
    sprintf(buf,"insert into tab values (1,'asdsf');");
    rc = do_exec(buf,&clcmd,&thread,NULL);
    // we have error
    assert(SQLITE_CONSTRAINT == rc);
    printf("Calling rollback\r\n");
    // rollback transaction
    rc = do_exec("ROLLBACK;",&clcmd,&thread,NULL);
    assert(SQLITE_OK == rc);

    // write to id 2, which must succeed
    thread.threadNum++;
    sprintf(buf,"INSERT INTO tab VALUES (%s,'%s');",initvals[3][0],initvals[3][1]);
    rc = do_exec(buf,&clcmd,&thread,NULL);
    assert(SQLITE_OK == rc);
    sprintf(buf,"SELECT * from tab where id=%s;",initvals[3][0]);
    rc = do_exec(buf,&clcmd,&thread,initvals[3]);
    assert(SQLITE_OK == rc);

    // read written data, will assert if not correct
    for (i = 0; i < ndbs; i++)
    {
        thread.curConn = clcmd.conn = &thread.conns[i];
        rc = do_exec("SELECT * from tab where id=1;",&clcmd,&thread,initvals[i]);
        assert(SQLITE_OK == rc);
    }

    memset(buf1,'b',4096*2);
    // Test checkpoints
    printf("Insert multiple wal file amount of data\r\n");
    // insert a lot of data
    for (i = 0; i < NINSERTS; i++)
    {
        for (j = 0; j < ndbs; j++)
        {
            thread.threadNum++;
            thread.curConn = clcmd.conn = &thread.conns[j];
            clcmd.conn->writeNumber = clcmd.conn->writeTermNumber = i;
            sprintf(buf,"insert into tab values (%d,'%s');",i+10,buf1);
            rc = do_exec(buf,&clcmd,&thread,NULL); 
            assert(SQLITE_OK == rc);
        }
    }
    
    do_open("copy1.db",&clcmd,&thread);
    thread.curConn = clcmd.conn = &thread.conns[3];
    rc = do_exec("select name, sql from sqlite_master where type='table';$PRAGMA cache_size=10;",&clcmd,&thread,NULL); 
    do_open("copy2.db",&clcmd,&thread);
    thread.curConn = clcmd.conn = &thread.conns[4];
    rc = do_exec("select name, sql from sqlite_master where type='table';$PRAGMA cache_size=10;",&clcmd,&thread,NULL); 

    for (i = 0; pgDone[0]+pgDone[1] < 2 ; i++)
    {
        PgHdr page;
        u32 commit;
        u32 wnum1;

        memset(&page,0,sizeof(PgHdr));

        // wal_iterate(&thread.conns[0],4096+WAL_FRAME_HDRSIZE,pgBuf,&pgDone,&pgLast);

        for (j = 3; j < 5; j++)
        {
            if (pgDone[j-3])
                continue;
            printf("ITERATE %d %d\n",i,j);fflush(stdout);
            if (wal_iterate_from(&thread.conns[j-3], &iter[j-3], 4096+WAL_FRAME_HDRSIZE, (u8*)pgBuf, &rc, &pgLast) == SQLITE_DONE)
            {
                pgDone[j-3] = 1;
                continue;
            }

            page.pData = pgBuf + WAL_FRAME_HDRSIZE;
            page.pgno = sqlite3Get4byte((u8*)pgBuf);
            commit = sqlite3Get4byte((u8*)&pgBuf[4]);

            wnum = thread.threadNum;
            thread.curConn = clcmd.conn = &thread.conns[j];
            thread.curConn->writeNumber = readUInt64((u8*)&pgBuf[8]);
            thread.curConn->writeTermNumber = readUInt64((u8*)&pgBuf[16]);
            wnum1 = sqlite3Get4byte((u8*)&pgBuf[28]);

            Btree *pBt = thread.curConn->db->aDb[0].pBt;
            assert(pBt != NULL);
            Pager *pPager = sqlite3BtreePager(pBt);
            assert(pPager->pWal != NULL);

            if (!thread.curConn->wal->writeLock)
            {
                sqlite3WalBeginReadTransaction(thread.curConn->wal, &rc);
                sqlite3WalBeginWriteTransaction(thread.curConn->wal);
            }

            if (wnum1 != thread.curConn->lastWriteThreadNum && thread.curConn->wal->dirty)
            {
                printf("Calling undo for uncommited write\n");
                pagerRollbackWal(pPager);
            }
            
            thread.threadNum = wnum1;
            
            page.pPager = pPager;
            rc = pagerWalFrames(pPager,&page,commit,commit);
            pPager->pWal->init = 0;
            thread.threadNum = wnum;
        }
    }
    for (i = 3; j < 5; j++)
    {
        thread.curConn = clcmd.conn = &thread.conns[j];
        sqlite3WalEndReadTransaction(thread.curConn->wal);
        sqlite3WalEndWriteTransaction(thread.curConn->wal);
    }
    check_large(&thread, &clcmd, buf, buf1);

    printf("Reset\r\n");
    // Close everything
    reset(&thread,".");
    printf("Check data\r\n");
    // Check if all still there
    check_large(&thread, &clcmd, buf, buf1);
    

    printf("Checkpointing\r\n");
    while (checkpoint_continue(&thread))
    {
    }

    // we are now left with the last 2 wal files. Close everything and reopen.
    // All the data must still be there
    reset(&thread,".");

    printf("Verifying data\r\n");
    check_large(&thread, &clcmd, buf, buf1);
    
    printf("Do rewind on first db\r\n");
    thread.curConn = clcmd.conn = &thread.conns[3];
    printf("REWIND result=%d\r\n",wal_rewind(clcmd.conn,NINSERTS-5));

    for (j = 0; j < 1000; j++)
    {
        char str[10];
        char *res[] = {str,buf1};
        
        sprintf(str,"%d",j+10);
        sprintf(buf,"select id from tab where id=%d;",j+10);
        rc = do_exec1(buf,&clcmd,&thread,res,0); 
        if (j >= NINSERTS-5)
        {
            // must return error, because we supplied result to check against but none was returned
            assert(SQLITE_OK != rc);
        }
        else
            assert(SQLITE_OK == rc);
    }

    printf("Tests succeeded\r\n");
    close_conns(&thread);
    free(thread.conns);
    cleanup(&thread);

	return 1;
}
