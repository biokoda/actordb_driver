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

int do_exec(char *txt,db_command *cmd, db_thread *thread, char *results[]);
int do_exec1(char *txt,db_command *cmd, db_thread *thread, char *results[], char print);
void cleanup(db_thread *thread);

void do_close(db_command *cmd,db_thread *thread)
{
    int rc;
    int *pActorPos;
    db_connection *conn = cmd->conn;
    int i = 0;
    // DB no longer open in erlang code.
    conn->nErlOpen--;

    if (conn->prepared != NULL)
    {
        for (i = 0; i < MAX_PREP_SQLS; i++)
        {
            if (conn->prepared[i] != 0)
            {
                sqlite3_finalize(conn->prepared[i]);
            }
                
            conn->prepared[i] = NULL;
        }
        free(conn->prepVersions);
        conn->prepVersions = NULL;
    }
    free(conn->prepared);
    conn->prepared = NULL;
    for (i = 0; i < MAX_STATIC_SQLS; i++)
    {
        sqlite3_finalize(conn->staticPrepared[i]);
        conn->staticPrepared[i] = NULL;
    }

    pActorPos = sqlite3HashFind(&thread->walHash,conn->dbpath);
    sqlite3HashInsert(&thread->walHash, conn->dbpath, NULL);
    free(pActorPos);

    rc = sqlite3_close(conn->db);
    if(rc != SQLITE_OK)
    {
        
    }
    // sqlite3HashClear(&cmd->conn->walPages);

    memset(conn,0,sizeof(db_connection));
}

void do_open(char *name, db_command *cmd, db_thread *thread) 
{
	char filename[MAX_PATHNAME];
    unsigned int size;
    int rc;
    int i;
    int *pActorPos;

	memset(filename,0,MAX_PATHNAME);
    memcpy(filename, thread->path, thread->pathlen);
    filename[thread->pathlen] = '/';

    sprintf(filename+thread->pathlen+1,"%s",name);
    cmd->conn = sqlite3HashFind(&thread->walHash,filename+thread->pathlen+1);

    if (cmd->conn == NULL)
    {
        for (i = 0; i < thread->nconns; i++)
        {
            if (!thread->conns[i].db)
                break;
        }
        
        printf("OPEN DB INDEX %d\r\n",i);fflush(stdout);
        cmd->conn = &thread->conns[i];
        cmd->connindex = i;

        printf("SQLITE OPEN %s, i %d, nconns %d\r\n",filename, i, thread->nconns);fflush(stdout);
        rc = sqlite3_open(filename,&(cmd->conn->db));
        if(rc != SQLITE_OK) 
        {
            printf("CAN NOT OPEN %s\r\n",filename);fflush(stdout);
            sqlite3_close(cmd->conn->db);
            cmd->conn = NULL;
            return;
        }
        size = strlen(name);

        memset(cmd->conn->dbpath,0,MAX_ACTOR_NAME);
        sprintf(cmd->conn->dbpath,"%s",name);

        pActorPos = malloc(sizeof(int));
        *pActorPos = i;
        sqlite3HashInsert(&thread->walHash, cmd->conn->dbpath, pActorPos);

        cmd->conn->thread = thread->index;
    }
    else
    {
        cmd->connindex = cmd->conn - thread->conns;
    }
    
    cmd->conn->nErlOpen++;
    cmd->conn->connindex = cmd->connindex;
}

int do_exec(char *txt,db_command *cmd, db_thread *thread, char *results[])
{
    return do_exec1(txt,cmd, thread, results,0);
}
int do_exec1(char *txt,db_command *cmd, db_thread *thread, char *results[], char print)
{
	sqlite3_stmt *statement = NULL;
	int rc = SQLITE_OK;
	int i;
    char buf[1024*10];

	if (!cmd->conn->wal_configured)
        cmd->conn->wal_configured = SQLITE_OK == sqlite3_wal_data(cmd->conn->db,(void*)thread);

    // printf("Query start %s\r\n",txt);
	rc = sqlite3_prepare_v2(cmd->conn->db, txt, strlen(txt), &(statement), NULL);
	assert(rc == SQLITE_OK);

    while ((rc = sqlite3_step(statement)) == SQLITE_ROW)
    {
    	for (i = 0; i < sqlite3_column_count(statement); i++)
    	{
            int type = sqlite3_column_type(statement, i);
    		if (print && i == 0)
    			printf("Row: ");
    		switch(type) {
		    case SQLITE_INTEGER:
			    print ? printf(" %d ",sqlite3_column_int(statement, i)) : 0;
                sprintf(buf,"%d",sqlite3_column_int(statement, i));
			    break;
		    // case SQLITE_FLOAT:
			   //  printf(" %f ",(float)sqlite3_column_double(statement, i));
			   //  break;
		    // case SQLITE_BLOB:
		    //     printf(" blob ");
		    //     break;
		    case SQLITE_NULL:
			    print ? printf(" null ") : 0;
                sprintf(buf,"null");
			    break;
		    case SQLITE_TEXT:
			    print ? printf(" \"%s\" ", sqlite3_column_text(statement, i)) : 0;
                snprintf(buf,1024*10-1,"%s",sqlite3_column_text(statement, i));
			    break;
		    }

            if (results != NULL)
            {
                // printf("Select matches shouldbe=%s is=%s\r\n",results[i],buf);
                assert(strcmp(results[i],buf) == 0);
            }
    	}
    	print ? printf("\r\n") : 0;
    }
    
    // else
    // 	printf("Query DONE OK\r\n");

    sqlite3_finalize(statement);
    if (rc != SQLITE_DONE)
    {
        printf("Query error=%d\r\n",rc);
        return rc;
    }
    return SQLITE_OK;
}

void close_conns(db_thread *thread)
{
	int i;
	db_command clcmd;
	memset(&clcmd,0,sizeof(db_command));

	for (i = 0; i < thread->nconns; i++)
    {
        if (thread->conns[i].db != NULL)
        {
            clcmd.conn = &thread->conns[i];
            // If no pages in wall, db will be closed
            do_close(&clcmd,thread);
        }
    }
}

// close all dbs, read whatever wal files are available (thus reopening dbs)
void reset(db_thread *thread, db_connection *conns)
{
    // close everything
    close_conns(thread);
    sqlite3HashClear(&thread->walHash);
    cleanup(thread);
    memset(thread,0,sizeof(db_thread));
    // init again
    sprintf(thread->path,".");
    thread->pathlen = strlen(thread->path);
    thread->nconns = 100;
    thread->conns = conns;

    // read wal
    read_thread_wal(thread);
}

void cleanup(db_thread *thread)
{
    wal_file *wFile;
    while (thread->walFile != NULL)
    {
        wFile = thread->walFile;
        thread->walFile = thread->walFile->prev;
        sqlite3OsCloseFree(wFile->pWalFd);
        free(wFile->filename);
        free(wFile);
    }
}

void check_large(db_thread *thread, db_command *clcmd, char *buf, char* buf1)
{
    int i,j,rc;
    for (i = 0; i < 3; i++)
    {
        thread->curConn = clcmd->conn = &thread->conns[i];
        for (j = 0; j < 1000; j++)
        {
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
    db_connection *conns;
    Wal *pWal;
	int i = 0,j = 0;
    int rc;
    int ndbs = 3;
    char buf[1024*10];
    char buf1[1024*10];
    char pgBuf[4096+WAL_FRAME_HDRSIZE];
    char pgDone = 0, pgLast = 0;
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

	sqlite3HashInit(&thread.walHash);
	conns = thread.conns = malloc(100*sizeof(db_connection));
    memset(conns,0,sizeof(db_connection)*100);
	thread.nconns = 100;
	read_thread_wal(&thread);

	// OPEN DBS and insert
    for (i = 0; i < ndbs; i++)
    {
        do_open(dbnames[i],&clcmd,&thread);
        thread.curConn = clcmd.conn = &thread.conns[i];
        do_exec("PRAGMA journal_mode=wal;",&clcmd,&thread,NULL);
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

	reset(&thread,conns);

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
    sprintf(buf,"INSERT INTO tab VALUES (%s,'%s');",initvals[3][0],initvals[3][1]);
    rc = do_exec(buf,&clcmd,&thread,initvals[3]);
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
    for (i = 0; i < 1000; i++)
    {
        for (j = 0; j < ndbs; j++)
        {
            thread.curConn = clcmd.conn = &thread.conns[j];
            sprintf(buf,"insert into tab values (%d,'%s');",i+10,buf1);
            rc = do_exec(buf,&clcmd,&thread,NULL); 
            assert(SQLITE_OK == rc);
        }
    }

    for (i = 0;; i++)
    {
        iterate_wal(&thread.conns[0],4096+WAL_FRAME_HDRSIZE,pgBuf,&pgDone,&pgLast);
        if(pgDone)
            break;
    }
    printf("Reset\r\n");
    // Close everything
    reset(&thread,conns);
    printf("Check data\r\n");
    // Check if all still there
    check_large(&thread, &clcmd, buf, buf1);
    

    printf("Checkpointing\r\n");
    while (checkpoint_continue(&thread))
    {
    }

    // we are now left with the last wal file. Close everything and reopen.
    // All the data must still be there
    reset(&thread,conns);

    printf("Verifying data\r\n");
    check_large(&thread, &clcmd, buf, buf1);
    printf("Tests succeeded\r\n");

    close_conns(&thread);
    free(thread.conns);
    cleanup(&thread);

	return 1;
}
