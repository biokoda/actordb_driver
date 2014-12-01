// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
#define _TESTAPP_ 1
// #define _TESTDBG_ 1
#ifdef __linux__
#define _GNU_SOURCE 1
#include <sys/mman.h>
#include <dlfcn.h>
#endif

#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>

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

void do_close(db_command *cmd,db_thread *thread)
{
    int rc;
    db_connection *conn = cmd->conn;
    int i = 0;
    // DB no longer open in erlang code.
    conn->nErlOpen--;

    // Only close if no pages in wal.
    if (!conn->nPages && conn->nErlOpen <= 0)
    {
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

        sqlite3HashInsert(&thread->walHash, conn->dbpath, NULL);

        rc = sqlite3_close(conn->db);
        if(rc != SQLITE_OK)
        {
            
        }
        // sqlite3HashClear(&cmd->conn->walPages);

        memset(conn,0,sizeof(db_connection));
    }
}

void do_open(char *name, db_command *cmd, db_thread *thread) 
{
	char filename[MAX_PATHNAME];
    unsigned int size;
    int rc;
    int i;

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
        
        printf("INDEX %d\r\n",i);fflush(stdout);
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

        memset(cmd->conn->dbpath,0,100);
        sprintf(cmd->conn->dbpath,"%s",name);

        sqlite3HashInsert(&thread->walHash, cmd->conn->dbpath, cmd->conn);

        cmd->conn->nPages = cmd->conn->nPrevPages = 0;
        cmd->conn->thread = thread->index;
    }
    else
    {
        cmd->connindex = cmd->conn - thread->conns;
    }
    
    cmd->conn->nErlOpen++;
    cmd->conn->connindex = cmd->connindex;
}


void do_exec(char *txt,db_command *cmd, db_thread *thread, char *results[])
{
	sqlite3_stmt *statement = NULL;
	int rc;
	int i;
    char buf[1024];

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
    		// if (i == 0)
    		// 	printf("Row: ");
    		switch(type) {
		    case SQLITE_INTEGER:
			    // printf(" %d ",sqlite3_column_int(statement, i));
                sprintf(buf,"%d",sqlite3_column_int(statement, i));
			    break;
		    // case SQLITE_FLOAT:
			   //  printf(" %f ",(float)sqlite3_column_double(statement, i));
			   //  break;
		    // case SQLITE_BLOB:
		    //     printf(" blob ");
		    //     break;
		    case SQLITE_NULL:
			    // printf(" null ");
                sprintf(buf,"null");
			    break;
		    case SQLITE_TEXT:
			    // printf(" \"%s\" ", sqlite3_column_text(statement, i));
                sprintf(buf,"'%s'",sqlite3_column_text(statement, i));
			    break;
		    }

            if (results != NULL)
            {
                assert(strcmp(results[i],buf) != 0);
                // printf("Select does not match shouldbe=%s is=%s\r\n",results[i],buf);
            }
    	}
    	// printf("\r\n");
    }

    if (rc != SQLITE_DONE)
    {
    	printf("Query error=%d\r\n",rc);
    }
    // else
    // 	printf("Query DONE OK\r\n");

    sqlite3_finalize(statement);

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
            thread->conns[i].nPages = 0;
            do_close(&clcmd,thread);
        }
    }
}

int main()
{
	db_thread thread;
	db_command clcmd;
    db_connection *conns;
	int i = 0;
    int ndbs = 3;
    char buf[1024];

    char* dbnames[] = {"my1.db","my2.db","my3.db"};
    char* initvals[3][2] = {{"1","'db1 text'"},
                            {"1","'db2 text'"},
                            {"1","'db3 text'"}};
	
    for (i = 0; i < ndbs; i++)
        remove(dbnames[i]);
	
    remove("wal.0");remove("wal.1");remove("wal.2");
	memset(&thread,0,sizeof(db_thread));
	memset(&clcmd,0,sizeof(db_command));

	// INIT THREAD
	sprintf(thread.path,".");
	thread.pathlen = strlen(thread.path);

	sqlite3HashInit(&thread.walHash);
	conns = thread.conns = malloc(100*sizeof(db_connection));
	thread.nconns = 100;
	read_thread_wal(&thread);

	// OPEN DBS
    for (i = 0; i < ndbs; i++)
    {
        do_open(dbnames[i],&clcmd,&thread);
        thread.curConn = clcmd.conn = &thread.conns[i];
        do_exec("PRAGMA journal_mode=wal;",&clcmd,&thread,NULL);
        do_exec("CREATE TABLE tab (id INTEGER PRIMARY KEY, val TEXT);",&clcmd,&thread,NULL);
        sprintf(buf,"INSERT INTO tab VALUES (%s,%s);",initvals[i][0],initvals[i][1]);
        do_exec(buf,&clcmd,&thread,NULL);
    }
	
    // Read
	for (i = 0; i < ndbs; i++)
    {
        thread.curConn = clcmd.conn = &thread.conns[i];
        do_exec("SELECT * from tab;",&clcmd,&thread,initvals[i]);
    }

	// close everything
	close_conns(&thread);
    sqlite3HashClear(&thread.walHash);
    memset(&thread,0,sizeof(db_thread));
    // init again
    sprintf(thread.path,".");
    thread.pathlen = strlen(thread.path);
    thread.nconns = 100;
    thread.conns = conns;

    printf("STARTING READ\n");

    // read wal
    read_thread_wal(&thread);

    // read written data
    for (i = 0; i < ndbs; i++)
    {
        thread.curConn = clcmd.conn = &thread.conns[i];
        do_exec("SELECT * from tab;",&clcmd,&thread,initvals[i]);
    }

    close_conns(&thread);
    free(thread.conns);

	return 1;
}
