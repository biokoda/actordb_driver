void reset(db_thread *thread, char *folder);
void close_conns(db_thread *thread);
void do_open(char *name, db_command *cmd, db_thread *thread);
void do_close(db_command *cmd,db_thread *thread);
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

    if (conn->staticPrepared)
    {
        for (i = 0; i < MAX_STATIC_SQLS; i++)
        {
            sqlite3_finalize(conn->staticPrepared[i]);
            conn->staticPrepared[i] = NULL;
        }
        free(conn->staticPrepared);
    }

    pActorPos = sqlite3HashFind(&thread->walHash,conn->dbpath);
    sqlite3HashInsert(&thread->walHash, conn->dbpath, NULL);
    free(pActorPos);
    free(conn->dbpath);

    rc = sqlite3_close(conn->db);
    assert(rc == SQLITE_OK);
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
    pActorPos = sqlite3HashFind(&thread->walHash,filename+thread->pathlen+1);

    if (pActorPos == NULL)
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

        cmd->conn->dbpath = malloc(MAX_ACTOR_NAME);
        memset(cmd->conn->dbpath,0,MAX_ACTOR_NAME);
        sprintf(cmd->conn->dbpath,"%s",name);

        pActorPos = malloc(sizeof(int));
        *pActorPos = i;
        sqlite3HashInsert(&thread->walHash, cmd->conn->dbpath, pActorPos);

        cmd->conn->wal_configured = SQLITE_OK == sqlite3_wal_data(cmd->conn->db,(void*)thread);
        sqlite3_exec(cmd->conn->db,"PRAGMA journal_mode=wal;",NULL,NULL,NULL);

        cmd->conn->thread = thread->index;
    }
    else
    {
        cmd->connindex = *pActorPos;
        cmd->conn = &thread->conns[cmd->connindex];
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
	int i, nresults = 0;
    char buf[1024*10];

	if (!cmd->conn->wal_configured)
        cmd->conn->wal_configured = SQLITE_OK == sqlite3_wal_data(cmd->conn->db,(void*)thread);

    // printf("Query start %s\r\n",txt);
	rc = sqlite3_prepare_v2(cmd->conn->db, txt, strlen(txt), &(statement), NULL);
	assert(rc == SQLITE_OK);

    while ((rc = sqlite3_step(statement)) == SQLITE_ROW)
    {
        nresults++;
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

    sqlite3_finalize(statement);
    if (rc != SQLITE_DONE)
    {
        printf("Query error=%d\r\n",rc);
        return rc;
    }
    // If we expected result, but query did not return anything, return error
    if (rc == SQLITE_DONE && nresults == 0 && results != NULL)
        return SQLITE_ERROR;
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
            thread->curConn = clcmd.conn = &thread->conns[i];
            // If no pages in wall, db will be closed
            do_close(&clcmd,thread);
            memset(&(thread->conns[i]),0,sizeof(db_connection));
        }
    }
}

// close all dbs, read whatever wal files are available (thus reopening dbs)
void reset(db_thread *thread, char *path)
{
	db_connection *conns = thread->conns;
	int nconns = thread->nconns;
    u32 wnum = thread->threadNum;

	// if empty thread info, this is init so create space for 100 connections
	if (conns == NULL)
	{
		nconns = 100;
		conns = malloc(nconns*sizeof(db_connection));
    	memset(conns,0,sizeof(db_connection)*nconns);
	}
    // close everything
    close_conns(thread);
    sqlite3HashClear(&thread->walHash);
    cleanup(thread);
    memset(thread,0,sizeof(db_thread));
    
    // init again
    strcpy(thread->path,path);
    thread->pathlen = strlen(thread->path);
    thread->nconns = nconns;
    thread->conns = conns;
    thread->threadNum = wnum;

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