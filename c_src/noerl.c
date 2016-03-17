// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
// #define _TESTDBG_ 1
#ifdef __linux__
#define _GNU_SOURCE 1
#include <sys/mman.h>
#include <dlfcn.h>
#include <signal.h>
#include <pthread.h>
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
#include "lz4.h"

static __thread db_thread *g_tsd_thread;
static __thread db_connection *g_tsd_conn;
static __thread u64 g_tsd_cursync;
static __thread mdbinf *g_tsd_wmdb;
static priv_data *g_pd;
#define enif_tsd_get pthread_getspecific

static void lock_wtxn(int env);

// wal.c code has been taken out of sqlite3.c and placed in wal.c file.
// Every wal interface function is changed, but the wal-index code remains unchanged.
#include "wal.c"
#include "nullvfs.c"

#define RTHREADS 4
#define NCONS 100
static db_connection *g_cons;

static void lock_wtxn(int nEnv)
{
	g_tsd_wmdb = &g_pd->wmdb[nEnv];

	if (g_tsd_wmdb->txn == NULL)
	{
		if (open_txn(g_tsd_wmdb, 0) == NULL)
			return;
	}
	g_tsd_cursync = g_pd->syncNumbers[nEnv];
}

static void unlock_write_txn(int nEnv, char syncForce, char *commit)
{
	int i;

	if (!g_tsd_wmdb)
		return;

	++g_tsd_wmdb->batchCounter;
	if (*commit || syncForce)
	{
		if (mdb_txn_commit(g_tsd_wmdb->txn) != MDB_SUCCESS)
			mdb_txn_abort(g_tsd_wmdb->txn);
		g_tsd_wmdb->txn = NULL;
		g_tsd_wmdb->batchCounter = 0;
		
		if (syncForce)
			mdb_env_sync(g_tsd_wmdb->env,1);

		if (syncForce)
			++g_pd->syncNumbers[nEnv];
		*commit = 1;
	}
	// else
	// 	DBG("UNLOCK %u",g_tsd_wmdb->usageCount);
	g_tsd_cursync = g_pd->syncNumbers[nEnv];
	g_tsd_wmdb = NULL;
}

static void *perform(void *arg)
{
	db_thread *thr = (db_thread*)arg;
	int i,rc;
	mdbinf* mdb = &thr->mdb;

	srand((u32)pthread_self());
	open_txn(mdb, MDB_RDONLY);
	thr->resFrames = alloca((SQLITE_DEFAULT_PAGE_SIZE/thr->maxvalsize + 1)*sizeof(MDB_val));
	
	for (i = 0; i < 1000*100; i++)
	{
		int j = rand() % NCONS;

		if (i % 1000 == 0)
			printf("r %lld %d\n",(i64)pthread_self(),i);

		if (sqlite3_mutex_try(g_cons[j].db->mutex) != 0)
			continue;

		g_tsd_conn = &g_cons[j];

		rc = sqlite3_exec(g_cons[j].db,"SELECT max(id) FROM tab;",NULL,NULL,NULL);
		if (rc != SQLITE_OK)
		{
			printf("Error select");
			break;
		}

		sqlite3_mutex_leave(g_cons[j].db->mutex);

		mdb_txn_reset(thr->mdb.txn);
		rc = mdb_txn_renew(thr->mdb.txn);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(mdb->txn, mdb->cursorLog);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(mdb->txn, mdb->cursorPages);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(mdb->txn, mdb->cursorInfo);
		if (rc != MDB_SUCCESS)
			break;
	}
	mdb_cursor_close(mdb->cursorLog);
	mdb_cursor_close(mdb->cursorPages);
	mdb_cursor_close(mdb->cursorInfo);
	mdb_txn_abort(mdb->txn);
	return NULL;
}


int main(int argc, const char* argv[])
{
	g_log = stdout;
	db_thread thr;
	db_thread threads[RTHREADS];
	pthread_t tids[RTHREADS];
	priv_data pd;
	mdbinf* mdb = &thr.mdb;
	int i, rc;
	db_connection *cons;
	g_pd = &pd;
	char commit = 1;
	MDB_env *menv = NULL;
	char *lmpath = "lmdb";
	MDB_txn *txn;
	MDB_val key = {1,(void*)"?"}, data = {0,NULL};
	MDB_envinfo stat;

	sqlite3_initialize();
	sqlite3_vfs_register(sqlite3_nullvfs(), 1);

	unlink(lmpath);

	memset(threads, 0, sizeof(threads));
	memset(&thr, 0, sizeof(db_thread));
	memset(&pd, 0, sizeof(priv_data));

	pd.wmdb = calloc(1,sizeof(mdbinf));
	pd.nEnvs = 1;
	pd.nReadThreads = RTHREADS;
	pd.nWriteThreads = 1;
	pd.syncNumbers = calloc(1,sizeof(u64));
	pd.actorIndexes = calloc(1,sizeof(atomic_llong));
	atomic_init(pd.actorIndexes,0);
	g_cons = cons = calloc(NCONS, sizeof(db_connection));
	g_tsd_cursync = 0;
	g_tsd_conn    = NULL;
	g_tsd_wmdb    = NULL;
	g_tsd_thread  = &thr;

	if (mdb_env_create(&menv) != MDB_SUCCESS)
		return -1;
	if (mdb_env_set_maxdbs(menv,5) != MDB_SUCCESS)
		return -1;
	if (mdb_env_set_mapsize(menv,1024*1024*1024) != MDB_SUCCESS)
		return -1;
	// Syncs are handled from erlang.
	if (mdb_env_open(menv, lmpath, MDB_NOSUBDIR|MDB_NOTLS|MDB_NOSYNC, 0664) != MDB_SUCCESS) //MDB_NOSYNC
		return -1;
	if (mdb_txn_begin(menv, NULL, 0, &txn) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(txn, "info", MDB_INTEGERKEY | MDB_CREATE, &pd.wmdb[0].infodb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(txn, "actors", MDB_CREATE, &pd.wmdb[0].actorsdb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(txn, "log", MDB_CREATE | MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERDUP, 
			&pd.wmdb[0].logdb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(txn, "pages", MDB_CREATE | MDB_DUPSORT, &pd.wmdb[0].pagesdb) != MDB_SUCCESS)
		return -1;
	if (mdb_txn_commit(txn) != MDB_SUCCESS)
		return -1;

	pd.wmdb[0].env = menv;
	thr.nEnv = 0;
	thr.isreadonly = 0;
	thr.mdb.env = menv;
	thr.mdb.infodb = pd.wmdb[0].infodb;
	thr.mdb.actorsdb = pd.wmdb[0].actorsdb;
	thr.mdb.logdb = pd.wmdb[0].logdb;
	thr.mdb.pagesdb = pd.wmdb[0].pagesdb;
	thr.maxvalsize = mdb_env_get_maxkeysize(mdb->env);
	thr.resFrames = alloca((SQLITE_DEFAULT_PAGE_SIZE/thr.maxvalsize + 1)*sizeof(MDB_val));
	open_txn(&thr.mdb, MDB_RDONLY);

	for (i = 0; i < NCONS; i++)
	{
		char filename[256];
		char commit = 1;
		g_tsd_conn = &cons[i];
		sprintf(filename, "ac%d.db", i);

		thr.pagesChanged = 0;

		rc = sqlite3_open(filename,&(cons[i].db));
		if(rc != SQLITE_OK)
		{
			DBG("Unable to open db");
			break;
		}
		rc = sqlite3_exec(cons[i].db,"PRAGMA synchronous=0;PRAGMA journal_mode=wal;",NULL,NULL,NULL);
		if (rc != SQLITE_OK)
		{
			DBG("unable to open wal");
			break;
		}
		cons[i].wal.inProgressTerm = 1;
		cons[i].wal.inProgressEvnum = 1;
		rc = sqlite3_exec(cons[i].db,"CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT);"
			"insert into tab values (1,'aaaa');",NULL,NULL,NULL);
		if (rc != SQLITE_OK)
		{
			DBG("Cant create table");
			break;
		}
		unlock_write_txn(thr.nEnv, 0, &commit);

		mdb_txn_reset(thr.mdb.txn);

		rc = mdb_txn_renew(thr.mdb.txn);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(thr.mdb.txn, mdb->cursorLog);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(thr.mdb.txn, mdb->cursorPages);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(thr.mdb.txn, mdb->cursorInfo);
		if (rc != MDB_SUCCESS)
			break;
	}
	// mdb_cursor_close(thr.mdb.cursorLog);
	// mdb_cursor_close(thr.mdb.cursorPages);
	// mdb_cursor_close(thr.mdb.cursorInfo);
	// mdb_txn_abort(thr.mdb.txn);


	for (i = 0; i < RTHREADS; i++)
	{
		threads[i].nEnv = 0;
		threads[i].isreadonly = 0;
		threads[i].mdb.env = menv;
		threads[i].mdb.infodb = pd.wmdb[0].infodb;
		threads[i].mdb.actorsdb = pd.wmdb[0].actorsdb;
		threads[i].mdb.logdb = pd.wmdb[0].logdb;
		threads[i].mdb.pagesdb = pd.wmdb[0].pagesdb;
		threads[i].maxvalsize = mdb_env_get_maxkeysize(mdb->env);
		pthread_create(&tids[i], NULL, perform, (void *)&threads[i]);
	}

	srand((u32)pthread_self() + time(NULL));
	for (i = 0; i < 1000*200; i++)
	{
		char commit = 1;
		int j = rand() % NCONS;
		db_connection *con = &g_cons[j];
		char str[100];
		if (sqlite3_mutex_try(con->db->mutex) != 0)
		{
			i--;
			continue;
		}

		if (i % 1000 == 0)
			printf("w %d\n",i);
		g_tsd_conn = con;
		lock_wtxn(thr.nEnv);

		thr.pagesChanged = 0;

		if (con->wal.firstCompleteEvnum+10 < con->wal.lastCompleteEvnum)
		{
			// printf("CHECKPOINT? %llu %llu\n",con->wal.firstCompleteEvnum,con->wal.lastCompleteEvnum);
			if (checkpoint(&con->wal, con->wal.lastCompleteEvnum-10) != SQLITE_OK)
			{
				printf("Checkpoint failed\n");
				break;
			}
		}
		con->wal.inProgressTerm = 1;
		con->wal.inProgressEvnum = con->wal.lastCompleteEvnum+1;

		sprintf(str,"INSERT INTO tab VALUES (%d,'VALUE VALUE13456');", i);
		sqlite3_exec(con->db,str,NULL,NULL,NULL);

		sqlite3_mutex_leave(con->db->mutex);

		unlock_write_txn(thr.nEnv, 0, &commit);

		mdb_txn_reset(thr.mdb.txn);
		rc = mdb_txn_renew(thr.mdb.txn);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(thr.mdb.txn, mdb->cursorLog);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(thr.mdb.txn, mdb->cursorPages);
		if (rc != MDB_SUCCESS)
			break;
		rc = mdb_cursor_renew(thr.mdb.txn, mdb->cursorInfo);
		if (rc != MDB_SUCCESS)
			break;
	}

	unlock_write_txn(thr.nEnv, 1, &commit);


	for (i = 0; i < RTHREADS; i++)
		pthread_join(tids[i],NULL);

	return 1;
}
