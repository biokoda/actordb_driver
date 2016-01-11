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

// info|pages|log|actors
#define PRINT_PAGES  1
#define PRINT_INFO   2
#define PRINT_LOG    4
#define PRINT_ACTORS 8

// Directly include sqlite3.c
// This way we are sure the included version of sqlite3 is actually used.
// If we were to just include "sqlite3.h" OSX would actually use /usr/lib/libsqlite3.dylib
#define SQLITE_API static
#define SQLITE_EXTERN static
#include "sqlite3.c"


#include "actordb_driver_nif.h"
#include "lz4.h"

static __thread db_thread *g_tsd_thread;
static __thread priv_data *g_tsd_pd;
static __thread db_connection *g_tsd_conn;
static __thread mdbinf *g_tsd_wmdb;
priv_data *g_pd;
#define enif_tsd_get pthread_getspecific

static void lock_wtxn(int env){}

// wal.c code has been taken out of sqlite3.c and placed in wal.c file.
// Every wal interface function is changed, but the wal-index code remains unchanged.
#include "wal.c"


typedef struct lmdb
{
	MDB_env *menv;
	MDB_txn *txn;
	MDB_dbi infodb;
	MDB_dbi logdb;
	MDB_dbi pagesdb;
	MDB_dbi actorsdb;
	MDB_cursor *cursorLog;
	MDB_cursor *cursorPages;
	MDB_cursor *cursorInfo;
	MDB_cursor *cursorActors;
}lmdb;


static int open_env(lmdb *lm, const char *pth, int flags)
{
	// #if defined(__APPLE__) || defined(_WIN32)
	// u64 dbsize = 4096*1024*1024LL;
	// #else
	// 	// 1TB def size on linux
	// 	u64 dbsize = 4096*1024*1024*128*2LL;
	// #endif
	int rc;

	if (mdb_env_create(&lm->menv) != MDB_SUCCESS)
		return -1;
	if (mdb_env_set_maxdbs(lm->menv,5) != MDB_SUCCESS)
		return -1;
	// if (mdb_env_set_mapsize(lm->menv,dbsize) != MDB_SUCCESS)
	// 	return -1;
	if (mdb_env_open(lm->menv, pth, MDB_NOSUBDIR | flags, 0664) != MDB_SUCCESS)
		return -1;

	if (mdb_txn_begin(lm->menv, NULL, flags, &lm->txn) != MDB_SUCCESS)
		return -1;

	if (mdb_dbi_open(lm->txn, "info", MDB_INTEGERKEY, &lm->infodb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(lm->txn, "actors", MDB_CREATE, &lm->actorsdb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(lm->txn, "log", MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERDUP, &lm->logdb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(lm->txn, "pages", MDB_DUPSORT, &lm->pagesdb) != MDB_SUCCESS)
		return -1;
	if (mdb_set_compare(lm->txn, lm->logdb, logdb_cmp) != MDB_SUCCESS)
		return -1;
	if (mdb_set_compare(lm->txn, lm->pagesdb, pagesdb_cmp) != MDB_SUCCESS)
		return -1;
	if (mdb_set_dupsort(lm->txn, lm->pagesdb, pagesdb_val_cmp) != MDB_SUCCESS)
		return -1;
	if (mdb_cursor_open(lm->txn, lm->logdb, &lm->cursorLog) != MDB_SUCCESS)
		return -1;
	if (mdb_cursor_open(lm->txn, lm->pagesdb, &lm->cursorPages) != MDB_SUCCESS)
		return -1;
	if (mdb_cursor_open(lm->txn, lm->infodb, &lm->cursorInfo) != MDB_SUCCESS)
		return -1;
	if (mdb_cursor_open(lm->txn, lm->actorsdb, &lm->cursorActors) != MDB_SUCCESS)
		return -1;

	return 0;
}

static void close_env(lmdb *lm)
{
	if (lm->txn)
		mdb_txn_commit(lm->txn);
	if (lm->menv)
		mdb_env_close(lm->menv);
}

static size_t file_size(const char *pth)
{
	size_t sz;
	FILE *file = fopen(pth,"rb");
	if (!file)
		return 0;

	fseek(file, 0L, SEEK_END);
	sz = ftell(file);
	fseek(file, 0L, SEEK_SET);
	fclose(file);

	return sz;
}


static int do_print(const char *pth, int what)
{
	struct lmdb lm;
	MDB_val key, data;
	int rc, op;

	if (open_env(&lm, pth, MDB_RDONLY) == -1)
	{
		printf("Unable to open lmdb environment\n");
		return -1;
	}
	

	if (what & PRINT_ACTORS)
	{
		printf("-----------------------actorsdb--------------------------\n");
		rc = mdb_cursor_get(lm.cursorActors,&key,&data,MDB_FIRST);
		while (rc == MDB_SUCCESS)
		{
			u64 index;
			memcpy(&index, data.mv_data, sizeof(u64));
			printf("Actor=%.*s, id=%llu\n",(int)key.mv_size, (char*)key.mv_data, index);
			rc = mdb_cursor_get(lm.cursorActors,&key,&data,MDB_NEXT);
		}
	}

	if (what & PRINT_LOG)
	{
		printf("-----------------------logdb--------------------------\n");
		rc = mdb_cursor_get(lm.cursorLog,&key,&data,MDB_FIRST);
		while (rc == MDB_SUCCESS)
		{
			u64 index, term, num;
			memcpy(&index, key.mv_data,                 sizeof(u64));
			memcpy(&term,  key.mv_data + sizeof(u64),   sizeof(u64));
			memcpy(&num,   key.mv_data + sizeof(u64)*2, sizeof(u64));
			printf("logdb: actor=%llu, term=%llu, evnum=%llu\n",index, term,num);

			op = MDB_FIRST_DUP;
			while ((rc = mdb_cursor_get(lm.cursorLog,&key,&data, op)) == MDB_SUCCESS)
			{
				u32 pgno;
				memcpy(&pgno,data.mv_data,sizeof(u32));
				printf("  pgno=%u\n",pgno);
				op = MDB_NEXT_DUP;
			}
			rc = mdb_cursor_get(lm.cursorLog,&key,&data,MDB_NEXT_NODUP);
		}
	}

	if (what & PRINT_PAGES)
	{
		printf("-----------------------pagesdb--------------------------\n");
		rc = mdb_cursor_get(lm.cursorPages,&key,&data,MDB_FIRST);
		while (rc == MDB_SUCCESS)
		{
			u64 index;
			u32 pgno;
			size_t ndupl;

			memcpy(&index, key.mv_data, sizeof(u64));
			memcpy(&pgno, key.mv_data + sizeof(u64), sizeof(u32));

			printf("pagesdb: actor=%llu, pgno=%u\n",index, pgno);

			mdb_cursor_count(lm.cursorPages,&ndupl);

			op = MDB_FIRST_DUP;
			while ((rc = mdb_cursor_get(lm.cursorPages,&key,&data, op)) == MDB_SUCCESS)
			{
				u64 term,num;
				u8 frag;
				memcpy(&term, data.mv_data,               sizeof(u64));
				memcpy(&num,  data.mv_data + sizeof(u64), sizeof(u64));
				frag = *(u8*)(data.mv_data + sizeof(u64)*2);
				printf("  evterm=%lld, evnum=%lld, frag=%d, pgsize=%ld\n",term,num,(int)frag,data.mv_size-sizeof(u64)*2-1);

				op = MDB_NEXT_DUP;
				ndupl--;
				if (ndupl == 0)
					break;
			}
			rc = mdb_cursor_get(lm.cursorPages,&key,&data,MDB_NEXT);
		}
	}
	
	if (what & PRINT_INFO)
	{
		printf("-----------------------infodb--------------------------\n");
		rc = mdb_cursor_get(lm.cursorInfo, &key, &data, MDB_FIRST);
		while (rc == MDB_SUCCESS)
		{
			u8 v;
			u64  index, fTerm, fEvnum, lTerm, lEvnum, iTerm, iEvnum;
			u32 mxPage,allPages;

			memcpy(&index, key.mv_data, sizeof(u64));
			v = *(u8*)(data.mv_data);
			memcpy(&fTerm,  data.mv_data+1,               sizeof(u64));
			memcpy(&fEvnum, data.mv_data+1+sizeof(u64),   sizeof(u64));
			memcpy(&lTerm,  data.mv_data+1+sizeof(u64)*2, sizeof(u64));
			memcpy(&lEvnum, data.mv_data+1+sizeof(u64)*3, sizeof(u64));
			memcpy(&iTerm,  data.mv_data+1+sizeof(u64)*4, sizeof(u64));
			memcpy(&iEvnum, data.mv_data+1+sizeof(u64)*5, sizeof(u64));
			memcpy(&mxPage, data.mv_data+1+sizeof(u64)*6, sizeof(u32));
			memcpy(&allPages, data.mv_data+1+sizeof(u64)*6+sizeof(u32), sizeof(u32));

			printf("actor=%llu, firstTerm=%llu, firstEvnum=%llu, lastTerm=%llu, lastEvnum=%llu,"
			"inprogTerm=%llu, inprogEvnum=%llu, mxPage=%u, allPages=%u\n",
			index,fTerm,fEvnum,lTerm,lEvnum,iTerm,iEvnum,mxPage,allPages);

			rc = mdb_cursor_get(lm.cursorInfo, &key, &data, MDB_NEXT);
		}
	}
	close_env(&lm);
	return 0;
}

// static void sighandle(int sig)
// {
// }

static int do_backup(const char *src, const char *dst)
{
	MDB_val key, data;
	lmdb rd, wr;
	int rc;

// #ifdef SIGPIPE
// 	signal(SIGPIPE, sighandle);
// #endif
// #ifdef SIGHUP
// 	signal(SIGHUP, sighandle);
// #endif
// 	signal(SIGINT, sighandle);
// 	signal(SIGTERM, sighandle);

	memset(&rd,0,sizeof(rd));
	memset(&wr,0,sizeof(wr));

	// If destination does not exist, do a simple complete copy
	if (file_size(dst) == 0)
	{
		int rc;

		if (mdb_env_create(&rd.menv) != MDB_SUCCESS)
			return -1;
		if (mdb_env_open(rd.menv, src, MDB_NOSUBDIR | MDB_RDONLY, 0600) != MDB_SUCCESS)
			return -1;

		if (strcmp(dst, "-") == 0)
			rc = mdb_env_copyfd(rd.menv, 1);
		else
			rc = mdb_env_copy2(rd.menv, dst, 0);
		if (rc != 0)
			fprintf(stderr,"Backup failed %s\n",strerror(rc));
		// goto bckp_done;
	}
	mdb_txn_commit(rd.txn);
	close_env(&rd);

	// if (open_env(&rd, src, MDB_RDONLY) == -1)
	// {
	// 	printf("Unable to open source environment\n");
	// 	return -1;
	// }
	if (open_env(&wr, dst, 0) == -1)
	{
		printf("Unable to open destination environment\n");
		return -1;
	}

	// Environment has been copied over, now delete state.
	// This means cluster configuration (nodes, schema, etc.).
	// This way this lmdb file can be used in an entirely different setup.

	rc = mdb_cursor_get(wr.cursorActors,&key,&data,MDB_FIRST);
	if (rc != MDB_SUCCESS)
		goto bckp_done;
	do 
	{
		u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
		MDB_val pgKey, pgVal, logKey, logVal;
		u32 pgno = 1;
		int pgop = MDB_SET;
		size_t nmsz = key.mv_size;
		char *nm = (char*)key.mv_data;
		u64 index,firstCompleteTerm,firstCompleteEvnum;
		u8 logKeyBuf[sizeof(u64)*3];

		// skip actor data
		if (nm[0] == '?')
			continue;
		else if (key.mv_size > 7 && strncmp(nm, "actors/",7) == 0)
			continue;
		else if (key.mv_size > 7 && strncmp(nm, "shards/",7) == 0)
			continue;

		memcpy(&index, data.mv_data, sizeof(u64));
		// printf("Check key %llu\n",index);
		key.mv_size = sizeof(index);
		key.mv_data = &index;
		rc = mdb_get(wr.txn,wr.infodb,&key,&data);
		if (rc != MDB_SUCCESS)
		{
			// printf("No info for: %.*s\n", (int)nmsz, nm);
			continue;
		}
		if (strncmp(nm,"state/global.__state__",nmsz) == 0)
		{
			MDB_val rnmKey, rnmVal;

			// Delete this entry in actors
			mdb_cursor_del(wr.cursorActors,0);

			rnmKey.mv_size = strlen("globalbckp");
			rnmKey.mv_data = "globalbckp";
			rnmVal.mv_size = sizeof(index);
			rnmVal.mv_data = &index;

			// Store it under globalbckp
			mdb_put(wr.txn, wr.actorsdb, &rnmKey, &rnmVal, 0);
			continue;
		}
		memcpy(&firstCompleteTerm, ((u8*)data.mv_data)+1, sizeof(u64));
		memcpy(&firstCompleteEvnum,((u8*)data.mv_data)+1+sizeof(u64), sizeof(u64));

		memcpy(pagesKeyBuf,               &index,sizeof(u64));
		memcpy(pagesKeyBuf + sizeof(u64), &pgno, sizeof(u32));
		pgKey.mv_data = pagesKeyBuf;
		pgKey.mv_size = sizeof(pagesKeyBuf);
		while (mdb_cursor_get(wr.cursorPages,&pgKey,&pgVal,pgop) == MDB_SUCCESS)
		{
			u64 aindex;
			memcpy(&aindex,pgKey.mv_data,sizeof(u64));
			if (aindex != index)
				break;
			mdb_cursor_del(wr.cursorPages, MDB_NODUPDATA);
			pgop = MDB_NEXT_NODUP;
		}

		memcpy(logKeyBuf,                 &index,          sizeof(u64));
		memcpy(logKeyBuf + sizeof(u64),   &firstCompleteTerm, sizeof(u64));
		memcpy(logKeyBuf + sizeof(u64)*2, &firstCompleteEvnum,sizeof(u64));
		logKey.mv_data = logKeyBuf;
		logKey.mv_size = sizeof(logKeyBuf);
		if (mdb_cursor_get(wr.cursorLog,&logKey,&logVal,MDB_SET) == MDB_SUCCESS)
		{
			u64 aindex;

			mdb_cursor_del(wr.cursorLog, MDB_NODUPDATA);
			while ((mdb_cursor_get(wr.cursorLog,&logKey,&logVal,MDB_NEXT_NODUP)) == MDB_SUCCESS)
			{
				memcpy(&aindex, logKey.mv_data, sizeof(u64));
				if (index != aindex)
					break;
				mdb_cursor_del(wr.cursorLog, MDB_NODUPDATA);
			}
		}

		// delete state (ranges,catchup,state/..)
		mdb_cursor_del(wr.cursorActors,0);
	}while ((rc = mdb_cursor_get(wr.cursorActors,&key,&data,MDB_NEXT)) == MDB_SUCCESS);
	mdb_txn_commit(wr.txn);


	// 
	// Should we support incremental backup from here? It may actually be slower since it involves checking
	// every actor if any pages have changed, if so doing inject/rewind.
	// 

bckp_done:
	close_env(&wr);
	return 0;
}

// static int do_checkpoint(const char *pth)
// {
// 	lmdb rd;
// 	u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
// 	MDB_val pgKey = {0,NULL}, pgVal = {0,NULL};
// 	u64 evnum,evterm,aindex = 126;
// 	u32 pgno = 65;
// 	int mrc;

// 	memcpy(pagesKeyBuf,               &aindex,sizeof(u64));
// 	memcpy(pagesKeyBuf + sizeof(u64), &pgno,  sizeof(u32));
// 	pgKey.mv_data = pagesKeyBuf;
// 	pgKey.mv_size = sizeof(pagesKeyBuf);

// 	if (open_env(&rd, pth, 0) == -1)
// 	{
// 		fprintf(stderr,"Unable to open source environment\n");
// 		return -1;
// 	}

// 	if (mdb_cursor_get(rd.cursorPages,&pgKey,&pgVal,MDB_SET) != MDB_SUCCESS)
// 	{
// 		return 0;
// 	}
// 	if (mdb_cursor_get(rd.cursorPages,&pgKey,&pgVal,MDB_FIRST_DUP) != MDB_SUCCESS)
// 		return 0;

// 	do{
// 		MDB_val pgDelKey = {0,NULL}, pgDelVal = {0,NULL};
// 		mdb_cursor_get(rd.cursorPages,&pgDelKey,&pgDelVal,MDB_GET_CURRENT);
// 		memcpy(&evterm, pgDelVal.mv_data,                 sizeof(u64));
// 		memcpy(&evnum,  (u8*)pgDelVal.mv_data+sizeof(u64),sizeof(u64));
// 		DBG("next %llu",evnum);
// 		// if (evnum < 5538) //5654 
// 		if (evnum == 5538)
// 		{
// 			mrc = mdb_cursor_del(rd.cursorPages,0);
// 			if (mrc != MDB_SUCCESS)
// 			{
// 				DBG("Unable to delete page on cursor! err=%d, evnum=%llu",mrc,evnum);
// 				return 1;
// 			}
// 			DBG("Deleted");
// 		}
		
// 	}while (mdb_cursor_get(rd.cursorPages,&pgKey,&pgVal,MDB_NEXT_DUP) == MDB_SUCCESS);
// 	mdb_txn_commit(rd.txn);
// 	return 0;
// }

static int do_extract(const char *pth, const char *actor, const char *type, const char *dst)
{
	lmdb rd;
	FILE *f;
	iterate_resource iter;
	u8 buf[PAGE_BUFF_SIZE];
	u8 hdrbuf[sizeof(u64)*2+sizeof(u32)*2];
	u32 done = 0;
	db_connection conn;
	int nfilled;
	db_thread thr;
	char actorpth[512];
	int i;

	memset(&iter,0,sizeof(iterate_resource));
	memset(&thr,0,sizeof(db_thread));
	memset(&conn,0,sizeof(db_connection));

	if (open_env(&rd, pth, MDB_RDONLY) == -1)
	{
		fprintf(stderr,"Unable to open source environment\n");
		return -1;
	}
	g_tsd_thread = &thr;
	g_tsd_conn = &conn;
	// pthread_setspecific(g_tsd_thread, &thr);
	// pthread_setspecific(g_tsd_conn, &conn);

	thr.mdb.env = rd.menv;
	thr.maxvalsize = mdb_env_get_maxkeysize(rd.menv);
	thr.resFrames = alloca((SQLITE_DEFAULT_PAGE_SIZE/thr.maxvalsize + 1)*sizeof(MDB_val));
	thr.mdb.infodb = rd.infodb;
	thr.mdb.logdb = rd.logdb;
	thr.mdb.pagesdb = rd.pagesdb;
	thr.mdb.actorsdb = rd.actorsdb;
	thr.mdb.txn = rd.txn;
	thr.mdb.cursorLog = rd.cursorLog;
	thr.mdb.cursorPages = rd.cursorPages;
	thr.mdb.cursorInfo = rd.cursorInfo;
	thr.isreadonly = 1;

	// conn.wal.thread = &thr;
	// conn.wal.rthread = &thr;
	// conn.wal.rthreadId = pthread_self();


	for (i = 0; i < 10; i++)
	{
		if (strcmp("termstore",actor) == 0 && strcmp("termstore",type) == 0)
			sprintf(actorpth,"termstore");
		else if (i == 0)
			sprintf(actorpth,"actors/%s.%s",actor,type);
		else if (i == 1)
			sprintf(actorpth,"shards/%s.%s",actor,type);
		else if (i == 2)
			sprintf(actorpth,"state/%s.%s",actor,type);
		else if (i == 3)
			sprintf(actorpth,"actors/%s",actor);
		else if (i == 4)
			sprintf(actorpth,"shards/%s",actor);
		else if (i == 5)
			sprintf(actorpth,"%s",actor);
		else
		{
			printf("Can not find actor\n");
			return -1;
		}

		if (sqlite3WalOpen(NULL, NULL, actorpth, 0, 0, NULL) == SQLITE_ERROR)
			continue;
		else
			break;
	}

	if (dst == NULL)
	{
		char nm[256];
		sprintf(nm,"%s.%s",actor,type);
		f = fopen(nm,"wb");
	}
	else if (strcmp(dst,"-") == 0)
		f = stdout;
	else
		f = fopen(dst,"wb");
	
	if (f == NULL)
	{
		fprintf(stderr,"Unable to open destination file\n");
		return -1;
	}

	while (!done)
	{
		nfilled = wal_iterate(&conn.wal, &iter, buf, PAGE_BUFF_SIZE, hdrbuf, &done);
		if (nfilled > 0)
		{
			u8 decompr[SQLITE_DEFAULT_PAGE_SIZE];
			int rc;

			rc = LZ4_decompress_safe((char*)buf,(char*)decompr,nfilled,sizeof(decompr));
			if (rc != SQLITE_DEFAULT_PAGE_SIZE)
			{
				fprintf(stderr,"Decompress bad size=%d\n",rc);
				return -1;
			}

			fwrite(decompr,1,sizeof(decompr),f);
		}
	}
	fclose(f);
	close_env(&rd);
	return 0;
}


int main(int argc, const char* argv[])
{
	g_log = stdout;
	priv_data pd;
	g_pd = &pd;

	memset(&pd, 0,sizeof(priv_data));
	// pthread_key_create(&g_tsd_thread, NULL);

	if (argc >= 3 && strcmp(argv[1],"print") == 0)
	{
		int flag;
		const char *path;

		if (argc == 3 || strcmp(argv[2],"all") == 0)
		{
			flag = PRINT_ACTORS | PRINT_LOG | PRINT_INFO | PRINT_PAGES;
			path = argc == 3 ? argv[2] : argv[3];
		}			
		else
		{
			path = argv[3];
			if (strcmp(argv[2],"info") == 0)
				flag = PRINT_INFO;
			else if (strcmp(argv[2],"pages") == 0)
				flag = PRINT_PAGES;
			else if (strcmp(argv[2],"log") == 0)
				flag = PRINT_LOG;
			else if (strcmp(argv[2],"actors") == 0)
				flag = PRINT_ACTORS;
		} 
		do_print(path, flag);
	}
	else if (argc == 4 && strcmp(argv[1],"backup") == 0)
	{
		char ch = 'y';
		
		if (file_size(argv[2]) == 0)
		{
			fprintf(stderr,"Source db empty\n");
			return 0;
		}
		if (file_size(argv[3]) != 0)
		{
			fprintf(stderr,"Destination already exists\n");
			return 0;
		}

		do_backup(argv[2],argv[3]);
	}
	else if ((argc == 5 || argc == 6) && strcmp(argv[1],"extract") == 0)
	{
		if (file_size(argv[2]) == 0)
		{
			fprintf(stderr,"Source db empty\n");
			return 0;
		}

		if (argc == 5)
			do_extract(argv[2],argv[3],argv[4],NULL);
		else
			do_extract(argv[2],argv[3],argv[4], argv[5]);
	}
	// else if (argc == 3 && strcmp(argv[1],"checkpoint") == 0)
	// {
	// 	do_checkpoint(argv[2]);
	// }
	else
	{
		printf("Backup:\n");
		printf("%s backup /path/to/source/lmdb /path/to/backup/lmdb\n",argv[0]);
		printf("To backup to stdout, use -\n");
		printf("\n");
		printf("Extract an individual actor to an sqlite file\n");
		printf("%s extract /path/to/lmdb_file actorname actortype out_file\n",argv[0]);
		printf("\n");
		printf("Diagnostic print DB structure. This can be a lot of data!\n");
		printf("%s print [all|info|pages|log|actors] /path/to/lmdb_file\n",argv[0]);
		printf("\n");
		return 1;
	}

	return 1;
}
