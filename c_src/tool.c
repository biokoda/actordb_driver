// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
// #define _TESTDBG_ 1
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

// wal.c code has been taken out of sqlite3.c and placed in wal.c file.
// Every wal interface function is changed, but the wal-index code remains unchanged.
#include "wal.c"
#include "lz4.h"
// #include "tool_do.c"

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
			printf("Actor=%.*s, id=%llu\n",(int)key.mv_size, key.mv_data, index);
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
			memcpy(&index, key.mv_data, sizeof(u64));
			memcpy(&pgno, key.mv_data + sizeof(u64), sizeof(u32));
			printf("pagesdb: actor=%llu, pgno=%u\n",index, pgno);

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

static void sighandle(int sig)
{
}

static int do_backup(const char *src, const char *dst)
{
	lmdb rd, wr;

#ifdef SIGPIPE
	signal(SIGPIPE, sighandle);
#endif
#ifdef SIGHUP
	signal(SIGHUP, sighandle);
#endif
	signal(SIGINT, sighandle);
	signal(SIGTERM, sighandle);

	memset(&rd,0,sizeof(rd));
	memset(&wr,0,sizeof(wr));

	if (file_size(dst) == 0)
	{
		int rc;
		if (mdb_env_create(&rd.menv) != MDB_SUCCESS)
			return -1;
		if (mdb_env_open(rd.menv, src, MDB_NOSUBDIR | MDB_RDONLY, 0600) != MDB_SUCCESS)
			return -1;

		rc = mdb_env_copy2(rd.menv, dst, 0);
		if (rc != 0)
			printf("Backup failed %s\n",strerror(rc));
		goto bckp_done;
	}

	if (open_env(&rd, src, MDB_RDONLY) == -1)
	{
		printf("Unable to open source environment\n");
		return -1;
	}
	if (open_env(&wr, dst, 0) == -1)
	{
		printf("Unable to open destination environment\n");
		return -1;
	}


bckp_done:
	close_env(&rd);
	close_env(&wr);
	return 0;
}


int main(int argc, const char* argv[])
{
	g_log = stdout;

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
		char ch;
		
		if (file_size(argv[2]) == 0)
		{
			printf("Source db empty\n");
			return 0;
		}

		printf("Read from:\n%s\nWrite to:\n%s\nConfirm (Y/N)?\n", argv[2], argv[3]);
		do
		{
			ch = fgetc(stdin);
		} while (ch != 'y' && ch != 'Y' && ch != 'N' && ch != 'n');
		
		if (ch == 'y' || ch == 'Y')
			do_backup(argv[2],argv[3]);
	}
	else
	{
		printf("Call examples:\n%s print [all|info|pages|log|actors] /path/to/lmdb_file\n",argv[0]);
		printf("%s backup /path/to/source/lmdb /path/to/backup/lmdb\n",argv[0]);
		printf("\n");
		return 1;
	}


	return 1;
}
