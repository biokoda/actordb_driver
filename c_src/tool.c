// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
#define _TESTDBG_ 1
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
#include "lz4.h"
// #include "tool_do.c"


static int logdb_cmp(const MDB_val *a, const MDB_val *b)
{
	i64 aActor,aEvterm,aEvnum,bActor,bEvterm,bEvnum;
	int diff;

	memcpy(&aActor,a->mv_data,sizeof(i64));
	memcpy(&bActor,b->mv_data,sizeof(i64));
	diff = aActor - bActor;
	if (diff == 0)
	{
		memcpy(&aEvterm, a->mv_data+sizeof(i64), sizeof(i64));
		memcpy(&bEvterm, b->mv_data+sizeof(i64), sizeof(i64));
		diff = aEvterm - bEvterm;
		if (diff == 0)
		{
			memcpy(&aEvnum, a->mv_data+sizeof(i64)*2, sizeof(i64));
			memcpy(&bEvnum, b->mv_data+sizeof(i64)*2, sizeof(i64));
			return aEvnum - bEvnum;
		}
		return diff;
	}
	return diff;
}

static int pagesdb_cmp(const MDB_val *a, const MDB_val *b)
{
	i64 aActor;
	i64 bActor;
	u32 aPgno;
	u32 bPgno;
	int diff;

	memcpy(&aActor,a->mv_data,sizeof(i64));
	memcpy(&bActor,b->mv_data,sizeof(i64));
	diff = aActor - bActor;
	if (diff == 0)
	{
		memcpy(&aPgno,a->mv_data + sizeof(i64),sizeof(u32));
		memcpy(&bPgno,b->mv_data + sizeof(i64),sizeof(u32));
		return aPgno - bPgno;
	}
	return diff;
}

static int pagesdb_val_cmp(const MDB_val *a, const MDB_val *b)
{
	i64 aEvterm,aEvnum;
	i64 bEvterm,bEvnum;
	u8 aCounter, bCounter;
	int diff;

	memcpy(&aEvterm, a->mv_data, sizeof(i64));
	memcpy(&bEvterm, b->mv_data, sizeof(i64));
	diff = aEvterm - bEvterm;
	if (diff == 0)
	{
		memcpy(&aEvnum, a->mv_data+sizeof(i64), sizeof(i64));
		memcpy(&bEvnum, b->mv_data+sizeof(i64), sizeof(i64));
		diff = aEvnum - bEvnum;
		if (diff == 0)
		{
			aCounter = ((u8*)a->mv_data)[sizeof(i64)*2];
			bCounter = ((u8*)b->mv_data)[sizeof(i64)*2];
			return aCounter - bCounter;
		}
		return diff;
	}
	return diff;
}

static int do_print(char *pth)
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
	MDB_val key, data;
	int rc, op;

	if (mdb_env_create(&menv) != MDB_SUCCESS)
		return -1;
	if (mdb_env_set_maxdbs(menv,5) != MDB_SUCCESS)
		return -1;
	if (mdb_env_set_mapsize(menv,4096*1024*128*10) != MDB_SUCCESS)
		return -1;
	if (mdb_env_open(menv, pth, MDB_NOSUBDIR | MDB_RDONLY, 0664) != MDB_SUCCESS)
		return -1;
	if (mdb_txn_begin(menv, NULL, MDB_RDONLY, &txn) != MDB_SUCCESS)
		return -1;

	if (mdb_dbi_open(txn, "info", MDB_INTEGERKEY, &infodb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(txn, "actors", MDB_CREATE, &actorsdb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(txn, "log", MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERDUP, &logdb) != MDB_SUCCESS)
		return -1;
	if (mdb_dbi_open(txn, "pages", MDB_DUPSORT, &pagesdb) != MDB_SUCCESS)
		return -1;
	if (mdb_set_compare(txn, logdb, logdb_cmp) != MDB_SUCCESS)
		return -1;
	if (mdb_set_compare(txn, pagesdb, pagesdb_cmp) != MDB_SUCCESS)
		return -1;
	if (mdb_set_dupsort(txn, pagesdb, pagesdb_val_cmp) != MDB_SUCCESS)
		return -1;
	if (mdb_cursor_open(txn, logdb, &cursorLog) != MDB_SUCCESS)
		return -1;
	if (mdb_cursor_open(txn, pagesdb, &cursorPages) != MDB_SUCCESS)
		return -1;
	if (mdb_cursor_open(txn, infodb, &cursorInfo) != MDB_SUCCESS)
		return -1;
	if (mdb_cursor_open(txn, actorsdb, &cursorActors) != MDB_SUCCESS)
		return -1;

	printf("-----------------------actorsdb--------------------------\n");
	rc = mdb_cursor_get(cursorActors,&key,&data,MDB_FIRST);
	while (rc == MDB_SUCCESS)
	{
		u64 index;
		memcpy(&index, data.mv_data, sizeof(u64));
		printf("Actor=%.*s, id=%llu\n",(int)key.mv_size, key.mv_data, index);
		rc = mdb_cursor_get(cursorActors,&key,&data,MDB_NEXT);
	}

	printf("-----------------------logdb--------------------------\n");
	rc = mdb_cursor_get(cursorLog,&key,&data,MDB_FIRST);
	while (rc == MDB_SUCCESS)
	{
		u64 index, term, num;
		memcpy(&index, key.mv_data,                 sizeof(u64));
		memcpy(&term,  key.mv_data + sizeof(u64),   sizeof(u64));
		memcpy(&num,   key.mv_data + sizeof(u64)*2, sizeof(u64));
		printf("logdb: actor=%llu, term=%llu, evnum=%llu\n",index, term,num);

		op = MDB_FIRST_DUP;
		while ((rc = mdb_cursor_get(cursorLog,&key,&data, op)) == MDB_SUCCESS)
		{
			u32 pgno;
			memcpy(&pgno,data.mv_data,sizeof(u32));
			printf("  pgno=%u\n",pgno);
			op = MDB_NEXT_DUP;
		}
		rc = mdb_cursor_get(cursorLog,&key,&data,MDB_NEXT_NODUP);
	}

	printf("-----------------------pagesdb--------------------------\n");
	rc = mdb_cursor_get(cursorPages,&key,&data,MDB_FIRST);
	while (rc == MDB_SUCCESS)
	{
		u64 index;
		u32 pgno;
		memcpy(&index, key.mv_data, sizeof(u64));
		memcpy(&pgno, key.mv_data + sizeof(u64), sizeof(u32));
		printf("pagesdb: actor=%llu, pgno=%u\n",index, pgno);

		op = MDB_FIRST_DUP;
		while ((rc = mdb_cursor_get(cursorPages,&key,&data, op)) == MDB_SUCCESS)
		{
			u64 term,num;
			u8 frag;
			memcpy(&term, data.mv_data,               sizeof(u64));
			memcpy(&num,  data.mv_data + sizeof(u64), sizeof(u64));
			frag = *(u8*)(data.mv_data + sizeof(u64)*2);
			printf("  evterm=%lld, evnum=%lld, frag=%d, pgsize=%ld\n",term,num,(int)frag,data.mv_size-sizeof(u64)*2-1);
			op = MDB_NEXT_DUP;
		}
		rc = mdb_cursor_get(cursorPages,&key,&data,MDB_NEXT);
	}

	printf("-----------------------infodb--------------------------\n");
	rc = mdb_cursor_get(cursorInfo, &key, &data, MDB_FIRST);
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

		rc = mdb_cursor_get(cursorInfo, &key, &data, MDB_NEXT);
	}
	return 0;
}


int main(int argc, char* argv[])
{
	g_log = stdout;

	if (argc != 3)
	{
		printf("Call: tool print /path/to/lmdb_file\n");
		return 1;
	}
	else if (strcmp(argv[1],"print") != 0)
	{
		printf("Invalid command\n");
		return 1;
	}

	if (strcmp(argv[1],"print") == 0)
	{
		do_print(argv[2]);
	}

	return 1;
}
