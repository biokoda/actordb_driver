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
#include "lz4.h"

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
// #include "tool_do.c"

static int logdb_cmp(const MDB_val *a, const MDB_val *b)
{
  // <<ActorIndex:64, Evterm:64, Evnum:64>>
  i64 aActor,aEvterm,aEvnum,bActor,bEvterm,bEvnum;
  int diff;

  aActor = *(i64*)a->mv_data;
  bActor = *(i64*)b->mv_data;
  diff = aActor - bActor;
  if (diff == 0)
  {
    aEvterm = *((i64*)a->mv_data+sizeof(i64));
    bEvterm = *((i64*)b->mv_data+sizeof(i64));
    diff = aEvterm - bEvterm;
    if (diff == 0)
    {
      aEvnum  = *((i64*)a->mv_data+sizeof(i64)*2);
      bEvnum  = *((i64*)a->mv_data+sizeof(i64)*2);
      return aEvnum - bEvnum;
    }
    else
    {
      return diff;
    }
  }
  else
  {
    return diff;
  }
}

static int pagesdb_cmp(const MDB_val *a, const MDB_val *b)
{
  // <<ActorIndex:64, Pgno:32/unsigned>>
  i64 aActor;
  i64 bActor;
  u32 aPgno;
  u32 bPgno;
  int diff;

  aActor = *(i64*)a->mv_data;
  bActor = *(i64*)b->mv_data;
  diff = aActor - bActor;
  if (diff == 0)
  {
    aPgno = *((u32*)a->mv_data+sizeof(i64));
    bPgno = *((u32*)b->mv_data+sizeof(i64));
    return aPgno - bPgno;
  }
  else
  {
    return diff;
  }
}

static MDB_txn* open_wtxn(db_thread *data)
{
  if (mdb_txn_begin(data->env, NULL, 0, &data->wtxn) != MDB_SUCCESS)
    return NULL;

  if (mdb_dbi_open(data->wtxn, "info", MDB_INTEGERKEY | MDB_CREATE, &data->infodb) != MDB_SUCCESS)
    return NULL;
  if (mdb_dbi_open(data->wtxn, "actors", MDB_CREATE, &data->actorsdb) != MDB_SUCCESS)
    return NULL;
  if (mdb_dbi_open(data->wtxn, "log", MDB_CREATE | MDB_INTEGERKEY, &data->logdb) != MDB_SUCCESS)
    return NULL;
  if (mdb_dbi_open(data->wtxn, "pages", MDB_CREATE | MDB_INTEGERKEY, &data->pagesdb) != MDB_SUCCESS)
    return NULL;
  if (mdb_set_compare(data->wtxn, data->logdb, logdb_cmp) != MDB_SUCCESS)
    return NULL;
  if (mdb_set_compare(data->wtxn, data->pagesdb, pagesdb_cmp) != MDB_SUCCESS)
    return NULL;
  if (mdb_cursor_open(data->wtxn, data->logdb, &data->cursorLog) != MDB_SUCCESS)
    return NULL;
  if (mdb_cursor_open(data->wtxn, data->pagesdb, &data->cursorPages) != MDB_SUCCESS)
    return NULL;


  return data->wtxn;
}


int main(int argc, char* argv[])
{
	db_thread thread;
	db_command clcmd;
  char *path = "dbfile";
  int i = 0;

  g_log = stdout;

  if (mdb_env_create(&thread.env) != MDB_SUCCESS)
    return 0;

  if (mdb_env_set_maxdbs(thread.env,4) != MDB_SUCCESS)
    return 0;

  // TODO: set this as an input parameter, right now 549GB
  if (mdb_env_set_mapsize(thread.env,4096*1024*1024*128) != MDB_SUCCESS)
    return 0;

  if (mdb_env_open(thread.env, path, MDB_NOSUBDIR | MDB_NOSYNC, 0664) != MDB_SUCCESS)
    return 0;

  if (open_wtxn(&thread) == NULL)
    return 0;

  // if db empty, our 4 databases were created. Commit.
  if (mdb_txn_commit(thread.wtxn) != MDB_SUCCESS)
    return 0;

  // Now reopen.
  if (open_wtxn(&thread) == NULL)
    return 0;

  for (i = 0; i < 10000;i++)
  {
    char name[256];
    snprintf(name,256,"actor/%d",i);
    Wal *pWal;
    db_connection con;
    memset(&con,0,sizeof(db_connection));
    con.dbpath = name;
    int rc;

    thread.curConn = &con;

    if (sqlite3WalOpen(NULL, NULL, name, 0, 0, &pWal, &thread) != SQLITE_OK)
    {
      break;
    }

    if (mdb_txn_commit(thread.wtxn) != MDB_SUCCESS)
    {
      DBG((g_log,"COmmit failed!\r\n"));
      return 0;
    }

    // {
    //   MDB_txn *txn;
    //   MDB_dbi actorsdb;
    //   MDB_val key, data;
    //
    //   if (mdb_txn_begin(thread.env, NULL, MDB_RDONLY, &txn) != MDB_SUCCESS)
    //     return SQLITE_ERROR;
    //
    //   if (mdb_dbi_open(txn, "actors", MDB_INTEGERKEY, &actorsdb) != MDB_SUCCESS)
    //     return SQLITE_ERROR;
    //
    //   key.mv_size = strlen(name);
    //   key.mv_data = name;
    //   rc = mdb_get(txn,actorsdb,&key,&data);
    //   if (rc == MDB_NOTFOUND)
    //     DBG((g_log,"NOTFOUND!!!"));
    //   if (rc == MDB_SUCCESS)
    //     DBG((g_log,"FOUND: %lld\r\n",*(i64*)data.mv_data));
    // }

    if (mdb_txn_begin(thread.env, NULL, 0, &thread.wtxn) != MDB_SUCCESS)
      return 0;
  }
  DBG((g_log,"Closing\r\n"));
  // mdb_dbi_close(thread.env,thread.infodb);
  // mdb_dbi_close(thread.env,thread.actorsdb);
  // mdb_dbi_close(thread.env,thread.logdb);
  // mdb_dbi_close(thread.env,thread.pagesdb);
  mdb_env_close(thread.env);

  return 0;
}
