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
    aEvterm = *(i64*)(a->mv_data+sizeof(i64));
    bEvterm = *(i64*)(b->mv_data+sizeof(i64));
    diff = aEvterm - bEvterm;
    if (diff == 0)
    {
      aEvnum  = *(i64*)(a->mv_data+sizeof(i64)*2);
      bEvnum  = *(i64*)(a->mv_data+sizeof(i64)*2);
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
    aPgno = *(u32*)(a->mv_data+sizeof(i64));
    bPgno = *(u32*)(b->mv_data+sizeof(i64));
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
  if (mdb_dbi_open(data->wtxn, "log", MDB_CREATE | MDB_DUPSORT | MDB_INTEGERKEY, &data->logdb) != MDB_SUCCESS)
    return NULL;
  if (mdb_dbi_open(data->wtxn, "pages", MDB_CREATE | MDB_DUPSORT | MDB_INTEGERKEY, &data->pagesdb) != MDB_SUCCESS)
    return NULL;
  // if (mdb_dbi_open(data->wtxn, "test1", MDB_CREATE | MDB_DUPSORT | MDB_INTEGERKEY, &data->testdb) != MDB_SUCCESS)
  //   return NULL;
  if (mdb_set_compare(data->wtxn, data->logdb, logdb_cmp) != MDB_SUCCESS)
    return NULL;
  if (mdb_set_compare(data->wtxn, data->pagesdb, pagesdb_cmp) != MDB_SUCCESS)
    return NULL;
  if (mdb_cursor_open(data->wtxn, data->logdb, &data->cursorLog) != MDB_SUCCESS)
    return NULL;
  if (mdb_cursor_open(data->wtxn, data->pagesdb, &data->cursorPages) != MDB_SUCCESS)
    return NULL;
  if (mdb_cursor_open(data->wtxn, data->infodb, &data->cursorInfo) != MDB_SUCCESS)
    return NULL;
  // if (mdb_cursor_open(data->wtxn, data->testdb, &data->cursorTest) != MDB_SUCCESS)
  //   return NULL;

  return data->wtxn;
}


int main(int argc, char* argv[])
{
	db_thread thread;
	db_command clcmd;
  char *path = "dbfile";
  int i = 0, j, rc;
  u8 page[SQLITE_DEFAULT_PAGE_SIZE];

  for (i = 0; i < SQLITE_DEFAULT_PAGE_SIZE; i++)
    page[i] = i;

  g_log = stdout;

  if (mdb_env_create(&thread.env) != MDB_SUCCESS)
    return 0;

  if (mdb_env_set_maxdbs(thread.env,5) != MDB_SUCCESS)
    return 0;

  // TODO: set this as an input parameter, right now 5GB
  if (mdb_env_set_mapsize(thread.env,4096*1024*128*10) != MDB_SUCCESS)
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

  // {
  //   // If we want sorted data values must be big endian
  //   MDB_val key, data;
  //   int j = 0;
  //   key.mv_data = &j;
  //   key.mv_size = sizeof(j);
  //   for (i = 0; i < 1000; i++)
  //   {
  //     u8 buf[10];
  //     sqlite3Put4byte(buf, i / 10);
  //     sqlite3Put4byte(buf+4, i);
  //     buf[8] = i;
  //     buf[9] = i;
  //     data.mv_data = buf;
  //     data.mv_size = sizeof(buf);
  //     mdb_put(thread.wtxn,thread.testdb,&key,&data,0);
  //   }
  //
  //   mdb_cursor_get(thread.cursorTest,&key,&data,MDB_FIRST_DUP);
  //   printf("First %d\n",*(int*)data.mv_data);
  //   for (i = 0; i < 100; i++)
  //   {
  //     mdb_cursor_get(thread.cursorTest,&key,&data,MDB_NEXT_DUP);
  //     printf("Next %d %d\n",sqlite3Get4byte(data.mv_data),sqlite3Get4byte(data.mv_data+4));
  //   }
  //   return 0;
  // }


  for (i = 0; i < 1000;i++)
  {
    char name[256];
    snprintf(name,256,"actor/%d",i);
    Wal *pWal;
    db_connection con;
    memset(&con,0,sizeof(db_connection));
    con.dbpath = name;

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
    if (open_wtxn(&thread) == NULL)
      return 0;

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
    // if (mdb_txn_begin(thread.env, NULL, 0, &thread.wtxn) != MDB_SUCCESS)
    //   return 0;
  }
  printf("DBs created. Inserting pages\n");
  for (j = 0; j < 10; j++)
  {
    for (i = 0; i <= 100; i++)
    {
      db_connection con;
      con.wal.thread = &thread;
      con.wal.index = j;
      thread.curConn = &con;

      if (i == 100)
      {
        if ((rc = mdb_txn_commit(thread.wtxn)) != MDB_SUCCESS)
        {
          DBG((g_log,"COmmit failed! %d\r\n",rc));
          return 0;
        }
        if (open_wtxn(&thread) == NULL)
        {
          printf("Cant reopen txn?!\n");
          return 0;
        }

        // Undo evnum = 9, evterm = 9
        DBG((g_log,"%lld %lld\n",con.wal.inProgressTerm, con.wal.inProgressEvnum));
        sqlite3WalUndo(&con.wal, NULL, NULL);
        continue;
      }

      con.writeTermNumber = i / 10;
      con.writeNumber = i / 10;

      PgHdr pgList;
      memset(&pgList,0,sizeof(PgHdr));
      pgList.pgno = i % 10;
      pgList.pData = page;

      if (sqlite3WalFrames(&con.wal, sizeof(page), &pgList, 10, (i % 10) == 0, 0) != SQLITE_OK)
      {
        printf("Write frames failed\n");
        break;
      }

      // if (i % 10 == 0)
      {
        if ((rc = mdb_txn_commit(thread.wtxn)) != MDB_SUCCESS)
        {
          DBG((g_log,"COmmit failed! %d\r\n",rc));
          return 0;
        }
        if (open_wtxn(&thread) == NULL)
        {
          printf("Cant reopen txn?!\n");
          return 0;
        }
      }
    }
  }

  if (mdb_txn_commit(thread.wtxn) != MDB_SUCCESS)
  {
    DBG((g_log,"COmmit failed!\r\n"));
    return 0;
  }
  if (open_wtxn(&thread) == NULL)
  {
    printf("Cant open txn\n");
    return 0;
  }

  // Print pages db. It must be correctly sorted.
  // pages should be missing for term 9 and evnum 9
  {
    MDB_val key, data;
    u64 term,evnum,actor;
    u32 pgno;
    int op = MDB_FIRST;

    while (mdb_cursor_get(thread.cursorPages,&key,&data,op) == MDB_SUCCESS)
    {
      actor = *(u64*)key.mv_data;
      pgno = *(u32*)(key.mv_data+sizeof(u64));
      term = readUInt64(data.mv_data);
      evnum = readUInt64(data.mv_data+sizeof(u64));

      printf("pages key: actor=%lld,pgno=%u --- value: evterm=%lld,evnum=%lld\n",actor,pgno,term,evnum);
      op = MDB_NEXT;
    }
  }

  // Print log db. It must be correctly sorted.
  {
    MDB_val key, data;
    u64 term,evnum,actor;
    u32 pgno;
    int op = MDB_FIRST;

    while (mdb_cursor_get(thread.cursorLog,&key,&data,op) == MDB_SUCCESS)
    {
      actor = *(u64*)key.mv_data;
      term = *(u64*)(key.mv_data+sizeof(u64));
      evnum = *(u64*)(key.mv_data+sizeof(u64)*2);
      pgno = sqlite3Get4byte(data.mv_data);

      printf("log key: actor=%lld,evterm=%lld,evnum=%lld -- value: pgno=%u\n",actor,term,evnum,pgno);
      op = MDB_NEXT;
    }
  }

  // Test with sqlite
  {
    char name[256];
    db_connection con;
    char val[512];
    char txt[600];

    memset(val,0,sizeof(val));
    memset(val,'a',500);
    memset(&con,0,sizeof(db_connection));
    snprintf(name,256,"sqlitedb");
    con.wal.thread = &thread;
    con.dbpath = name;
    thread.curConn = &con;

    rc = sqlite3_open(name,&(con.db));
    if (rc != SQLITE_OK)
    {
      printf("Cant open=%d\n",rc);
      return 0;
    }
    sqlite3_wal_data(con.db,&thread);
    rc = sqlite3_exec(con.db,"PRAGMA journal_mode=wal;create table tab (id integer primary key, val text);",NULL,NULL,NULL);
    if (rc != SQLITE_OK)
    {
      printf("Exec failed=%d\n",rc);
      return 0;
    }

    for (i = 0; i < 1000; i++)
    {
      con.writeNumber = i;
      con.writeTermNumber = i / 10;
      sprintf(txt,"insert into tab values (%d,'text for %s');",i,val);
      // printf("calling: %s\n",txt);
      rc = sqlite3_exec(con.db,txt,NULL,NULL,NULL);
      if (rc != SQLITE_OK)
      {
        printf("Exec failed\n");
        break;
      }

      if ((rc = mdb_txn_commit(thread.wtxn)) != MDB_SUCCESS)
      {
        DBG((g_log,"COmmit failed! %d\r\n",rc));
        return 0;
      }
      if (open_wtxn(&thread) == NULL)
      {
        printf("Cant reopen txn?!\n");
        return 0;
      }
      // rc = sqlite3_prepare_v2(con.db, txt, strlen(txt), &(statement), NULL);
    }

    for (i = 0; i < 1000; i++)
    {
      sprintf(txt,"select * from tab where id=%d;",i);
      printf("calling: %s\n",txt);
      rc = sqlite3_exec(con.db,txt,NULL,NULL,NULL);
      if (rc != SQLITE_OK)
        break;
    }
  }



  // for (i = 0; i < 10; i++)
  // {
  //   db_connection con;
  //   u32 n;
  //   con.wal.thread = &thread;
  //   con.wal.index = 0;
  //   thread.curConn = &con;
  //   con.wal.curFrame.mv_size = 0;
  //   con.wal.curFrame.mv_data = NULL;
  //
  //   sqlite3WalFindFrame(&con.wal, i, &n);
  //   if (!n)
  //   {
  //     printf("Frame not found!!\n");
  //     break;
  //   }
  //   sqlite3WalReadFrame(&con.wal, n, sizeof(page), page);
  // }


  mdb_txn_commit(thread.wtxn);
  // mdb_dbi_close(thread.env,thread.infodb);
  // mdb_dbi_close(thread.env,thread.actorsdb);
  // mdb_dbi_close(thread.env,thread.logdb);
  // mdb_dbi_close(thread.env,thread.pagesdb);
  mdb_env_close(thread.env);

  return 0;
}
