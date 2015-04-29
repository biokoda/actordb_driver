/* ActorDB -> sqlite -> lz4 -> REPLICATION pipe -> LMDB
** WAL is where sqlite ends and lmdb starts.

** We will not be doing any endian checking/encoding, as we do not care about any big-endian platforms.

** LMDB schema:
** - Actors DB: {<<ActorName/binary>>, <<ActorIndex:64>>}

** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,CompressedPage/binary>>}
**   Pages db is a dupsort database. It stores lz4 compressed sqlite pages. There can be multiple
**   pages for one pgno. This is to leave room for replication.
**   When a page is requested from sqlite, it will use the highest commited page.
**   Once replication has run its course, old pages are deleted.

** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
**   Also a dupsort. Every key is one sqlite write transaction. Values are a list of pages
**   that have changed.

** - Info DB: {<<ActorIndex:64>>, <<V,FirstCompleteTerm:64,FirstCompleteEvnum:64,
                                      LastCompleteTerm:64,LastCompleteEvnum:64,
                                      InprogressTerm:64,InProgressEvnum:64>>}
**   V (version) = 1
**   FirstComplete(term/evnum) - First entry for actor in Log DB.
**   LastComplete(term/evnum) - Last entry in log that is commited.
**   InProgress(term/evnum) - pages from this evnum+evterm combination are not commited. If actor has just opened
**                            and it has these values set, it must delete pages to continue.

** On writes log, pages and info db are updated.
** Non-live replication is simply a matter of looking up log and sending the right pages.
** If stale replication or actor copy, simply traverse actor pages from 0 forward until reaching
** the end.
** Undo is a matter of checking the pages of last write in log db and deleting them in log and pages db.
*/
int sqlite3WalOpen(sqlite3_vfs *pVfs, sqlite3_file *pDbFd, const char *zWalName, int bNoShm, i64 mxWalSize, Wal **ppWal, void *walData)
{
  MDB_dbi infodb;
  MDB_txn *txn;
  MDB_val key, data;
  int rc;
  db_thread *thr = (db_thread*)walData;
  Wal *pWal = &thr->curConn->wal;

  if (mdb_txn_begin(thr->env, NULL, MDB_RDONLY, &txn) != MDB_SUCCESS)
    return SQLITE_ERROR;

  if (mdb_dbi_open(txn, "info", MDB_INTEGERKEY, &infodb) != MDB_SUCCESS)
    return SQLITE_ERROR;

  key.mv_size = strlen(thr->curConn->dbpath);
  key.mv_data = thr->curConn->dbpath;
  rc = mdb_get(txn,infodb,&key,&data);

  if (rc == MDB_NOTFOUND)
  {
  }
  else if (rc == MDB_SUCCESS)
  {
  }
  else
  {
    mdb_txn_abort(txn);
    return SQLITE_ERROR;  
  }

  mdb_txn_abort(txn);

  (*ppWal) = pWal;
  return SQLITE_OK;
}


int sqlite3WalClose(Wal *pWal, int sync_flags, int nBuf, u8 *zBuf)
{
  return SQLITE_OK;
}

/* Set the limiting size of a WAL file. */
void sqlite3WalLimit(Wal* wal, i64 size)
{
}

/* Used by readers to open (lock) and close (unlock) a snapshot.  A
** snapshot is like a read-transaction.  It is the state of the database
** at an instant in time.  sqlite3WalOpenSnapshot gets a read lock and
** preserves the current state even if the other threads or processes
** write to or checkpoint the WAL.  sqlite3WalCloseSnapshot() closes the
** transaction and releases the lock.
*/
int sqlite3WalBeginReadTransaction(Wal *pWal, int *pChanged)
{
  return SQLITE_OK;
}

void sqlite3WalEndReadTransaction(Wal *pWal)
{
}

/* Read a page from the write-ahead log, if it is present. */
int sqlite3WalFindFrame(Wal *pWal, Pgno pgno, u32 *piRead)
{
  db_thread *thread = pWal->thread;


  return SQLITE_OK;
}

int sqlite3WalReadFrame(Wal *pWal, u32 iRead, int nOut, u8 *pOut)
{
  return SQLITE_OK;
}

/* If the WAL is not empty, return the size of the database. */
Pgno sqlite3WalDbsize(Wal *pWal)
{
  return 0;
}

/* Obtain or release the WRITER lock. */
int sqlite3WalBeginWriteTransaction(Wal *pWal)
{
  return SQLITE_OK;
}
int sqlite3WalEndWriteTransaction(Wal *pWal)
{
  return SQLITE_OK;
}

/* Undo any frames written (but not committed) to the log */
int sqlite3WalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx)
{
  return SQLITE_OK;
}

/* Return an integer that records the current (uncommitted) write
** position in the WAL */
void sqlite3WalSavepoint(Wal *pWal, u32 *aWalData)
{
}

/* Move the write position of the WAL back to iFrame.  Called in
** response to a ROLLBACK TO command. */
int sqlite3WalSavepointUndo(Wal *pWal, u32 *aWalData)
{
  return SQLITE_OK;
}

/* Write a frame or frames to the log. */
int sqlite3WalFrames(Wal *pWal, int szPage, PgHdr *pList, Pgno nTruncate, int isCommit, int sync_flags)
{
  PgHdr *p;
  db_thread *thr = pWal->thread;

  for(p=pList; p; p=p->pDirty)
  {
    thr->pagesChanged++;
  }

  return SQLITE_OK;
}

/* Copy pages from the log to the database file */
int sqlite3WalCheckpoint(
  Wal *pWal,                      /* Write-ahead log connection */
  int eMode,                      /* One of PASSIVE, FULL and RESTART */
  int (*xBusy)(void*),            /* Function to call when busy */
  void *pBusyArg,                 /* Context argument for xBusyHandler */
  int sync_flags,                 /* Flags to sync db file with (or 0) */
  int nBuf,                       /* Size of buffer nBuf */
  u8 *zBuf,                       /* Temporary buffer to use */
  int *pnLog,                     /* OUT: Number of frames in WAL */
  int *pnCkpt                     /* OUT: Number of backfilled frames in WAL */
)
{
  return SQLITE_OK;
}



/* Return the value to pass to a sqlite3_wal_hook callback, the
** number of frames in the WAL at the point of the last commit since
** sqlite3WalCallback() was called.  If no commits have occurred since
** the last call, then return 0.
*/
int sqlite3WalCallback(Wal *pWal)
{
  return SQLITE_OK;
}

/* Tell the wal layer that an EXCLUSIVE lock has been obtained (or released)
** by the pager layer on the database file.
*/
int sqlite3WalExclusiveMode(Wal *pWal, int op)
{
  return SQLITE_OK;
}

/* Return true if the argument is non-NULL and the WAL module is using
** heap-memory for the wal-index. Otherwise, if the argument is NULL or the
** WAL module is using shared-memory, return false.
*/
int sqlite3WalHeapMemory(Wal *pWal)
{
  return pWal != NULL;
}





// New function. It adds thread pointer to wal structure.
SQLITE_API int sqlite3_wal_data(
  sqlite3 *db,
  void *pArg
  ){

  int rt = SQLITE_NOTFOUND;
  int i;
  for(i=0; i<db->nDb; i++){
    Btree *pBt = db->aDb[i].pBt;
    if( pBt ){
      Pager *pPager = sqlite3BtreePager(pBt);
      if (pPager->pWal)
      {
          // pPager->pWal->thread = (db_thread*)pArg;
          rt = SQLITE_OK;
      }
      else
      {
        pPager->walData = pArg;
        rt = SQLITE_OK;
      }
    }
  }
  return rt;
}

void writeUInt64(unsigned char* buf, unsigned long long num)
{
  buf[0] = num >> 56;
  buf[1] = num >> 48;
  buf[2] = num >> 40;
  buf[3] = num >> 32;
  buf[4] = num >> 24;
  buf[5] = num >> 16;
  buf[6] = num >> 8;
  buf[7] = num;
}

u64 readUInt64(u8* buf)
{
  return ((u64)buf[0] << 56) + ((u64)buf[1] << 48) + ((u64)buf[2] << 40) + ((u64)buf[3] << 32) +
       ((u64)buf[4] << 24) + ((u64)buf[5] << 16)  + ((u64)buf[6] << 8) + buf[7];
}
