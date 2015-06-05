/* ActorDB -> sqlite -> lz4 -> REPLICATION pipe -> LMDB
** WAL is where sqlite ends and lmdb starts.

** LMDB schema:
** - Actors DB: {<<ActorName/binary>>, <<ActorIndex:64>>}
**   {"?",MaxInteger} -> when adding actors, increment this value
**                    -> Indexes start at 100

** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,FragIndex:8,CompressedPage/binary>>}
**   Pages db is a dupsort database. It stores lz4 compressed sqlite pages. There can be multiple
**   pages for one pgno. This is to leave room for replication.
**   When a page is requested from sqlite, it will use the highest commited page.
**   Once replication has run its course, old pages are deleted.
**   Pages that are too large to be placed in a single value are added into multiple dupsort values. FragIndex
**   counts down. If there are 3 pages, first frag will be FragIndex=2.

** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
**   Also a dupsort. Every key is one sqlite write transaction. Values are a list of pages
**   that have changed. Pgno is written big endian because memcmp will return correct sort order.

** - Info DB: {<<ActorIndex:64>>, <<V,FirstCompleteTerm:64,FirstCompleteEvnum:64,
									LastCompleteTerm:64,LastCompleteEvnum:64,
									 InprogressTerm:64,InProgressEvnum:64, DbSize:32>>}
**   V (version) = 1
**   FirstComplete(term/evnum) - First entry for actor in Log DB.
**   LastComplete(term/evnum) - Last entry in log that is commited.
**   InProgress(term/evnum) - pages from this evnum+evterm combination are not commited. If actor has just opened
**and it has these values set, it must delete pages to continue.

** On writes log, pages and info db are updated.
** Non-live replication is simply a matter of looking up log and sending the right pages.
** If stale replication or actor copy, simply traverse actor pages from 0 forward until reaching
** the end.

** Undo is a matter of checking the pages of last write in log db and deleting them in log and pages db.

** Endianess: Data is written as is. Practicaly no relevant platforms are in big endian and I can't see
** a scenario where a lmdb file would be moved between different endian platforms.
*/
int checkpoint(Wal *pWal, u64 evterm, u64 evnum);
int iterate(Wal *pWal, iterate_resource *iter, u8 *buf, int bufsize, u32 *done);
int findframe(Wal *pWal, Pgno pgno, u32 *piRead, u64 limitTerm, u64 limitEvnum);

// 1. Figure out actor index, create one if it does not exist
// 2. check info for evnum/evterm data
int sqlite3WalOpen(sqlite3_vfs *pVfs, sqlite3_file *pDbFd, const char *zWalName,
  int bNoShm, i64 mxWalSize, Wal **ppWal, void *walData)
{
	MDB_val key, data;
	int rc;
	db_thread *thr = (db_thread*)walData;

	Wal *pWal = &thr->curConn->wal;
	pWal->thread = thr;
	MDB_dbi actorsdb = thr->actorsdb, infodb = thr->infodb;
	MDB_txn *txn;

	DBG((g_log,"Wal name=%s\n",zWalName));

	if (mdb_txn_begin(thr->env, NULL, MDB_RDONLY, &txn) != MDB_SUCCESS)
		return SQLITE_ERROR;

	// if (mdb_dbi_open(txn, "actors", MDB_INTEGERKEY, &actorsdb) != MDB_SUCCESS)
	//   return SQLITE_ERROR;
	//
	// if (mdb_dbi_open(txn, "info", MDB_INTEGERKEY, &infodb) != MDB_SUCCESS)
	//   return SQLITE_ERROR;

	key.mv_size = strlen(zWalName);
	key.mv_data = (void*)zWalName;//thr->curConn->dbpath;
	rc = mdb_get(txn,actorsdb,&key,&data);

	// This is new actor, assign an index
	if (rc == MDB_NOTFOUND)
	{
		i64 index = 0;
		MDB_val key1 = {1,(void*)"?"};

		rc = mdb_get(txn,actorsdb,&key1,&data);
		if (rc == MDB_NOTFOUND)
		{
			// this is first actor at index 0
			DBG((g_log,"first actor!\r\n"));
		}
		else if (rc == MDB_SUCCESS)
		{
			// index = *(i64*)data.mv_data;
			memcpy(&index,data.mv_data,sizeof(i64));
			DBG((g_log,"index assigned=%lld\r\n",index));
		}
		else
		{
			mdb_txn_abort(txn);
			return SQLITE_ERROR;
		}

		pWal->index = index++;
		key1.mv_size = 1;
		key1.mv_data = (void*)"?";
		data.mv_size = sizeof(i64);
		data.mv_data = (void*)&index;
		DBG((g_log,"Writing index %lld\r\n",index));
		if (mdb_put(thr->wtxn,actorsdb,&key1,&data,0) != MDB_SUCCESS)
		{
			mdb_txn_abort(txn);
			return SQLITE_ERROR;
		}

		// key is already set to actorname
		data.mv_size = sizeof(i64);
		data.mv_data = (void*)&pWal->index;
		if (mdb_put(thr->wtxn,actorsdb,&key,&data,0) != MDB_SUCCESS)
		{
			mdb_txn_abort(txn);
			return SQLITE_ERROR;
		}
		thr->pagesChanged++;

		thr->forceCommit = 1;
	}
	// Actor exists, read evnum/evterm info
	else if (rc == MDB_SUCCESS)
	{
		// data contains index
		key = data;
		pWal->index = *(i64*)data.mv_data;
		DBG((g_log,"Actor at index %lld\r\n",pWal->index));
		rc = mdb_get(txn,infodb,&key,&data);

		if (rc == MDB_SUCCESS)
		{
			if (*(u8*)data.mv_data != 1)
			{
				mdb_txn_abort(txn);
				return SQLITE_ERROR;
			}
			// pWal->firstCompleteTerm = *(v);
			memcpy(&pWal->firstCompleteTerm, data.mv_data+1, sizeof(i64));
			// pWal->firstCompleteEvnum = *(v+sizeof(i64));
			memcpy(&pWal->firstCompleteEvnum, data.mv_data+1+sizeof(i64), sizeof(i64));
			// pWal->lastCompleteTerm = *(v+sizeof(i64)*2);
			memcpy(&pWal->lastCompleteTerm, data.mv_data+1+sizeof(i64)*2, sizeof(i64));
			// pWal->lastCompleteEvnum = *(v+sizeof(i64)*3);
			memcpy(&pWal->lastCompleteEvnum, data.mv_data+1+sizeof(i64)*3, sizeof(i64));
			// pWal->inProgressTerm = *(v+sizeof(i64)*4);
			memcpy(&pWal->inProgressTerm, data.mv_data+1+sizeof(i64)*4, sizeof(i64));
			// pWal->inProgressEvnum = *(v+sizeof(i64)*5);
			memcpy(&pWal->inProgressEvnum, data.mv_data+1+sizeof(i64)*5, sizeof(i64));
			memcpy(&pWal->mxPage, data.mv_data+1+sizeof(i64)*6,sizeof(u32));

			if (pWal->inProgressTerm != 0)
			{
				// TODO: Delete pages from an incomplete write.
			}
		}
		else if (rc == MDB_NOTFOUND)
		{
			// DBG((g_log,"Info for actor not found\r\n"));
		}
	}
	else
	{
		mdb_txn_abort(txn);
		return SQLITE_ERROR;
	}

	mdb_txn_abort(txn);

	pWal->changed = 1;
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
	DBG((g_log,"Begin read trans %d\n",pWal->changed));
	*pChanged = pWal->changed;
	return SQLITE_OK;
}

void sqlite3WalEndReadTransaction(Wal *pWal)
{
}

/* Read a page from the write-ahead log, if it is present. */
int sqlite3WalFindFrame(Wal *pWal, Pgno pgno, u32 *piRead)
{
	if (pWal->inProgressTerm > 0 || pWal->inProgressEvnum > 0)
		return findframe(pWal, pgno, piRead, pWal->inProgressTerm, pWal->inProgressEvnum);
	else
		return findframe(pWal, pgno, piRead, pWal->lastCompleteTerm, pWal->lastCompleteEvnum);
}

int findframe(Wal *pWal, Pgno pgno, u32 *piRead, u64 limitTerm, u64 limitEvnum)
{
	db_thread *thr = pWal->thread;
	MDB_val key, data;
	int rc;
	u8 pagesKeyBuf[sizeof(i64)+sizeof(u32)];

	DBG((g_log,"FIND FRAME pgno=%u\n",pgno));

	// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Counter,CompressedPage/binary>>}
	memcpy(pagesKeyBuf,               &pWal->index,sizeof(i64));
	memcpy(pagesKeyBuf + sizeof(i64), &pgno,       sizeof(u32));
	key.mv_size = sizeof(pagesKeyBuf);
	key.mv_data = pagesKeyBuf;

	// u32 pgno2 = *(u32*)(key.mv_data+sizeof(i64));
	// DBG((g_log,"RUN %d\n",pgno2));
	rc = mdb_cursor_get(thr->cursorPages,&key,&data,MDB_SET_KEY);
	if (rc == MDB_SUCCESS)
	{
		rc = mdb_cursor_get(thr->cursorPages,&key,&data,MDB_LAST_DUP);
		if (rc == MDB_SUCCESS)
		{
			while (1)
			{
				char frag1 = *(char*)(data.mv_data+sizeof(i64)*2);
				int frag = frag1;
				u64 term, evnum;

				memcpy(&term,  data.mv_data,               sizeof(u64));
				memcpy(&evnum, data.mv_data + sizeof(u64), sizeof(u64));
				if (term > limitTerm || evnum > limitEvnum)
				{
					rc = mdb_cursor_get(thr->cursorPages,&key,&data,MDB_PREV_DUP);
					if (rc == MDB_SUCCESS)
						continue;
					else
					{
						*piRead = 0;
						break;
					}
				}
				// DBG((g_log,"SUCCESS? %d frag=%d\n",pgno1,frag));
				pWal->nResFrames = frag;
				pWal->resFrames[frag--] = data;

				while (frag >= 0)
				{
					rc = mdb_cursor_get(thr->cursorPages,&key,&data,MDB_PREV_DUP);
					frag = *(u8*)(data.mv_data+sizeof(i64)*2);
					DBG((g_log,"SUCCESS? %d frag=%d\n",pgno,frag));
					pWal->resFrames[frag--] = data;
				}
				*piRead = 1;
				break;
			}
		}
		else
			*piRead = 0;
	}
	else if (rc == MDB_NOTFOUND)
	{
		DBG((g_log,"Frame not found!\r\n"));
		*piRead = 0;
	}
	else
	{
		DBG((g_log,"Error?! %d\r\n",rc));
		*piRead = 0;
	}
	return SQLITE_OK;
}

int sqlite3WalReadFrame(Wal *pWal, u32 iRead, int nOut, u8 *pOut)
{
	DBG((g_log,"Read frame\n"));
	// i64 term, evnum;
	if (pWal->nResFrames == 0)
	{
		if (LZ4_decompress_safe((char*)(pWal->resFrames[0].mv_data+sizeof(i64)*2+1),(char*)pOut,
							  pWal->resFrames[0].mv_size-(sizeof(i64)*2+1),nOut) > 0)
		{
	#ifdef _TESTDBG_
			{
				i64 term, evnum;
				memcpy(&term,  pWal->resFrames[0].mv_data,             sizeof(i64));
				memcpy(&evnum, pWal->resFrames[0].mv_data+sizeof(i64), sizeof(i64));
				DBG((g_log,"Term=%lld, evnum=%lld, framesize=%d\n",term,evnum,(int)pWal->resFrames[0].mv_size));
			}
	#endif
			return SQLITE_OK;
		}
	}
	else
	{
		u8 pagesBuf[PAGE_BUFF_SIZE];
		int frags = pWal->nResFrames;
		int pos = 0;

		while (frags >= 0)
		{
			// DBG((g_log,"Read frame %d\n",pos));
			memcpy(pagesBuf + pos, pWal->resFrames[frags].mv_data+sizeof(i64)*2+1, pWal->resFrames[frags].mv_size-(sizeof(i64)*2+1));
			pos += pWal->resFrames[frags].mv_size-(sizeof(i64)*2+1);
			frags--;
		}
		pWal->nResFrames = 0;

		if (LZ4_decompress_safe((char*)pagesBuf,(char*)pOut,pos,nOut) > 0)
			return SQLITE_OK;
	}
	return SQLITE_ERROR;
}

/* If the WAL is not empty, return the size of the database. */
Pgno sqlite3WalDbsize(Wal *pWal)
{
	if (pWal)
	{
		DBG((g_log,"Dbsize %u\n",pWal->mxPage));
		return pWal->mxPage;
	}
	DBG((g_log,"Dbsize 0\n"));
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

// return number of bytes written
int iterate(Wal *pWal, iterate_resource *iter, u8 *buf, int bufsize, u32 *done)
{
	// MDB_val logKey, logVal;
	// MDB_val pgKey, pgVal;
	// db_thread *thr = pWal->thread;
	// u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
	// int pgop;
	int bufused = 0;

	// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
	// Delete from
	// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Count,CompressedPage/binary>>}
	if (!iter->started)
	{
		if (iter->evnum + iter->evterm == 0)
		{
			// If any writes come after iterator started, we must ignore those pages.
			iter->evnum = pWal->lastCompleteEvnum;
			iter->evterm = pWal->lastCompleteTerm;
			iter->pgnoPos = 1;
			iter->entiredb = 1;
            iter->mxPage = pWal->mxPage;
		}
		iter->started = 1;
	}

	// send entire db (without history)
	if (iter->entiredb)
	{
		u32 iRead = 0;

		while (1)
		{
			const int hsz = (sizeof(i64)*2+1);
			findframe(pWal, iter->pgnoPos, &iRead, iter->evterm, iter->evnum);

			if (!iRead)
			{
				*done = iter->mxPage;
				return bufused;
			}


			// page is compressed into single frame
			if (pWal->nResFrames == 0)
			{
				const int pagesz = pWal->resFrames[0].mv_size - hsz;

				if (bufsize < bufused+pagesz+2)
					return bufused;
				write16bit((char*)buf+bufused, pagesz);
				memcpy(buf+bufused+2,pWal->resFrames[0].mv_data + hsz, pagesz);
				bufused += pagesz+2;
			}
			else
			{
				int frags = pWal->nResFrames;
				int frsize = 0;
				while (frags >= 0)
				{
					frsize += pWal->resFrames[frags].mv_size-hsz;
					frags--;
				}
				if (bufsize < bufused+frsize+2)
					return bufused;

				frags = pWal->nResFrames;
				frsize = 0;
				while (frags >= 0)
				{
					const int pagesz = pWal->resFrames[frags].mv_size - hsz;
					write16bit((char*)buf+bufused, pagesz);
					memcpy(buf+bufused+2, pWal->resFrames[frags].mv_data + hsz, pagesz);
					bufused += pagesz+2;
				}

				pWal->nResFrames = 0;
			}
			iter->pgnoPos++;
		}
	}
	else
	{
		// memcpy(pagesKeyBuf,               &pWal->index,  sizeof(u64));
		// memcpy(pagesKeyBuf + sizeof(i64), &iter->pgnoPos,sizeof(u32));
		// pgKey.mv_data = pagesKeyBuf;
		// pgKey.mv_size = sizeof(pagesKeyBuf);
		//
		// if (mdb_cursor_get(thr->cursorPages,&pgKey,&pgVal,MDB_SET) != MDB_SUCCESS)
		// {
		//     return SQLITE_ERROR;
		// }
		// pgop = MDB_LAST_DUP;
	}
	return SQLITE_OK;
}

// Delete all pages up to limitEvterm and limitEvnum
int checkpoint(Wal *pWal, u64 limitEvterm, u64 limitEvnum)
{
	MDB_val logKey, logVal;
	MDB_val pgKey, pgVal;
	u8 logKeyBuf[sizeof(u64)*3];
	u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
	db_thread *thr = pWal->thread;
	int logop, pgop, mrc;
	u64 evnum,evterm,aindex;

	if (pWal->inProgressTerm == 0)
		return SQLITE_OK;

	logKey.mv_data = logKeyBuf;
	logKey.mv_size = sizeof(logKeyBuf);

	DBG((g_log,"checkpoint\r\n"));

	memcpy(logKeyBuf,                 &pWal->index,          sizeof(u64));
	memcpy(logKeyBuf + sizeof(i64),   &pWal->firstCompleteTerm, sizeof(u64));
	memcpy(logKeyBuf + sizeof(i64)*2, &pWal->firstCompleteEvnum,sizeof(u64));

	if (mdb_cursor_get(thr->cursorLog,&logKey,&logVal,MDB_SET) != MDB_SUCCESS)
	{
		DBG((g_log,"Key not found in log for undo\n"));
		return SQLITE_OK;
	}

	while (pWal->firstCompleteTerm < limitEvterm || pWal->firstCompleteEvnum < limitEvnum)
	{
		// For every page here
		// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
		// Delete from
		// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Count,CompressedPage/binary>>}
		logop = MDB_FIRST_DUP;
		while ((mrc = mdb_cursor_get(thr->cursorLog,&logKey,&logVal,logop)) == MDB_SUCCESS)
		{
			u32 pgno;
			memcpy(&pgno,logVal.mv_data,sizeof(u32));
			memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
			memcpy(pagesKeyBuf + sizeof(i64), &pgno,       sizeof(u32));
			pgKey.mv_data = pagesKeyBuf;
			pgKey.mv_size = sizeof(pagesKeyBuf);

			pgop = MDB_FIRST_DUP;
			if (mdb_cursor_get(thr->cursorPages,&pgKey,&pgVal,MDB_SET) != MDB_SUCCESS)
			{
				continue;
			}
			while (mdb_cursor_get(thr->cursorPages,&pgKey,&pgVal,pgop) == MDB_SUCCESS)
			{
				memcpy(&evterm, pgVal.mv_data,            sizeof(u64));
				memcpy(&evnum,  pgVal.mv_data+sizeof(i64),sizeof(u64));
				// DBG((g_log,"progress term %lld, progress evnum %lld, curterm %lld, curnum %lld\n",
				//   pWal->inProgressTerm, pWal->inProgressEvnum, term, evnum));
				if (evterm < limitEvterm || evnum < limitEvnum)
				{
					// DBG((g_log,"DELETING!\n"));
					mdb_cursor_del(thr->cursorPages,0);
				}
				else
				{
					break;
				}

				pgop = MDB_NEXT_DUP;
			}

			logop = MDB_NEXT_DUP;
		}
		if (mdb_cursor_del(thr->cursorLog,MDB_NODUPDATA) != MDB_SUCCESS)
		{
			DBG((g_log,"Unable to cleanup key from logdb\n"));
		}
		if (mdb_cursor_get(thr->cursorLog,&logKey,&logVal,MDB_NEXT) != MDB_SUCCESS)
		{
			DBG((g_log,"Unable to move to next log\n"));
			break;
		}
		memcpy(&aindex, logKey.mv_data,                 sizeof(u64));
		memcpy(&evterm, logKey.mv_data + sizeof(u64),   sizeof(u64));
		memcpy(&evnum,  logKey.mv_data + sizeof(u64)*2, sizeof(u64));

		if (aindex != pWal->index)
		{
			DBG((g_log,"Reached another actor\n"));
			break;
		}
		pWal->firstCompleteTerm = evterm;
		pWal->firstCompleteEvnum = evnum;
	}

	// no dirty pages, but will write info
	sqlite3WalFrames(pWal, SQLITE_DEFAULT_PAGE_SIZE, NULL, pWal->mxPage, 1, 0);

	return SQLITE_OK;
}

/* Undo any frames written (but not committed) to the log */
int sqlite3WalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx)
{
	MDB_val logKey, logVal;
	// MDB_val pgKey, pgVal;
	u8 logKeyBuf[sizeof(u64)*3];
	// u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
	db_thread *thr = pWal->thread;
	int logop, rc = SQLITE_OK, mrc;
	// u64 term,evnum;

	if (pWal->inProgressTerm == 0)
		return SQLITE_OK;

	logKey.mv_data = logKeyBuf;
	logKey.mv_size = sizeof(logKeyBuf);

	DBG((g_log,"Undo\r\n"));

	// For every page here
	// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
	// Delete from
	// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Count,CompressedPage/binary>>}
	memcpy(logKeyBuf,                 &pWal->index,          sizeof(i64));
	memcpy(logKeyBuf + sizeof(i64),   &pWal->inProgressTerm, sizeof(i64));
	memcpy(logKeyBuf + sizeof(i64)*2, &pWal->inProgressEvnum,sizeof(i64));

	if (mdb_cursor_get(thr->cursorLog,&logKey,&logVal,MDB_SET) != MDB_SUCCESS)
	{
		DBG((g_log,"Key not found in log for undo\n"));
		return SQLITE_OK;
	}
	logop = MDB_FIRST_DUP;
	while ((mrc = mdb_cursor_get(thr->cursorLog,&logKey,&logVal,logop)) == MDB_SUCCESS)
	{
		u32 pgno;
		memcpy(&pgno,logVal.mv_data,sizeof(u32));

		// memcpy(pagesKeyBuf,               &pWal->index,sizeof(i64));
		// memcpy(pagesKeyBuf + sizeof(i64), &pgno,       sizeof(u32));
		// pgKey.mv_data = pagesKeyBuf;
		// pgKey.mv_size = sizeof(pagesKeyBuf);
		//
		// // DBG((g_log,"UNDO pgno=%d\n",pgno));
		//
		// pgop = MDB_LAST_DUP;
		// if (mdb_cursor_get(thr->cursorPages,&pgKey,&pgVal,MDB_SET) != MDB_SUCCESS)
		// {
		// 	// DBG((g_log,"Key not found in log for undo\n"));
		// 	continue;
		// }
		// while (mdb_cursor_get(thr->cursorPages,&pgKey,&pgVal,pgop) == MDB_SUCCESS)
		// {
		// 	memcpy(&term, pgVal.mv_data,            sizeof(i64));
		// 	memcpy(&evnum,pgVal.mv_data+sizeof(i64),sizeof(i64));
		// 	// DBG((g_log,"progress term %lld, progress evnum %lld, curterm %lld, curnum %lld\n",
		// 	//   pWal->inProgressTerm, pWal->inProgressEvnum, term, evnum));
		// 	if (term >= pWal->inProgressTerm && evnum >= pWal->inProgressEvnum)
		// 	{
		//         // Probably not necessary, just abort txn???
		// 		mdb_cursor_del(thr->cursorPages,0);
		// 	}
		// 	else
		// 	{
		// 		// Pages are sorted so we can break if term/evnum is smaller
		// 		// This while should only execute once.
		// 		break;
		// 	}
		//
		// 	pgop = MDB_PREV_DUP;
		// }
		if (xUndo)
			rc = xUndo(pUndoCtx, pgno);

		logop = MDB_NEXT_DUP;
	}
	// if (mdb_cursor_del(thr->cursorLog,MDB_NODUPDATA) != MDB_SUCCESS)
	// {
	// 	DBG((g_log,"Unable to cleanup key from logdb\n"));
	// }

	// DBG((g_log,"Undo done!\n"));
	pWal->inProgressTerm = pWal->inProgressEvnum = 0;

  return rc;
}

/* Return an integer that records the current (uncommitted) write
** position in the WAL */
void sqlite3WalSavepoint(Wal *pWal, u32 *aWalData)
{
	DBG((g_log,"SAVEPOINT\n"));
}

/* Move the write position of the WAL back to iFrame.  Called in
** response to a ROLLBACK TO command. */
int sqlite3WalSavepointUndo(Wal *pWal, u32 *aWalData)
{
	DBG((g_log,"SAVEPOINT UNDO\n"));
	return SQLITE_OK;
}

/* Write a frame or frames to the log. */
int sqlite3WalFrames(Wal *pWal, int szPage, PgHdr *pList, Pgno nTruncate, int isCommit, int sync_flags)
{
	PgHdr *p;
	db_thread *thr = pWal->thread;
	// db_connection *pCon = thr->curConn;
	MDB_val key, data;
	int rc;

	key.mv_size = sizeof(i64);
	key.mv_data = (void*)&pWal->index;

	// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Fragment,CompressedPage/binary>>}
	for(p=pList; p; p=p->pDirty)
	{
		u8 pagesKeyBuf[sizeof(i64)+sizeof(u32)];
		u8 pagesBuf[PAGE_BUFF_SIZE];
		int full_size = 0;
		int page_size = LZ4_compress_default((char*)p->pData,(char*)pagesBuf+sizeof(i64)*2+1,szPage,sizeof(pagesBuf));
		char fragment_index = 0;
		int skipped = 0;

		DBG((g_log,"Insert frame actor=%lld, pgno=%u, term=%lld, evnum=%lld, commit=%d, truncate=%d, compressedsize=%d\n",
		pWal->index,p->pgno,pWal->inProgressTerm,pWal->inProgressEvnum,isCommit,nTruncate,page_size));

		memcpy(pagesKeyBuf,               &pWal->index,sizeof(i64));
		memcpy(pagesKeyBuf + sizeof(i64), &p->pgno,    sizeof(u32));
		key.mv_size = sizeof(pagesKeyBuf);
		key.mv_data = pagesKeyBuf;

		memcpy(pagesBuf,               &pWal->inProgressTerm,  sizeof(i64));
		memcpy(pagesBuf + sizeof(i64), &pWal->inProgressEvnum, sizeof(i64));

		full_size = page_size + sizeof(i64)*2 + 1;
		if (full_size < thr->maxvalsize)
			fragment_index = 0;
		else
		{
			full_size = page_size;
			skipped = thr->maxvalsize - sizeof(i64)*2 - 1;
			full_size -= skipped;
			while(full_size > 0)
			{
				full_size -= (thr->maxvalsize - sizeof(i64)*2 - 1);
				fragment_index++;
			}
			full_size = page_size + sizeof(i64)*2 +1;
		}

		pagesBuf[sizeof(i64)*2] = fragment_index;
		data.mv_size = fragment_index == 0 ? full_size : thr->maxvalsize;
		data.mv_data = pagesBuf;

		if ((rc = mdb_cursor_put(thr->cursorPages,&key,&data,0)) != MDB_SUCCESS)
		{
			// printf("Cursor put failed to pages %d\n",rc);
			DBG((g_log,"CURSOR PUT FAILED: %d, datasize=%d\r\n",rc,full_size));
			return SQLITE_ERROR;
		}

		fragment_index--;
		skipped = data.mv_size;
		while (fragment_index >= 0)
		{
			DBG((g_log,"Insert fragment %d\n",(int)fragment_index));
			if (fragment_index == 0)
				data.mv_size = full_size - skipped + sizeof(i64)*2 + 1;
			else
				data.mv_size = thr->maxvalsize;
			data.mv_data = pagesBuf + skipped - (sizeof(i64)*2+1);
			memcpy(pagesBuf + skipped - (sizeof(i64)*2+1), &pWal->inProgressTerm,  sizeof(i64));
			memcpy(pagesBuf + skipped - (sizeof(i64)+1),   &pWal->inProgressEvnum, sizeof(i64));
			pagesBuf[skipped-1] = fragment_index;

			if ((rc = mdb_cursor_put(thr->cursorPages,&key,&data,0)) != MDB_SUCCESS)
			{
				DBG((g_log,"CURSOR secondary PUT FAILED: err=%d, datasize=%d, skipped=%d, frag=%d\r\n",
				rc,full_size, skipped, (int)fragment_index));
				return SQLITE_ERROR;
			}
			skipped += data.mv_size - sizeof(i64)*2 - 1;
			fragment_index--;
		}

		thr->pagesChanged++;
	}
	// printf("\n");
	// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
	for(p=pList; p; p=p->pDirty)
	{
		u8 logKeyBuf[sizeof(i64)*3];

		memcpy(logKeyBuf,                 &pWal->index,           sizeof(i64));
		memcpy(logKeyBuf + sizeof(i64),   &pWal->inProgressTerm,  sizeof(i64));
		memcpy(logKeyBuf + sizeof(i64)*2, &pWal->inProgressEvnum, sizeof(i64));
		key.mv_size = sizeof(logKeyBuf);
		key.mv_data = logKeyBuf;

		data.mv_size = sizeof(u32);
		data.mv_data = &p->pgno;

		if (mdb_cursor_put(thr->cursorLog,&key,&data,0) != MDB_SUCCESS)
		{
			// printf("Cursor put failed to log\n");
			DBG((g_log,"CURSOR PUT TO LOG FAILED: %d\r\n",rc));
			return SQLITE_ERROR;
		}
	}
  /** - Info DB: {<<ActorIndex:64>>, <<V,FirstCompleteTerm:64,FirstCompleteEvnum:64,
										LastCompleteTerm:64,LastCompleteEvnum:64,
										InprogressTerm:64,InProgressEvnum:64>>} */
	{
		u8 infoBuf[1+6*sizeof(i64)+sizeof(u32)];
		if (isCommit)
		{
			pWal->lastCompleteTerm = pWal->inProgressTerm;
			pWal->lastCompleteEvnum = pWal->inProgressEvnum;
			pWal->inProgressTerm = pWal->inProgressEvnum = 0;
			pWal->mxPage = nTruncate;
			pWal->changed = 0;
		}
		else
		{
			pWal->changed = 1;
		}

		infoBuf[0] = 1;
		memcpy(infoBuf+1,               &pWal->firstCompleteTerm,sizeof(i64));
		memcpy(infoBuf+1+sizeof(i64),   &pWal->firstCompleteEvnum,sizeof(i64));
		memcpy(infoBuf+1+sizeof(i64)*2, &pWal->lastCompleteTerm,sizeof(i64));
		memcpy(infoBuf+1+sizeof(i64)*3, &pWal->lastCompleteEvnum,sizeof(i64));
		memcpy(infoBuf+1+sizeof(i64)*4, &pWal->inProgressTerm,sizeof(i64));
		memcpy(infoBuf+1+sizeof(i64)*5, &pWal->inProgressEvnum,sizeof(i64));
		memcpy(infoBuf+1+sizeof(i64)*6, &pWal->mxPage,sizeof(u32));

		key.mv_size = sizeof(i64);
		key.mv_data = &pWal->index;
		data.mv_size = sizeof(infoBuf);
		data.mv_data = infoBuf;
		if (mdb_cursor_put(thr->cursorInfo,&key,&data,0) != MDB_SUCCESS)
		{
			// printf("Cursor put failed to info\n");
			DBG((g_log,"CURSOR PUT TO INFO FAILED: %d\r\n",rc));
			return SQLITE_ERROR;
		}
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
	DBG((g_log,"Checkpoint\n"));
	return SQLITE_OK;
}



/* Return the value to pass to a sqlite3_wal_hook callback, the
** number of frames in the WAL at the point of the last commit since
** sqlite3WalCallback() was called.  If no commits have occurred since
** the last call, then return 0.
*/
int sqlite3WalCallback(Wal *pWal)
{
	DBG((g_log,"Callback\n"));
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
	DBG((g_log,"heap\n"));
	return pWal != NULL;
}





// New function. It adds thread pointer to wal structure.
SQLITE_API int sqlite3_wal_data(
  sqlite3 *db,
  void *pArg
  ){

	int rt = SQLITE_NOTFOUND;
	int i;
	for(i=0; i<db->nDb; i++)
	{
		Btree *pBt = db->aDb[i].pBt;
		if( pBt )
		{
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
