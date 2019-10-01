/* ActorDB -> sqlite -> lz4 -> REPLICATION pipe -> LMDB
** WAL is where sqlite ends and lmdb starts.

** LMDB schema:
** - Actors DB: {<<ActorName/binary>>, <<ActorIndex:64>>}
**   {"?",MaxInteger} -> when adding actors, increment this value

** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,FragIndex:8,CompressedPage/binary>>}
**   Pages db is a dupsort database. It stores lz4 compressed sqlite pages. There can be multiple
**   pages for one pgno. This is to leave room for replication.
**   When a page is requested from sqlite, it will use the highest commited page.
**   Once replication has run its course, old pages are deleted.
**   Pages that are too large to be placed in a single value are added into multiple dupsort values. FragIndex
**   counts down. If there are 3 pages, first frag will be FragIndex=2.

** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
**   Also a dupsort. Every key is one sqlite write transaction. Values are a list of pages
**   that have changed.

** - Info DB: {<<ActorIndex:64>>, <<V,FirstCompleteTerm:64,FirstCompleteEvnum:64,
			LastCompleteTerm:64,LastCompleteEvnum:64,
			InprogressTerm:64,InProgressEvnum:64, DbSize:32,
			CurrentTerm:64,VFSize:8,VotedFor:VFSize/binary>>}
**   V (version) = 1
**   FirstComplete(term/evnum) - First entry for actor in Log DB.
**   LastComplete(term/evnum) - Last entry in log that is commited.
**   InProgress(term/evnum) - pages from this evnum+evterm combination are not commited. If actor has just opened
**                            and it has these values set, it must delete pages to continue.
**   CurrentTerm - raft parameter. This stores the highest term actor has seen
**   VotedFor - which node actor voted for

** On writes log, pages and info db are updated.
** Non-live replication is simply a matter of looking up log and sending the right pages.
** If stale replication or actor copy, simply traverse actor pages from 0 forward until reaching
** the end.

** Undo is a matter of checking the pages of last write in log db and deleting them in log and pages db.

** Endianess: Data is written as is. Practicaly no relevant platforms are in big endian and I can't see
** a scenario where a lmdb file would be moved between different endian platforms.

** Vacuum is not (and should) not be used. A DB that shrinks poses problems for replication.
** Replication completes with nTruncate set to mx page that we have replicated. This may be
** actual mxPage but will usually not be. If DB shrank with a write, we don't know if nTruncate is
** actually new mxPage or not. With regular write nTruncate will always be mxPage.
*/
static int checkpoint(Wal *pWal, u64 evnum);
static int findframe(db_thread *thr, Wal *pWal, Pgno pgno, u32 *piRead, u64 limitTerm, u64 limitEvnum, u64 *outTerm, u64 *outEvnum);
static int storeinfo(Wal *pWal, u64 currentTerm, u8 votedForSize, u8 *votedFor);
static int doundo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx, u8 delPages);
static u64 get8byte(u8* buf);
static void put8byte(u8* buf, u64 num);
static MDB_txn* open_txn(mdbinf *data, int flags);
static int register_actor(u64 index, char *name);

// 1. Figure out actor index, create one if it does not exist
// 2. check info for evnum/evterm data
int sqlite3WalOpen(sqlite3_vfs *pVfs, sqlite3_file *pDbFd, const char *zWalName,int bNoShm, i64 mxWalSize, Wal **ppWal)
{
	MDB_val key, data;
	int rc;

	db_thread *thr 		= g_tsd_thread;
	db_connection *conn = g_tsd_conn;
	mdbinf * const mdb 	= &thr->mdb;
	Wal *pWal = &conn->wal;
	MDB_dbi actorsdb, infodb;
	MDB_txn *txn = mdb->txn;
	int offset = 0, cutoff = 0, nmLen = 0;

	if (!thr)
		return SQLITE_ERROR;

	actorsdb = mdb->actorsdb;
	infodb = mdb->infodb;

	if (zWalName[0] == '/')
		offset = 1;
	nmLen = strlen(zWalName+offset);
	if (zWalName[offset+nmLen-1] == 'l' && zWalName[offset+nmLen-2] == 'a' &&
		zWalName[offset+nmLen-3] == 'w' && zWalName[offset+nmLen-4] == '-')
		cutoff = 4;

	DBG("Wal name=%s %lld",zWalName,(i64)txn);

	// shorten size to ignore "-wal" at the end
	key.mv_size = nmLen-cutoff;
	key.mv_data = (void*)(zWalName+offset);//thr->curConn->dbpath;
	rc = mdb_get(txn,actorsdb,&key,&data);

	// This is new actor, assign an index
	if (rc == MDB_NOTFOUND)
	{
		i64 index = 0;
		// MDB_val key1 = {1,(void*)"?"};
		#ifndef _TESTAPP_
		qitem *item;
		db_command *cmd;

		item = command_create(conn->wthreadind,-1,g_pd);
		cmd = (db_command*)item->cmd;
		cmd->type = cmd_actorsdb_add;

		index = atomic_fetch_add_explicit(&g_pd->actorIndexes[thr->nEnv], 1, memory_order_relaxed);
		pWal->index = index;

		cmd->arg = enif_make_string(item->env,zWalName,ERL_NIF_LATIN1);
		cmd->arg1 = enif_make_uint64(item->env, index);
		push_command(conn->wthreadind, -1, g_pd, item);

		#else
		if (thr->isreadonly)
		{
			return SQLITE_ERROR;
		}
		else
		{
			char filename[MAX_PATHNAME];
			sprintf(filename,"%.*s",(int)(nmLen-cutoff),zWalName+offset);
			index = atomic_fetch_add_explicit(&g_pd->actorIndexes[thr->nEnv], 1, memory_order_relaxed);
			pWal->index = index;
			if (register_actor(index, filename) != SQLITE_OK)
				return SQLITE_ERROR;
		}
		#endif
	}
	// Actor exists, read evnum/evterm info
	else if (rc == MDB_SUCCESS)
	{
		// data contains index
		key = data;
		pWal->index = *(i64*)data.mv_data;
		DBG("Actor at index %lld",pWal->index);
		rc = mdb_get(txn,infodb,&key,&data);

		if (rc == MDB_SUCCESS)
		{
			if (*(u8*)data.mv_data != 1)
			{
				return SQLITE_ERROR;
			}
			memcpy(&pWal->firstCompleteTerm, ((u8*)data.mv_data)+1, sizeof(u64));
			memcpy(&pWal->firstCompleteEvnum,((u8*)data.mv_data)+1+sizeof(u64), sizeof(u64));
			memcpy(&pWal->lastCompleteTerm,  ((u8*)data.mv_data)+1+sizeof(u64)*2, sizeof(u64));
			memcpy(&pWal->lastCompleteEvnum, ((u8*)data.mv_data)+1+sizeof(u64)*3, sizeof(u64));
			memcpy(&pWal->inProgressTerm,    ((u8*)data.mv_data)+1+sizeof(u64)*4, sizeof(u64));
			memcpy(&pWal->inProgressEvnum,   ((u8*)data.mv_data)+1+sizeof(u64)*5, sizeof(u64));
			memcpy(&pWal->mxPage,   ((u8*)data.mv_data)+1+sizeof(u64)*6,            sizeof(u32));
			memcpy(&pWal->allPages, ((u8*)data.mv_data)+1+sizeof(u64)*6+sizeof(u32),sizeof(u32));
			pWal->readSafeTerm = pWal->lastCompleteTerm;
			pWal->readSafeEvnum = pWal->lastCompleteEvnum;
			pWal->readSafeMxPage = pWal->mxPage;

			// if (pWal->inProgressTerm != 0)
			// {
			// 	doundo(pWal,NULL,NULL,1);
			// }
		}
		else if (rc == MDB_NOTFOUND)
		{
			// DBG("Info for actor not found"));
		}
	}
	else
	{
		DBG("Error open=%d",rc);
		thr->forceCommit = 2;
		return SQLITE_ERROR;
	}

	conn->changed = 1;
	if (ppWal != NULL)
		(*ppWal) = pWal;
	return SQLITE_OK;
}

int register_actor(u64 index, char *name)
{
	MDB_val key = {0,NULL}, data = {0, NULL};
	int offset = 0, cutoff = 0, rc;
	mdbinf *mdb;
	size_t nmLen;
	u64 topIndex;
	db_thread *thread = g_tsd_thread;

	DBG("REGISTER ACTOR");

	if (!g_tsd_wmdb)
		lock_wtxn(thread->nEnv);
	mdb = g_tsd_wmdb;
	if (!mdb)
		return SQLITE_ERROR;

	if (name[0] == '/')
		offset = 1;
	nmLen = strlen(name+offset);
	if (name[offset+nmLen-1] == 'l' && name[offset+nmLen-2] == 'a' &&
		name[offset+nmLen-3] == 'w' && name[offset+nmLen-4] == '-')
		cutoff = 4;

	key.mv_size = nmLen-cutoff;
	key.mv_data = (void*)(name+offset);
	data.mv_size = sizeof(u64);
	data.mv_data = (void*)&index;
	DBG("Writing actors index for=%s",name);
	if ((rc = mdb_put(mdb->txn,mdb->actorsdb,&key,&data,0)) != MDB_SUCCESS)
	{
		DBG("Unable to write actor index!! %llu %d", index, rc);
		return SQLITE_ERROR;
	}

	key.mv_size = 1;
	key.mv_data = (void*)"?";

	if (mdb_get(mdb->txn,mdb->actorsdb,&key,&data) == MDB_SUCCESS)
		memcpy(&topIndex,data.mv_data,sizeof(u64));
	else
		topIndex = 0;

	index++;
	if (topIndex < index)
	{
		data.mv_size = sizeof(u64);
		data.mv_data = (void*)&index;

		DBG("Writing ? index %lld",index);
		if (mdb_put(mdb->txn,mdb->actorsdb,&key,&data,0) != MDB_SUCCESS)
		{
			DBG("Unable to write ? index!! %llu", index);
			return SQLITE_ERROR;
		}
	}

	thread->pagesChanged++;
	return SQLITE_OK;
}


int sqlite3WalClose(Wal *pWal,sqlite3* db, int sync_flags, int nBuf, u8 *zBuf)
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
	// db_connection* const conn = enif_tsd_get(g_tsd_conn);
	db_connection* conn = g_tsd_conn;
	*pChanged = conn->changed;
	DBG("Begin read trans %d",*pChanged);
	if (conn->changed)
		conn->changed = 0;
	return SQLITE_OK;
}

void sqlite3WalEndReadTransaction(Wal *pWal)
{
}

/* Read a page from the write-ahead log, if it is present. */
int sqlite3WalFindFrame(Wal *pWal, Pgno pgno, u32 *piRead)
{
	// db_thread * const thread = enif_tsd_get(g_tsd_thread);
	db_thread *thread = g_tsd_thread;
	if (!thread)
		return SQLITE_ERROR;
	/*if (thr->isreadonly)
	{
		u64 readSafeEvnum, readSafeTerm;
		#ifndef _TESTAPP_
		enif_mutex_lock(pWal->mtx);
		#endif
		readSafeEvnum = pWal->rthread->readSafeEvnum;
		readSafeTerm = pWal->rthread->readSafeTerm;
		#ifndef _TESTAPP_
		enif_mutex_unlock(pWal->mtx);
		#endif

		return findframe(pWal->rthread, pWal, pgno, piRead, readSafeTerm, readSafeEvnum, NULL, NULL);
	}
	else*/ if (pWal->inProgressTerm > 0 || pWal->inProgressEvnum > 0)
		return findframe(thread, pWal, pgno, piRead, pWal->inProgressTerm,
			pWal->inProgressEvnum, NULL, NULL);
	else
		return findframe(thread, pWal, pgno, piRead, pWal->lastCompleteTerm,
			pWal->lastCompleteEvnum, NULL, NULL);
}

static int findframe(db_thread *thr, Wal *pWal, Pgno pgno, u32 *piRead, u64 limitTerm,
	u64 limitEvnum, u64 *outTerm, u64 *outEvnum)
{
	MDB_val key, data;
	int rc;
	size_t ndupl = 0;
	u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
	mdbinf *mdb;

	if (thr->pagesChanged)
	{
		mdb = g_tsd_wmdb;
	}
	else
		mdb = &thr->mdb;

	track_time(7,thr);
	DBG("FIND FRAME pgno=%u, index=%llu, limitterm=%llu, limitevnum=%llu",
		pgno,pWal->index,limitTerm,limitEvnum);

	// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>,
	// 			<<Evterm:64,Evnum:64,Counter,CompressedPage/binary>>}
	memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
	memcpy(pagesKeyBuf + sizeof(u64), &pgno,       sizeof(u32));
	key.mv_size = sizeof(pagesKeyBuf);
	key.mv_data = pagesKeyBuf;

	// u32 pgno2 = *(u32*)(key.mv_data+sizeof(u64));
	// DBG("RUN %d",pgno2);
	rc = mdb_cursor_get(mdb->cursorPages,&key,&data,MDB_SET_KEY);
	if (rc == MDB_SUCCESS)
	{
		mdb_cursor_count(mdb->cursorPages,&ndupl);
		if (ndupl == 0)
		{
			*piRead = 0;
			return SQLITE_OK;
		}
		rc = mdb_cursor_get(mdb->cursorPages,&key,&data,MDB_LAST_DUP);
		if (rc == MDB_SUCCESS)
		{
			while (1)
			{
				char frag1 = *(char*)(((char*)data.mv_data)+sizeof(u64)*2);
				int frag = frag1;
				u64 term, evnum;

				memcpy(&term,  data.mv_data,               sizeof(u64));
				memcpy(&evnum, ((u8*)data.mv_data) + sizeof(u64), sizeof(u64));
				if (term > limitTerm || evnum > limitEvnum)
				{
					rc = mdb_cursor_get(mdb->cursorPages,&key,&data,MDB_PREV_DUP);
					if (rc == MDB_SUCCESS)
						continue;
					else
					{
						DBG("Cant move to prev dup, term=%llu, evnum=%llu,"
							" limitterm=%llu, limitevnum=%llu",term,evnum,limitTerm,limitEvnum);
						*piRead = 0;
						break;
					}
				}
				if (outTerm != NULL)
					*outTerm = term;
				if (outEvnum != NULL)
					*outEvnum = evnum;

				DBG("Found page size=%ld, frags=%d",data.mv_size,(int)frag);
				thr->nResFrames = frag;
				thr->resFrames[frag--] = data;

				while (frag >= 0)
				{
					rc = mdb_cursor_get(mdb->cursorPages,&key,&data,MDB_PREV_DUP);
					frag = *(((u8*)data.mv_data)+sizeof(u64)*2);
					// DBG("SUCCESS? %d frag=%d, size=%ld",pgno,frag,data.mv_size);
					thr->resFrames[frag--] = data;
				}
				*piRead = 1;
				break;
			}
		}
		else
		{
			DBG("Find page no last dup");
			*piRead = 0;
		}
	}
	else if (rc == MDB_NOTFOUND)
	{
		DBG("Frame not found!");
		*piRead = 0;
	}
	else
	{
		DBG("ERROR findframe: %d",rc);
		*piRead = 0;
	}
	return SQLITE_OK;
}

static int readframe(Wal *pWal, u32 iRead, int nOut, u8 *pOut)
{
	int result 			  = 0;
	// db_thread * const thr = enif_tsd_get(g_tsd_thread);
	db_thread *thr = g_tsd_thread;

	// #ifndef _WIN32
	// if (pthread_equal(pthread_self(), pWal->rthreadId))
	// #else
	// if (GetCurrentThreadId() == pWal->rthreadId)
	// #endif
	// 	thr = pWal->rthread;
	// else
	// 	thr = pWal->thread;

	DBG("Read frame");
	// i64 term, evnum;
	if (thr->nResFrames == 0)
	{
		result = LZ4_decompress_safe((((char*)thr->resFrames[0].mv_data)+sizeof(u64)*2+1),
			(char*)pOut,
			thr->resFrames[0].mv_size-(sizeof(u64)*2+1),
			nOut);
		#ifdef _TESTDBG_
		if (result > 0)
		{

			{
				i64 term, evnum;
				memcpy(&term,  thr->resFrames[0].mv_data,             sizeof(u64));
				memcpy(&evnum, (u8*)thr->resFrames[0].mv_data+sizeof(u64), sizeof(u64));
				DBG("Term=%lld, evnum=%lld, framesize=%d",
					term,evnum,(int)thr->resFrames[0].mv_size);
			}
		}
		#endif
	}
	else
	{
		u8 pagesBuf[PAGE_BUFF_SIZE];
		int frags = thr->nResFrames;
		int pos = 0;

		while (frags >= 0)
		{
			// DBG("Read frame %d",pos);
			memcpy(pagesBuf + pos,
				((char*)thr->resFrames[frags].mv_data)+sizeof(u64)*2+1,
				thr->resFrames[frags].mv_size-(sizeof(u64)*2+1));
			pos += thr->resFrames[frags].mv_size-(sizeof(u64)*2+1);
			frags--;
		}
		thr->nResFrames = 0;

		result = LZ4_decompress_safe((char*)pagesBuf,(char*)pOut,pos,nOut);
	}
	return result;
}

int sqlite3WalReadFrame(Wal *pWal, u32 iRead, int nOut, u8 *pOut)
{
	if (readframe(pWal,iRead,nOut,pOut) == SQLITE_DEFAULT_PAGE_SIZE)
		return SQLITE_OK;
	else
		return SQLITE_ERROR;
}

/* If the WAL is not empty, return the size of the database. */
Pgno sqlite3WalDbsize(Wal *pWal)
{
	if (pWal)
	{
		DBG("Dbsize %u",pWal->mxPage);
		return pWal->mxPage;
	}
	DBG("Dbsize 0");
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

static int fillbuff(db_thread *thr, Wal *pWal, iterate_resource *iter, u8* buf, int bufsize)
{
	int bufused = 0;
	int frags = thr->nResFrames;
	int frsize = 0;
	const int hsz = (sizeof(u64)*2+1);

	while (frags >= 0)
	{
		frsize += thr->resFrames[frags].mv_size-hsz;
		frags--;
	}

	frags = thr->nResFrames;
	frsize = 0;
	while (frags >= 0)
	{
		const int pagesz = thr->resFrames[frags].mv_size - hsz;

		memcpy(buf+bufused, (u8*)thr->resFrames[frags].mv_data + hsz, pagesz);
		bufused += pagesz;
		frags--;

		DBG("bufused=%d, pagesz=%d",bufused,pagesz);
	}
	thr->nResFrames = 0;

	return bufused;
}

// return number of bytes written
static int wal_iterate(Wal *pWal, iterate_resource *iter, u8 *buf, int bufsize, u8 *hdr, u32 *done)
{
	// db_thread* const thr = enif_tsd_get(g_tsd_thread);
	db_thread *thr 		 = g_tsd_thread;
	mdbinf* const mdb 	 = &thr->mdb;
	u32 mxPage;
	u64 readSafeEvnum, readSafeTerm;

	#ifndef _TESTAPP_
	enif_mutex_lock(pWal->mtx);
	#endif
	readSafeEvnum = pWal->lastCompleteEvnum;
	readSafeTerm = pWal->lastCompleteTerm;
	mxPage = pWal->mxPage;
	#ifndef _TESTAPP_
	enif_mutex_unlock(pWal->mtx);
	#endif

	if (!iter->started)
	{
		if (iter->evnum + iter->evterm == 0)
		{
			// If any writes come after iterator started, we must ignore those pages.
			iter->evnum = readSafeEvnum;
			iter->evterm = readSafeTerm;
			iter->pgnoPos = 1;
			iter->entiredb = 1;
			iter->mxPage = mxPage;
			if (pWal->mxPage == 0)
			{
				DBG("ERROR: Iterate on empty DB %llu",pWal->lastCompleteEvnum);
				*done = 1;
				return 0;
			}
		}
		else
		{
			// set mxPage to highest pgno we find.
			iter->pgnoPos = iter->mxPage = 0;
			DBG("Iterate rsterm=%llu rsevnum=%llu",readSafeTerm,readSafeEvnum);
		}
		iter->started = 1;
	}

	// send entire db (without history)
	if (iter->entiredb)
	{
		u32 iRead = 0;
		findframe(thr, pWal, iter->pgnoPos, &iRead, iter->evterm, iter->evnum, NULL, NULL);

		if (!iRead)
		{
			DBG("Iterate did not find page");
			*done = iter->mxPage;
			return 0;
		}
		DBG("Iter pos=%u, mx=%u, safemx=%u",iter->pgnoPos, iter->mxPage, mxPage);
		if (iter->pgnoPos == iter->mxPage)
			*done = iter->mxPage;
		put8byte(hdr,                           iter->evterm);
		put8byte(hdr+sizeof(u64),               iter->evnum);
		put4byte(hdr+sizeof(u64)*2,             iter->pgnoPos);
		put4byte(hdr+sizeof(u64)*2+sizeof(u32), *done);
		iter->pgnoPos++;
		return fillbuff(thr, pWal, iter, buf, bufsize);
	}
	else
	{
		MDB_val logKey, logVal;
		int logop;
		u8 logKeyBuf[sizeof(u64)*3];
		int rc;
		// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}

		memcpy(logKeyBuf,                 &pWal->index,  sizeof(u64));
		memcpy(logKeyBuf + sizeof(u64),   &iter->evterm, sizeof(u64));
		memcpy(logKeyBuf + sizeof(u64)*2, &iter->evnum, sizeof(u64));
		logKey.mv_data = logKeyBuf;
		logKey.mv_size = sizeof(logKeyBuf);
		DBG("iterate looking for, matchterm=%llu matchevnum=%llu",iter->evterm,iter->evnum);
		if (mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_SET) != MDB_SUCCESS)
		{
			// Evterm/evnum combination not found. Check if evnum is there.
			// If so return evterm. It will mean a node is in conflict.
			DBG("Key not found in log");
			if (readSafeEvnum == iter->evnum)
			{
				iter->evterm = readSafeTerm;
				iter->termMismatch = 1;
			}
			else
			{
				memcpy(logKeyBuf,                 &pWal->index,  sizeof(u64));
				memcpy(logKeyBuf + sizeof(u64),   &readSafeTerm, sizeof(u64));
				memcpy(logKeyBuf + sizeof(u64)*2, &readSafeEvnum,sizeof(u64));
				if (mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_SET) != MDB_SUCCESS)
				{
					DBG("Key not found in log for undo");
					*done = 1;
					return 0;
				}
				while (mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_PREV_NODUP) == MDB_SUCCESS)
				{
					u64 aindex, term, evnum;

					mdb_cursor_get(mdb->cursorLog,&logKey, &logVal, MDB_GET_CURRENT);
					memcpy(&aindex, logKey.mv_data,              sizeof(u64));
					memcpy(&term,   (u8*)logKey.mv_data+sizeof(u64),  sizeof(u64));
					memcpy(&evnum,  (u8*)logKey.mv_data+sizeof(u64)*2,sizeof(u64));

					DBG("Iterate on term=%llu, evnum=%llu, looking for=%llu",term,evnum,iter->evnum);

					if (aindex != pWal->index)
						break;
					if (iter->evnum == evnum)
					{
						iter->evterm = term;
						iter->termMismatch = 1;
						break;
					}
				}
			}

			*done = 1;
			return 0;
		}

		// We start iterate from next evnum not current. Input evterm/evnum is match_index and match_term.
		// It needs next.
		if (iter->started == 1 &&
			(rc = mdb_cursor_get(mdb->cursorLog,&logKey, &logVal, MDB_NEXT_NODUP)) != MDB_SUCCESS)
		{
			*done = 1;
			return 0;
		}
		else
		{
			u64 aindex;

			rc = mdb_cursor_get(mdb->cursorLog,&logKey, &logVal, MDB_GET_CURRENT);
			if (rc != MDB_SUCCESS)
			{
				*done = 1;
				return 0;
			}
			memcpy(&aindex,       (u8*)logKey.mv_data,              sizeof(u64));
			memcpy(&iter->evterm, (u8*)logKey.mv_data+sizeof(u64),  sizeof(u64));
			memcpy(&iter->evnum,  (u8*)logKey.mv_data+sizeof(u64)*2,sizeof(u64));
			if (aindex != pWal->index)
			{
				*done = 1;
				return 0;
			}
			// To keep from moving iter->evterm/iter->evnum forward more than once.
			iter->started = 2;
		}

		logop = MDB_FIRST_DUP;
		while ((rc = mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,logop)) == MDB_SUCCESS)
		{
			u64 evnum,evterm;
			u32 pgno;
			u32 iRead;

			logop = MDB_NEXT_DUP;

			mdb_cursor_get(mdb->cursorLog,&logKey, &logVal, MDB_GET_CURRENT);
			memcpy(&pgno,logVal.mv_data,sizeof(u32));

			DBG("iterate at pgno=%u, pgnopos=%u",pgno,iter->pgnoPos);

			if (pgno <= iter->pgnoPos)
				continue;

			findframe(thr, pWal, pgno, &iRead, iter->evterm, iter->evnum, &evterm, &evnum);

			if (iRead == 0)
			{
				DBG("ERROR: Did not find frame for pgno=%u, evterm=%llu, evnum=%llu",
					pgno, iter->evterm, iter->evnum);
				*done = 1;
				return 0;
			}

			if (evterm != iter->evterm || evnum != iter->evnum)
			{
				DBG("ERROR: Evterm/evnum does not match,looking for: evterm=%llu, evnum=%llu, "
				"got: evterm=%llu, evnum=%llu", iter->evterm, iter->evnum, evterm, evnum);
				*done = 1;
				return 0;
			}

			iter->pgnoPos = pgno;
			if ((rc = mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,logop)) == MDB_SUCCESS)
				*done = 0;
			else
				*done = iter->pgnoPos;
			DBG( "logcursor get next %d, done=%u",rc,*done);
			put8byte(hdr,                           iter->evterm);
			put8byte(hdr+sizeof(u64),               iter->evnum);
			put4byte(hdr+sizeof(u64)*2,             iter->pgnoPos);
			put4byte(hdr+sizeof(u64)*2+sizeof(u32), *done);
			return fillbuff(thr, pWal, iter, buf, bufsize);
		}
		*done = 1;
		return 0;
	}
}

// Delete all pages up to limitEvterm and limitEvnum
static int checkpoint(Wal *pWal, u64 limitEvnum)
{
	MDB_val logKey, logVal;
	u8 logKeyBuf[sizeof(u64)*3];
	u64 evnum,evterm,aindex;
	mdbinf* mdb;

	// db_thread* const thr = enif_tsd_get(g_tsd_thread);
	db_thread *thr 		 = g_tsd_thread;
	int logop, mrc 		 = MDB_SUCCESS;
	// u8 somethingDeleted  = 0;
	int allPagesDiff 	 = 0;

	if (!thr)
		return SQLITE_ERROR;
	if (!g_tsd_wmdb)
		lock_wtxn(thr->nEnv);
	mdb = g_tsd_wmdb;
	if (!mdb)
		return SQLITE_ERROR;

	// if (pWal->inProgressTerm == 0)
	// 	return SQLITE_OK;

	DBG("checkpoint actor=%llu, fct=%llu, fcev=%llu, limitEvnum=%llu",pWal->index,
		pWal->firstCompleteTerm,pWal->firstCompleteEvnum,limitEvnum);

	while (pWal->firstCompleteEvnum < limitEvnum)
	{
		logKey.mv_data = logKeyBuf;
		logKey.mv_size = sizeof(logKeyBuf);
		memcpy(logKeyBuf,                 &pWal->index,          sizeof(u64));
		memcpy(logKeyBuf + sizeof(u64),   &pWal->firstCompleteTerm, sizeof(u64));
		memcpy(logKeyBuf + sizeof(u64)*2, &pWal->firstCompleteEvnum,sizeof(u64));
		if (mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_SET) != MDB_SUCCESS)
		{
			DBG("Key not found in log for checkpoint %llu %llu\r\n",
				pWal->firstCompleteTerm, pWal->firstCompleteEvnum);

			mrc = mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_FIRST);
			if (mrc != MDB_SUCCESS)
				return SQLITE_ERROR;
			while (aindex != pWal->index)
			{
				mrc = mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_NEXT);
				if (mrc != MDB_SUCCESS)
					return SQLITE_ERROR;
				aindex = *(u64*)(logKey.mv_data);
			}
			if (aindex != pWal->index)
				return SQLITE_ERROR;

			memcpy(&evterm, (u8*)logKey.mv_data + sizeof(u64),   sizeof(u64));
			memcpy(&evnum,  (u8*)logKey.mv_data + sizeof(u64)*2, sizeof(u64));
			pWal->firstCompleteTerm = evterm;
			pWal->firstCompleteEvnum = evnum;
		}

		DBG("checkpoint evnum=%llu",pWal->firstCompleteEvnum);
		// For every page here
		// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
		// Delete from
		// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Count,CompressedPage/binary>>}
		logop = MDB_FIRST_DUP;
		while ((mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,logop)) == MDB_SUCCESS)
		{
			u32 pgno;
			size_t ndupl;
			u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
			MDB_val pgKey = {0,NULL}, pgVal = {0,NULL};
			u64 pgnoLimitEvnum;

			logop = MDB_NEXT_DUP;
			memcpy(&pgno, logVal.mv_data,sizeof(u32));

			DBG("checkpoint pgno=%u",pgno);

			memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
			memcpy(pagesKeyBuf + sizeof(u64), &pgno,       sizeof(u32));
			pgKey.mv_data = pagesKeyBuf;
			pgKey.mv_size = sizeof(pagesKeyBuf);

			if (mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,MDB_SET) != MDB_SUCCESS)
			{
				continue;
			}
			mdb_cursor_count(mdb->cursorPages,&ndupl);

			if (mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,MDB_LAST_DUP) == MDB_SUCCESS)
				memcpy(&pgnoLimitEvnum,  (u8*)pgVal.mv_data+sizeof(u64),sizeof(u64));
			else
				continue;

			pgnoLimitEvnum = pgnoLimitEvnum < limitEvnum ? pgnoLimitEvnum : limitEvnum;

			if (mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,MDB_FIRST_DUP) != MDB_SUCCESS)
				continue;

			do
			{
				u8 frag;
				MDB_val pgDelKey = {0,NULL}, pgDelVal = {0,NULL};

				mdb_cursor_get(mdb->cursorPages,&pgDelKey,&pgDelVal,MDB_GET_CURRENT);

				frag = *((u8*)pgDelVal.mv_data+sizeof(u64)*2);
				memcpy(&evterm, pgDelVal.mv_data,            sizeof(u64));
				memcpy(&evnum,  (u8*)pgDelVal.mv_data+sizeof(u64),sizeof(u64));
				DBG("limit limitevnum %lld, curnum %lld, dupl %zu, frag=%d",
					limitEvnum, evnum, ndupl,(int)frag);

				if (evnum < pgnoLimitEvnum)
				{
					mrc = mdb_cursor_del(mdb->cursorPages,0);
					if (mrc != MDB_SUCCESS)
					{
						DBG("Unable to delete page on cursor!.");
						break;
					}
					// else
					// {
					// 	DBG("Deleted page!");
					// 	// somethingDeleted = 1;
					// }
					if (frag == 0)
						allPagesDiff++;
				}

				ndupl--;
				if (!ndupl)
					break;
				mrc = mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,MDB_NEXT_DUP);
			} while (mrc == MDB_SUCCESS);
		}

		mrc = mdb_cursor_del(mdb->cursorLog,MDB_NODUPDATA);
		if (mrc != MDB_SUCCESS)
		{
			DBG("Can not delete log");
			break;
		}

		// move forward
		if ((mrc = mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_NEXT_NODUP)) != MDB_SUCCESS)
		{
			DBG("Unable to move to next log %d",mrc);
			break;
		}

		// read next key data
		memcpy(&aindex, logKey.mv_data,                 sizeof(u64));
		memcpy(&evterm, (u8*)logKey.mv_data + sizeof(u64),   sizeof(u64));
		memcpy(&evnum,  (u8*)logKey.mv_data + sizeof(u64)*2, sizeof(u64));

		if (aindex != pWal->index)
		{
			DBG("Reached another actor");
			break;
		}
		pWal->firstCompleteTerm = evterm;
		pWal->firstCompleteEvnum = evnum;
		pWal->allPages -= allPagesDiff;
		allPagesDiff = 0;
		DBG("Checkpint fce now=%lld",(u64)evnum);
	}

	// no dirty pages, but will write info
	if (sqlite3WalFrames(pWal, SQLITE_DEFAULT_PAGE_SIZE, NULL, pWal->mxPage, 1, 0) == SQLITE_OK)
		return SQLITE_OK;
	else
		return SQLITE_ERROR;
}


static int doundo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx, u8 delPages)
{
	MDB_val logKey, logVal;
	MDB_val pgKey, pgVal;
	u8 logKeyBuf[sizeof(u64)*3];
	int logop, pgop, rc, mrc;
	mdbinf *mdb;

	// db_thread* const thr = enif_tsd_get(g_tsd_thread);
	db_thread *thr = g_tsd_thread;
	rc  = SQLITE_OK;
	if (!thr)
		return SQLITE_OK;

	if (pWal->inProgressTerm == 0)
		return SQLITE_OK;

	if (!g_tsd_wmdb)
		lock_wtxn(thr->nEnv);
	mdb = g_tsd_wmdb;
	if (!mdb)
		return SQLITE_ERROR;

	logKey.mv_data = logKeyBuf;
	logKey.mv_size = sizeof(logKeyBuf);

	DBG("Undo");

	// For every page here
	// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
	// Delete from
	// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Count,CompressedPage/binary>>}
	memcpy(logKeyBuf,                 &pWal->index,          sizeof(u64));
	memcpy(logKeyBuf + sizeof(u64),   &pWal->inProgressTerm, sizeof(u64));
	memcpy(logKeyBuf + sizeof(u64)*2, &pWal->inProgressEvnum,sizeof(u64));

	if (mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_SET) != MDB_SUCCESS)
	{
		DBG("Key not found in log for undo, index=%llu, term=%llu, evnum=%llu",
		pWal->index, pWal->inProgressTerm, pWal->inProgressEvnum);
		return SQLITE_OK;
	}
	logop = MDB_FIRST_DUP;
	while ((mrc = mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,logop)) == MDB_SUCCESS)
	{
		u32 pgno;
		u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
		u64 term,evnum;

		memcpy(&pgno,logVal.mv_data,sizeof(u32));

		if (delPages)
		{
			size_t ndupl;
			memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
			memcpy(pagesKeyBuf + sizeof(u64), &pgno,       sizeof(u32));
			pgKey.mv_data = pagesKeyBuf;
			pgKey.mv_size = sizeof(pagesKeyBuf);

			DBG("UNDO pgno=%d",pgno);

			pgop = MDB_FIRST_DUP;
			if (mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,MDB_SET) != MDB_SUCCESS)
			{
				logop = MDB_NEXT_DUP;
				DBG("Key not found in log for undo");
				continue;
			}
			mdb_cursor_count(mdb->cursorPages,&ndupl);
			while (mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,pgop) == MDB_SUCCESS)
			{
				u8 frag = *((u8*)pgVal.mv_data+sizeof(u64)*2);
				memcpy(&term, pgVal.mv_data,                 sizeof(u64));
				memcpy(&evnum,(u8*)pgVal.mv_data+sizeof(u64),sizeof(u64));
				DBG("progress term %lld, progress evnum %lld, curterm %lld, curnum %lld",
				  pWal->inProgressTerm, pWal->inProgressEvnum, term, evnum);
				if (term >= pWal->inProgressTerm && evnum >= pWal->inProgressEvnum)
				{
					if (mdb_cursor_del(mdb->cursorPages,0) != MDB_SUCCESS)
					{
						DBG("Can not delete undo");
						rc = SQLITE_ERROR;
						break;
					}
					if (frag == 0)
						pWal->allPages--;
					ndupl--;
					if (!ndupl)
						break;
				}

				pgop = MDB_NEXT_DUP;
			}
			pWal->inProgressTerm = pWal->inProgressEvnum = 0;
			storeinfo(pWal,0,0,NULL);
			thr->pagesChanged++;
		}

		if (xUndo)
			rc = xUndo(pUndoCtx, pgno);

		logop = MDB_NEXT_DUP;
	}
	// if (mdb_cursor_del(mdb->cursorLog,MDB_NODUPDATA) != MDB_SUCCESS)
	// {
	// 	DBG("Unable to cleanup key from logdb"));
	// }

	DBG("Undo done!");

	return rc;
}

/* Undo any frames written (but not committed) to the log */
int sqlite3WalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx)
{
	DBG("sqlite3WalUndo");
	return doundo(pWal,xUndo, pUndoCtx,1);
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

static int wal_rewind_int(mdbinf *mdb, Wal *pWal, u64 limitEvnum)
{
	MDB_val logKey, logVal;
	u8 logKeyBuf[sizeof(u64)*3];
	int allPagesDiff = 0;
	int logop, rc;
	u64 evnum,evterm,aindex;

	// u8 somethingDeleted = 0;
	memcpy(logKeyBuf,                 &pWal->index,          sizeof(u64));
	memcpy(logKeyBuf + sizeof(u64),   &pWal->lastCompleteTerm, sizeof(u64));
	memcpy(logKeyBuf + sizeof(u64)*2, &pWal->lastCompleteEvnum,sizeof(u64));

	logKey.mv_data = logKeyBuf;
	logKey.mv_size = sizeof(logKeyBuf);

	if ((rc = mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_SET)) != MDB_SUCCESS)
	{
		DBG("Key not found in log for rewind %llu %llu",
			pWal->lastCompleteTerm,pWal->lastCompleteEvnum);
		return SQLITE_OK;
	}

	while (pWal->lastCompleteEvnum >= limitEvnum)
	{
		// mdb_cursor_count(thr->cursorLog,&ndupl);
		// For every page here
		// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
		// Delete from
		// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Count,CompressedPage/binary>>}
		logop = MDB_LAST_DUP;
		while ((rc = mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,logop)) == MDB_SUCCESS)
		{
			u32 pgno;
			// u8 rewrite = 0;
			u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
			MDB_val pgKey, pgVal;
			size_t ndupl;
			// size_t rewritePos = 0;
			int pgop;

			memcpy(&pgno, logVal.mv_data,sizeof(u32));
			DBG("Moving to pgno=%u, evnum=%llu",pgno,pWal->lastCompleteEvnum);

			memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
			memcpy(pagesKeyBuf + sizeof(u64), &pgno,       sizeof(u32));
			pgKey.mv_data = pagesKeyBuf;
			pgKey.mv_size = sizeof(pagesKeyBuf);

			logop = MDB_PREV_DUP;
			if (mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,MDB_SET) != MDB_SUCCESS)
			{
				continue;
			}
			mdb_cursor_count(mdb->cursorPages,&ndupl);
			if (ndupl == 0)
				continue;
			if (mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,MDB_LAST_DUP) != MDB_SUCCESS)
				continue;
			pgop = MDB_PREV_DUP;
			do
			{
				u8 frag;
				MDB_val pgDelKey = {0,NULL}, pgDelVal = {0,NULL};

				if (mdb_cursor_get(mdb->cursorPages,&pgDelKey,&pgDelVal,MDB_GET_CURRENT) != MDB_SUCCESS)
					break;
				frag = *((u8*)pgDelVal.mv_data+sizeof(u64)*2);
				memcpy(&evnum,  (u8*)pgDelVal.mv_data+sizeof(u64),sizeof(u64));
				DBG("Deleting pgno=%u, evnum=%llu",pgno,evnum);
				if (evnum >= limitEvnum)
				{
					// Like checkpoint, we can not trust this will succeed.
					rc = mdb_cursor_del(mdb->cursorPages,0);
					if (rc != MDB_SUCCESS)
					{
						DBG("Unable to delete rewind page!!!");
						break;
					}
					// else
					// {
					// 	// This is normal operation. Delete page and set flag
					// 	// that something is deleted.
					// 	DBG("Rewind page deleted!");
					// 	somethingDeleted = 1;
					// }
					if (frag == 0)
						allPagesDiff++;
				}
				else
				{
					// No ugliness happened. Either there is nothing to delete or
					// we deleted a few pages off the top and we are done.
					break;
				}
				ndupl--;
				if (!ndupl)
					break;
				rc = mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,pgop);
			} while (rc == MDB_SUCCESS);
			DBG("Done looping pages %d",rc);
			// If we moved through all pages and rewrite did not happen
			// and this is last page, we have shrunk DB.
			if (!ndupl && pgno == pWal->mxPage)
				pWal->mxPage--;
		}
		if (mdb_cursor_del(mdb->cursorLog,MDB_NODUPDATA) != MDB_SUCCESS)
		{
			DBG("Rewind Unable to cleanup key from logdb");
		}
		if (mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_PREV_NODUP) != MDB_SUCCESS)
		{
			DBG("Rewind Unable to move to next log");
			break;
		}
		memcpy(&aindex, logKey.mv_data,                 sizeof(u64));
		memcpy(&evterm, (u8*)logKey.mv_data + sizeof(u64),   sizeof(u64));
		memcpy(&evnum,  (u8*)logKey.mv_data + sizeof(u64)*2, sizeof(u64));

		if (aindex != pWal->index)
		{
			DBG("Rewind Reached another actor=%llu, me=%llu",aindex,pWal->index);
			break;
		}
		pWal->lastCompleteTerm = evterm;
		pWal->lastCompleteEvnum = evnum;
		pWal->allPages -= allPagesDiff;
		allPagesDiff = 0;
	}
	return SQLITE_OK;
}

static int storeinfo(Wal *pWal, u64 currentTerm, u8 votedForSize, u8 *votedFor)
{
	MDB_val key = {0,NULL}, data = {0,NULL};
	int rc;
	db_thread *thr = g_tsd_thread;
	mdbinf* mdb;

	if (!g_tsd_wmdb)
		lock_wtxn(thr->nEnv);
	mdb = g_tsd_wmdb;
	if (!mdb)
		return SQLITE_ERROR;

	key.mv_size = sizeof(u64);
	key.mv_data = &pWal->index;
	if (votedFor == NULL)
	{
		rc = mdb_cursor_get(mdb->cursorInfo,&key,&data,MDB_SET_KEY);

		if (rc == MDB_SUCCESS && data.mv_size >= (1+sizeof(u64)*6+sizeof(u32)*2+sizeof(u64)+1))
		{
			memcpy(&currentTerm, (u8*)data.mv_data+1+sizeof(u64)*6+sizeof(u32)*2, sizeof(u64));
			votedForSize = (u8)((u8*)data.mv_data)[1+sizeof(u64)*6+sizeof(u32)*2+sizeof(u64)];
			//votedFor = data.mv_data+1+sizeof(u64)*6+sizeof(u32)*2+sizeof(u64)+1;
			votedFor = alloca(votedForSize);
			memcpy(votedFor,(u8*)data.mv_data+1+sizeof(u64)*6+sizeof(u32)*2+sizeof(u64)+1, votedForSize);
			// DBG("Voted for %.*s",(int)votedForSize,(char*)votedFor));
		}
	}
	key.mv_size = sizeof(u64);
	key.mv_data = &pWal->index;
	data.mv_data = NULL;
	data.mv_size = 1+sizeof(u64)*6+sizeof(u32)*2+sizeof(u64)+1+votedForSize;
	rc = mdb_cursor_put(mdb->cursorInfo,&key,&data,MDB_RESERVE);
	if (rc == MDB_SUCCESS)
	{
		u8 *infoBuf = data.mv_data;
		infoBuf[0] = 1;
		memcpy(infoBuf+1,               &pWal->firstCompleteTerm,sizeof(u64));
		memcpy(infoBuf+1+sizeof(u64),   &pWal->firstCompleteEvnum,sizeof(u64));
		memcpy(infoBuf+1+sizeof(u64)*2, &pWal->lastCompleteTerm, sizeof(u64));
		memcpy(infoBuf+1+sizeof(u64)*3, &pWal->lastCompleteEvnum,sizeof(u64));
		memcpy(infoBuf+1+sizeof(u64)*4, &pWal->inProgressTerm,   sizeof(u64));
		memcpy(infoBuf+1+sizeof(u64)*5, &pWal->inProgressEvnum,  sizeof(u64));
		memcpy(infoBuf+1+sizeof(u64)*6, &pWal->mxPage,           sizeof(u32));
		memcpy(infoBuf+1+sizeof(u64)*6+sizeof(u32), &pWal->allPages,sizeof(u32));
		memcpy(infoBuf+1+sizeof(u64)*6+sizeof(u32)*2, &currentTerm, sizeof(u64));
		infoBuf[1+sizeof(u64)*7+sizeof(u32)*2] = votedForSize;
		memcpy(infoBuf+2+sizeof(u64)*7+sizeof(u32)*2, votedFor, votedForSize);
		thr->forceCommit = 1;
		return SQLITE_OK;
	}
	return SQLITE_ERROR;
}

/* Write a frame or frames to the log. */
int sqlite3WalFrames(Wal *pWal, int szPage, PgHdr *pList, Pgno nTruncate, int isCommit, int sync_flags)
{
	PgHdr *p;
	MDB_val key, data;
	int rc;
	mdbinf* mdb;
	MDB_txn* txn;
	db_thread *thr      = g_tsd_thread;
	db_connection* pCon	= g_tsd_conn;

	DBG("sqlite3WalFrames");

	if (!thr)
		return SQLITE_ERROR;
	if (!g_tsd_wmdb)
		lock_wtxn(thr->nEnv);
	mdb = g_tsd_wmdb;
	txn = mdb->txn;

	if (!mdb)
		return SQLITE_ERROR;

	key.mv_size = sizeof(u64);
	key.mv_data = (void*)&pWal->index;

	// Term/evnum must always be increasing
	while ((pWal->inProgressTerm > 0 && pWal->inProgressTerm < pWal->lastCompleteTerm) ||
		(pWal->inProgressEvnum > 0 && pWal->inProgressEvnum < pWal->lastCompleteEvnum))
	{
		u64 preTerm = pWal->lastCompleteTerm;
		u64 preNum = pWal->lastCompleteEvnum;
		DBG("Must do rewind. inprog_term=%llu, inprog_evnum=%llu, lc_term=%llu, lc_evnum=%llu",
			pWal->inProgressTerm, pWal->inProgressEvnum, pWal->lastCompleteTerm, pWal->lastCompleteEvnum);

		wal_rewind_int(mdb,pWal,pWal->lastCompleteEvnum);
		if (preTerm == pWal->lastCompleteTerm && preNum == pWal->lastCompleteEvnum)
			break;
	}

	track_time(2,thr);
	// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Fragment,CompressedPage/binary>>}
	for(p=pList; p; p=p->pDirty)
	{
		u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
		u8 pagesBuf[PAGE_BUFF_SIZE];
		int full_size = 0;
		int page_size = LZ4_compress_default((char*)p->pData,(char*)pagesBuf+sizeof(u64)*2+1,szPage,sizeof(pagesBuf));
		char fragment_index = 0;
		int skipped = 0;
		track_time(3,thr);

		DBG("Insert frame, actor=%lld, pgno=%u, "
			"term=%lld, evnum=%lld, commit=%d, truncate=%d, compressedsize=%d",
		pWal->index,p->pgno,pWal->inProgressTerm,pWal->inProgressEvnum,
		isCommit,nTruncate,page_size);

		if (pCon->doReplicate)
		{
			u8 hdr[sizeof(u64)*2+sizeof(u32)*2];
			put8byte(hdr,               pWal->inProgressTerm);
			put8byte(hdr+sizeof(u64),   pWal->inProgressEvnum);
			put4byte(hdr+sizeof(u64)*2, p->pgno);
			if (p->pDirty)
				put4byte(hdr+sizeof(u64)*2+sizeof(u32), 0);
			else
				put4byte(hdr+sizeof(u64)*2+sizeof(u32), nTruncate);
			#ifndef _TESTAPP_
			wal_page_hook(thr,pagesBuf+sizeof(u64)*2+1, page_size, hdr, sizeof(hdr));
			#endif
		}

		memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
		memcpy(pagesKeyBuf + sizeof(u64), &p->pgno,    sizeof(u32));
		key.mv_size = sizeof(pagesKeyBuf);
		key.mv_data = pagesKeyBuf;


		// Check if there are pages with the same or higher evnum/evterm. If there are, delete them.
		// This can happen if sqlite flushed some page to disk before commiting, because there were
		// so many pages that they could not be held in memory. Or it could happen if pages need to be
		// overwritten because there was a write that did not pass raft consensus.
		rc = mdb_cursor_get(mdb->cursorPages,&key,&data,MDB_SET_KEY);
		if (rc == MDB_SUCCESS)
		{
			size_t ndupl;
			mdb_cursor_count(mdb->cursorPages,&ndupl);

			rc = mdb_cursor_get(mdb->cursorPages,&key,&data,MDB_LAST_DUP);
			if (rc == MDB_SUCCESS)
			{
				MDB_val pgDelKey = {0,NULL}, pgDelVal = {0,NULL};
				u64 evnum, evterm;
				u8 frag = *((u8*)data.mv_data+sizeof(u64)*2);
				memcpy(&evterm, data.mv_data,               sizeof(u64));
				memcpy(&evnum,  (u8*)data.mv_data + sizeof(u64), sizeof(u64));

				while ((evterm > pWal->inProgressTerm || evnum >= pWal->inProgressEvnum))
						//(pWal->inProgressTerm + pWal->inProgressEvnum) > 0)
				{
					DBG("Deleting pages higher or equal to current. "
					"Evterm=%llu, evnum=%llu, curterm=%llu, curevn=%llu, dupl=%ld",
					evterm,evnum,pWal->inProgressTerm,pWal->inProgressEvnum,ndupl);

					if (pgDelKey.mv_data != NULL)
					{
						if ((rc = mdb_del(txn,mdb->pagesdb,&pgDelKey,&pgDelVal)) != MDB_SUCCESS)
						{
							DBG("Unable to cleanup page from pagedb %d",rc);
							break;
						}
						pgDelKey.mv_data = NULL;
					}
					mdb_cursor_get(mdb->cursorPages,&pgDelKey,&pgDelVal,MDB_GET_CURRENT);

					// if (mdb_cursor_del(mdb->cursorPages,0) != MDB_SUCCESS)
					// {
					// 	DBG("Cant delete!");
					// 	break;
					// }

					if (frag == 0)
						pWal->allPages--;
					ndupl--;
					if (!ndupl)
						break;
					rc = mdb_cursor_get(mdb->cursorPages,&key,&data,MDB_PREV_DUP);
					if (rc != MDB_SUCCESS)
						break;
					memcpy(&evterm, data.mv_data,               sizeof(u64));
					memcpy(&evnum,  (u8*)data.mv_data + sizeof(u64), sizeof(u64));
					frag = *((u8*)data.mv_data+sizeof(u64)*2);
				}
				if (pgDelKey.mv_data != NULL)
				{
					if ((rc = mdb_del(txn,mdb->pagesdb,&pgDelKey,&pgDelVal)) != MDB_SUCCESS)
					{
						DBG("Unable to cleanup page from pagedb %d",rc);
						break;
					}
					pgDelKey.mv_data = NULL;
				}
			}
			memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
			memcpy(pagesKeyBuf + sizeof(u64), &p->pgno,    sizeof(u32));
			key.mv_size = sizeof(pagesKeyBuf);
			key.mv_data = pagesKeyBuf;
		}
		track_time(4,thr);

		memcpy(pagesBuf,               &pWal->inProgressTerm,  sizeof(u64));
		memcpy(pagesBuf + sizeof(u64), &pWal->inProgressEvnum, sizeof(u64));

		full_size = page_size + sizeof(u64)*2 + 1;
		if (full_size < thr->maxvalsize)
			fragment_index = 0;
		else
		{
			full_size = page_size;
			skipped = thr->maxvalsize - sizeof(u64)*2 - 1;
			full_size -= skipped;
			while(full_size > 0)
			{
				full_size -= (thr->maxvalsize - sizeof(u64)*2 - 1);
				fragment_index++;
			}
			full_size = page_size + sizeof(u64)*2 +1;
		}

		pagesBuf[sizeof(u64)*2] = fragment_index;
		data.mv_size = fragment_index == 0 ? full_size : thr->maxvalsize;
		data.mv_data = pagesBuf;

		// fragment_index == 0 ? MDB_APPENDDUP : 0
		if ((rc = mdb_cursor_put(mdb->cursorPages,&key,&data,0)) != MDB_SUCCESS)
		{
			// printf("Cursor put failed to pages %d",rc);
			DBG("ERROR: cursor put failed=%d, datasize=%d",rc,full_size);
			return SQLITE_ERROR;
		}

		fragment_index--;
		skipped = data.mv_size;
		while (fragment_index >= 0)
		{
			DBG("Insert fragment %d",(int)fragment_index);
			if (fragment_index == 0)
				data.mv_size = full_size - skipped + sizeof(u64)*2 + 1;
			else
				data.mv_size = thr->maxvalsize;
			data.mv_data = pagesBuf + skipped - (sizeof(u64)*2+1);
			memcpy(pagesBuf + skipped - (sizeof(u64)*2+1), &pWal->inProgressTerm,  sizeof(u64));
			memcpy(pagesBuf + skipped - (sizeof(u64)+1),   &pWal->inProgressEvnum, sizeof(u64));
			pagesBuf[skipped-1] = fragment_index;

			if ((rc = mdb_cursor_put(mdb->cursorPages,&key,&data,0)) != MDB_SUCCESS)
			{
				DBG("ERROR: cursor secondary put failed: err=%d, datasize=%d, skipped=%d, frag=%d",
				rc,full_size, skipped, (int)fragment_index);
				return SQLITE_ERROR;
			}
			skipped += data.mv_size - sizeof(u64)*2 - 1;
			fragment_index--;
		}

		thr->pagesChanged++;
	}
	// printf("");
	// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
	if (pWal->inProgressTerm > 0)
	{
		for(p=pList; p; p=p->pDirty)
		{
			u8 logKeyBuf[sizeof(u64)*3];

			DBG("Inserting to log");

			memcpy(logKeyBuf,                 &pWal->index,           sizeof(u64));
			memcpy(logKeyBuf + sizeof(u64),   &pWal->inProgressTerm,  sizeof(u64));
			memcpy(logKeyBuf + sizeof(u64)*2, &pWal->inProgressEvnum, sizeof(u64));
			key.mv_size = sizeof(logKeyBuf);
			key.mv_data = logKeyBuf;

			data.mv_size = sizeof(u32);
			data.mv_data = &p->pgno;

			if (mdb_cursor_put(mdb->cursorLog,&key,&data,0) != MDB_SUCCESS)
			{
				// printf("Cursor put failed to log");
				DBG("ERROR: cursor put to log failed: %d",rc);
				return SQLITE_ERROR;
			}

			pWal->allPages++;
		}
	}
	else
	{
		DBG("Skipping log");
		for(p=pList; p; p=p->pDirty)
			pWal->allPages++;
	}
  /** - Info DB: {<<ActorIndex:64>>, <<V,FirstCompleteTerm:64,FirstCompleteEvnum:64,
										LastCompleteTerm:64,LastCompleteEvnum:64,
										InprogressTerm:64,InProgressEvnum:64>>} */
	{
		if (isCommit)
		{
			DBG("Commit actor=%llu fct=%llu, fcev=%llu, lct=%llu, lcev=%llu, int=%llu, inev=%llu",
				pWal->index,
				pWal->firstCompleteTerm, pWal->firstCompleteEvnum, pWal->lastCompleteTerm,
				pWal->lastCompleteEvnum, pWal->inProgressTerm,pWal->inProgressEvnum);

			#ifndef _TESTAPP_
			enif_mutex_lock(pWal->mtx);
			#endif
			pWal->lastCompleteTerm = pWal->inProgressTerm > 0 ?
				pWal->inProgressTerm : pWal->lastCompleteTerm;
			pWal->lastCompleteEvnum = pWal->inProgressEvnum > 0 ?
				pWal->inProgressEvnum : pWal->lastCompleteEvnum;
			if (pWal->firstCompleteTerm == 0)
			{
				pWal->firstCompleteTerm = pWal->inProgressTerm;
				pWal->firstCompleteEvnum = pWal->inProgressEvnum;
			}
			pWal->inProgressTerm = pWal->inProgressEvnum = 0;
			pWal->mxPage =  pWal->mxPage > nTruncate ? pWal->mxPage : nTruncate;
			// pWal->changed = 0;
			thr->forceCommit = 1;
			pCon->dirty = 0;
			#ifndef _TESTAPP_
			enif_mutex_unlock(pWal->mtx);
			#endif
			DBG("cur mxpage=%u",pWal->mxPage);
		}
		else
		{
			// pWal->changed = 1;
			pCon->dirty = 1;
		}
		thr->pagesChanged++;

		rc = storeinfo(pWal,0,0,NULL);
		if (rc != SQLITE_OK)
			return rc;

		track_time(5,thr);
	}

	return SQLITE_OK;
}



/* Copy pages from the log to the database file */
int sqlite3WalCheckpoint(
  Wal *pWal,                      /* Write-ahead log connection */
  sqlite3 *db,
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
	DBG("Checkpoint");
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
	DBG("heap");
	return pWal != NULL;
}



static u64 get8byte(u8* buf)
{
	return ((u64)buf[0] << 56) + ((u64)buf[1] << 48) + ((u64)buf[2] << 40) + ((u64)buf[3] << 32) +
		((u64)buf[4] << 24) + ((u64)buf[5] << 16)  + ((u64)buf[6] << 8) + buf[7];
}
static void put8byte(u8* buf, u64 num)
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

static int logdb_cmp(const MDB_val *a, const MDB_val *b)
{
	i64 aActor,aEvterm,aEvnum,bActor,bEvterm,bEvnum;
	int diff;

	memcpy(&aActor,a->mv_data,sizeof(i64));
	memcpy(&bActor,b->mv_data,sizeof(i64));
	diff = aActor - bActor;
	if (diff == 0)
	{
		memcpy(&aEvterm, (u8*)a->mv_data+sizeof(i64), sizeof(i64));
		memcpy(&bEvterm, (u8*)b->mv_data+sizeof(i64), sizeof(i64));
		diff = aEvterm - bEvterm;
		if (diff == 0)
		{
			memcpy(&aEvnum, (u8*)a->mv_data+sizeof(i64)*2, sizeof(i64));
			memcpy(&bEvnum, (u8*)b->mv_data+sizeof(i64)*2, sizeof(i64));
			return aEvnum - bEvnum;
		}
		return diff;
	}
	return diff;
}

static int pagesdb_cmp(const MDB_val *a, const MDB_val *b)
{
	u64 aActor;
	u64 bActor;
	u32 aPgno;
	u32 bPgno;
	int diff;

	memcpy(&aActor,a->mv_data,sizeof(u64));
	memcpy(&bActor,b->mv_data,sizeof(u64));
	diff = aActor - bActor;
	if (diff == 0)
	{
		memcpy(&aPgno,(u8*)a->mv_data + sizeof(u64),sizeof(u32));
		memcpy(&bPgno,(u8*)b->mv_data + sizeof(u64),sizeof(u32));
		return aPgno - bPgno;
	}
	return diff;
}

static int pagesdb_val_cmp(const MDB_val *a, const MDB_val *b)
{
	u64 aEvterm,aEvnum;
	u64 bEvterm,bEvnum;
	u8 aCounter, bCounter;
	int diff;

	memcpy(&aEvterm, a->mv_data, sizeof(u64));
	memcpy(&bEvterm, b->mv_data, sizeof(u64));
	diff = aEvterm - bEvterm;
	if (diff == 0)
	{
		memcpy(&aEvnum, (u8*)a->mv_data+sizeof(u64), sizeof(u64));
		memcpy(&bEvnum, (u8*)b->mv_data+sizeof(u64), sizeof(u64));
		diff = aEvnum - bEvnum;
		if (diff == 0)
		{
			aCounter = ((u8*)a->mv_data)[sizeof(u64)*2];
			bCounter = ((u8*)b->mv_data)[sizeof(u64)*2];
			return aCounter - bCounter;
		}
		return diff;
	}
	return diff;
}

static MDB_txn* open_txn(mdbinf *data, int flags)
{
	if (mdb_txn_begin(data->env, NULL, flags, &data->txn) != MDB_SUCCESS)
		return NULL;
	// if (mdb_dbi_open(data->txn, "info", MDB_INTEGERKEY, &data->infodb) != MDB_SUCCESS)
	// 	return NULL;
	// if (mdb_dbi_open(data->txn, "actors", 0, &data->actorsdb) != MDB_SUCCESS)
	// 	return NULL;
	// if (mdb_dbi_open(data->txn, "log", MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERDUP, &data->logdb) != MDB_SUCCESS)
	// 	return NULL;
	// if (mdb_dbi_open(data->txn, "pages", MDB_DUPSORT, &data->pagesdb) != MDB_SUCCESS)
	// 	return NULL;
	if (mdb_set_compare(data->txn, data->logdb, logdb_cmp) != MDB_SUCCESS)
		return NULL;
	if (mdb_set_compare(data->txn, data->pagesdb, pagesdb_cmp) != MDB_SUCCESS)
		return NULL;
	if (mdb_set_dupsort(data->txn, data->pagesdb, pagesdb_val_cmp) != MDB_SUCCESS)
		return NULL;
	if (mdb_cursor_open(data->txn, data->logdb, &data->cursorLog) != MDB_SUCCESS)
		return NULL;
	if (mdb_cursor_open(data->txn, data->pagesdb, &data->cursorPages) != MDB_SUCCESS)
		return NULL;
	if (mdb_cursor_open(data->txn, data->infodb, &data->cursorInfo) != MDB_SUCCESS)
		return NULL;

	return data->txn;
}

sqlite3_file *sqlite3WalFile(Wal *pWal)
{
	return NULL;
}
