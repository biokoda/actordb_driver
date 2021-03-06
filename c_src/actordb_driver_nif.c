// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
// #define _TESTDBG_ 1
#ifdef __linux__
#define _GNU_SOURCE 1
#include <sys/mman.h>
#include <dlfcn.h>
#endif

#ifndef  _WIN32
#include <sys/time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <netdb.h>
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

static void wal_page_hook(void *data,void *page,int pagesize,void* header, int headersize);
static qitem* command_create(int writeThreadNum, int readThreadNum, priv_data *p);
static ERL_NIF_TERM push_command(int writeThreadNum, int readThreadNum, priv_data *pd, qitem *item);
static void lock_wtxn(int env);

#ifdef _WIN32
#define __thread __declspec( thread )
#endif

static __thread db_thread      *g_tsd_thread = NULL;
static __thread db_connection  *g_tsd_conn = NULL;
static __thread mdbinf         *g_tsd_wmdb = NULL;
static __thread u64             g_tsd_cursync = 0;

static priv_data               *g_pd;
static int                      g_nbatch = 0;  // how many writes to batch together
static u8                       g_transsync;   // if every transaction is a sync to disk
// allocated on_load, used as global counters from erlang
// When loading size of array is set. Every counter takes up 8*sizeof(i64).
static _Atomic(i64)            *g_counters = NULL;
static int                      g_nCounters = 0;
static int                      g_timeCounter = -1;  // which index in counters is timer
static ErlNifResourceType *db_connection_type;
static ErlNifResourceType *iterate_type;

#include "wal.c"
#include "nullvfs.c"

ERL_NIF_TERM atom_ok;
ERL_NIF_TERM atom_false;
ERL_NIF_TERM atom_error;
ERL_NIF_TERM atom_rows;
ERL_NIF_TERM atom_columns;
ERL_NIF_TERM atom_undefined;
ERL_NIF_TERM atom_rowid;
ERL_NIF_TERM atom_changes;
ERL_NIF_TERM atom_done;
ERL_NIF_TERM atom_iter;
ERL_NIF_TERM atom_blob;
ERL_NIF_TERM atom_wthreads;
ERL_NIF_TERM atom_rthreads;
ERL_NIF_TERM atom_paths;
ERL_NIF_TERM atom_staticsqls;
ERL_NIF_TERM atom_dbsize;
ERL_NIF_TERM atom_logname;
ERL_NIF_TERM atom_nbatch;
ERL_NIF_TERM atom_lmdbsync;
ERL_NIF_TERM atom_tcpfail;
ERL_NIF_TERM atom_drivername;
ERL_NIF_TERM atom_again;
ERL_NIF_TERM atom_counters;
ERL_NIF_TERM atom_maxreqtime;
ERL_NIF_TERM atom_timecounter;
ERL_NIF_TERM atom_pluginfiles;

static ERL_NIF_TERM make_atom(ErlNifEnv *env, const char *atom_name)
{
	ERL_NIF_TERM atom;

	if(enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1))
		return atom;

	return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM make_ok_tuple(ErlNifEnv *env, ERL_NIF_TERM value)
{
	return enif_make_tuple2(env, atom_ok, value);
}

static ERL_NIF_TERM make_error_tuple(ErlNifEnv *env, const char *reason)
{
	return enif_make_tuple2(env, atom_error, make_atom(env, reason));
}

static void lock_wtxn(int nEnv)
{
	u32 i;
	mdbinf *wmdb = NULL;
	DBG("Trying to lock wmtx");
	for (i = 0; enif_mutex_trylock(g_pd->wthrMutexes[nEnv]) != 0; ++i)
	{
		#ifdef _WIN32
		Sleep(1);
		#else
		if (i > 1000000)
			usleep(i / 100000);
		#endif
	}
	DBG("lock wtxn %u",i);
	wmdb = g_tsd_wmdb = &g_pd->wmdb[nEnv];

	if (wmdb->txn == NULL)
	{
		if (open_txn(wmdb, 0) == NULL)
			return;
	}

	g_tsd_cursync = g_pd->syncNumbers[nEnv];
}

static void unlock_write_txn(int nEnv, char syncForce, char *commit, char hasWritten)
{
	int i;
	mdbinf *wmdb = NULL;

	if (!g_tsd_wmdb)
		return;
	wmdb = g_tsd_wmdb;

	++wmdb->batchCounter;
	DBG("Batch count %u",wmdb->batchCounter);
	wmdb->hasWritten |= hasWritten;
	if (*commit || syncForce || wmdb->batchCounter > g_nbatch)
	{
		if (mdb_txn_commit(wmdb->txn) != MDB_SUCCESS)
			mdb_txn_abort(wmdb->txn);
		wmdb->txn = NULL;
		wmdb->batchCounter = 0;
		wmdb->commitCount++;

		if (syncForce && wmdb->hasWritten)
			mdb_env_sync(wmdb->env,1);

		if (g_transsync || syncForce)
		{
			++g_pd->syncNumbers[nEnv];
			wmdb->hasWritten = 0;
		}
		*commit = 1;
	}
	g_tsd_cursync = g_pd->syncNumbers[nEnv];
	g_tsd_wmdb = NULL;

	// Send a cmd_synced to all write threads if we executed commit.
	// They must send back replies to ops.
	if (*commit && g_nbatch)
	{
		for (i = 0; i < g_pd->nWriteThreads; i++)
		{
			qitem *item;
			db_command *cmd;
			int thrind = nEnv*g_pd->nWriteThreads + i;
			if (g_tsd_thread->nThread == i) {
				continue;
			}
			if (g_pd->wtasks[thrind] == NULL)
				break;
			item = command_create(thrind,-1, g_pd);
			if (!item) {
				continue;
			}
			cmd = (db_command*)item->cmd;
			cmd->type = cmd_synced;
			push_command(thrind, -1, g_pd, item);
		}
	}
	enif_mutex_unlock(g_pd->wthrMutexes[nEnv]);
}

#define PACKET_ITEMS 9

static void wal_page_hook(void *data,void *buff,int buffUsed,void* header, int headersize)
{
	db_thread *thread = (db_thread *) data;
	db_connection *conn = g_tsd_conn;
	int i = 0;
	int completeSize = 0;
#ifndef  _WIN32
	struct iovec iov[PACKET_ITEMS];
#else
	WSABUF iov[PACKET_ITEMS];
#endif
	u8 packetLen[4];
	u8 lenPrefix[2];
	u8 lenPage[2];
	u8 lenVarPrefix[2];
	u8 lenHeader = (char)headersize;
	int rt;

	if (!conn->doReplicate)
	{
		return;
	}
	// conn->nSent = conn->failFlags = 0;

	completeSize = buffUsed+2+headersize+1+conn->packetPrefixSize+2+conn->packetVarPrefix.size+2;
	put2byte(lenPage,buffUsed);
	put4byte(packetLen,completeSize);
	put2byte(lenPrefix,conn->packetPrefixSize);
	put2byte(lenVarPrefix,conn->packetVarPrefix.size);

#ifndef _WIN32
	// Entire size
	iov[0].iov_base = packetLen;
	iov[0].iov_len = 4;
	// Prefix size and prefix data
	iov[1].iov_base = lenPrefix;
	iov[1].iov_len = 2;
	iov[2].iov_base = conn->packetPrefix;
	iov[2].iov_len = conn->packetPrefixSize;
	// Variable prefix
	iov[3].iov_base = lenVarPrefix;
	iov[3].iov_len = 2;
	iov[4].iov_base = conn->packetVarPrefix.data;
	iov[4].iov_len = conn->packetVarPrefix.size;
	// header size and header data
	iov[5].iov_base = &lenHeader;
	iov[5].iov_len = 1;
	iov[6].iov_base = header;
	iov[6].iov_len = headersize;
	// page size and page data
	iov[7].iov_base = lenPage;
	iov[7].iov_len = 2;
	iov[8].iov_base = buff;
	iov[8].iov_len = buffUsed;
#else
	// Entire size
	iov[0].buf = packetLen;
	iov[0].len = 4;
	// Prefix size and prefix data
	iov[1].buf = lenPrefix;
	iov[1].len = 2;
	iov[2].buf = conn->packetPrefix;
	iov[2].len = conn->packetPrefixSize;
	// Variable prefix
	iov[3].buf = lenVarPrefix;
	iov[3].len = 2;
	iov[4].buf = conn->packetVarPrefix.data;
	iov[4].len = conn->packetVarPrefix.size;
	// header size and header data
	iov[5].buf = &lenHeader;
	iov[5].len = 1;
	iov[6].buf = header;
	iov[6].len = headersize;
	// page size and page data
	iov[7].buf = lenPage;
	iov[7].len = 2;
	iov[8].buf = buff;
	iov[8].len = buffUsed;
#endif

	for (i = 0; i < MAX_CONNECTIONS; i++)
	{
		if (thread->sockets[i] > 3 && thread->socket_types[i] <= conn->doReplicate)
		{
			// sockets are blocking. We presume we are not
			//  network bound thus there should not be a lot of blocking
#ifndef _WIN32
			rt = writev(thread->sockets[i],iov,PACKET_ITEMS);
#else
			if (WSASend(thread->sockets[i],iov,PACKET_ITEMS, &rt, 0, NULL, NULL) != 0)
				rt = 0;
#endif
			if (rt != completeSize+4)
			{
				// close(thread->sockets[i]);
				thread->sockets[i] = 0;
				fail_send(i);
			}
		}
	}

	// var prefix only sent with first packet
	conn->packetVarPrefix.data = NULL;
	conn->packetVarPrefix.size = 0;
}

void fail_send(int i)
{
	db_thread *thr = g_tsd_thread;
	DBG("fail_send %d", i);
	enif_send(NULL, &g_pd->tunnelConnector, thr->env,
		enif_make_tuple4(thr->env,
			atom_tcpfail, atom_drivername,
			enif_make_int(thr->env, thr->nEnv * g_pd->nWriteThreads + thr->nThread),
			enif_make_int(thr->env, i)));
	enif_clear_env(thr->env);
}


static ERL_NIF_TERM do_all_tunnel_call(db_command *cmd,db_thread *thread, ErlNifEnv *env)
{
#ifndef  _WIN32
	struct iovec iov[PACKET_ITEMS];
#else
	WSABUF iov[PACKET_ITEMS];
#endif
	u8 packetLen[4];
	u8 bodyLen[4];
	ErlNifBinary head, body;
	u8 *bodyCompressed = NULL;
	int nsent = 0, i = 0, rt = 0, nelements = 2, compressedSize = 0;
	unsigned mxSz;
	int sockets[MAX_CONNECTIONS];


	memset(sockets,0,sizeof(int)*MAX_CONNECTIONS);
	memset(&head,0,sizeof(ErlNifBinary));
	memset(&body,0,sizeof(ErlNifBinary));

	if (memcmp(sockets,thread->sockets,sizeof(int)*MAX_CONNECTIONS) == 0)
		return enif_make_int(env,0);

	if (!enif_inspect_iolist_as_binary(env,cmd->arg,&(head)))
		return atom_false;

	if (cmd->arg1)
	{
		if (!enif_inspect_iolist_as_binary(env,cmd->arg1,&body))
			return atom_false;
		mxSz = LZ4_COMPRESSBOUND(body.size)+4;
		if (mxSz < 64*1024)
			bodyCompressed = alloca(mxSz);
		if (!bodyCompressed)
		{
			if (thread->bufSize < mxSz)
			{
				bodyCompressed = realloc(thread->wBuffer,mxSz);
				if (!bodyCompressed)
					return atom_false;
				thread->bufSize = mxSz;
				thread->wBuffer = bodyCompressed;
			}
			else
			{
				bodyCompressed = thread->wBuffer;
			}
		}
		compressedSize = 4 + LZ4_compress_default((const char*)body.data, (char*)bodyCompressed+4, body.size, mxSz-4);
		nelements = 4;
	}

	if (bodyCompressed)
	{
		put4byte(packetLen,head.size + compressedSize + 4);
		put4byte(bodyLen,compressedSize);
		put4byte(bodyCompressed,body.size);
	}
	else
		put4byte(packetLen,head.size);

#ifndef  _WIN32
	iov[0].iov_base = packetLen;
	iov[0].iov_len = 4;
	iov[1].iov_len = head.size;
	iov[1].iov_base = head.data;
	if (cmd->arg1)
	{
		iov[2].iov_len = 4;
		iov[2].iov_base = bodyLen;
		iov[3].iov_len = compressedSize;
		iov[3].iov_base = bodyCompressed;
	}
#else
	iov[0].buf = packetLen;
	iov[0].len = 4;
	iov[1].len = head.size;
	iov[1].buf = head.data;
	if (cmd->arg1)
	{
		iov[2].buf = bodyLen;
		iov[2].len = 4;
		iov[3].buf = bodyCompressed;
		iov[3].len = compressedSize;
	}
#endif

	for (i = 0; i < MAX_CONNECTIONS; i++)
	{
		if (thread->sockets[i] > 3 && thread->socket_types[i] == 1)
		{
#ifndef _WIN32
			rt = writev(thread->sockets[i],iov, nelements);
#else
			if (WSASend(thread->sockets[i],iov,nelements, &rt, 0, NULL, NULL) != 0)
				rt = 0;
#endif
			if (rt != head.size+compressedSize+4 + (cmd->arg1 ? 4 : 0))
			{
				// close(thread->sockets[i]);
				thread->sockets[i] = 0;
				fail_send(i);
			}
			nsent++;
		}
	}

	return enif_make_int(env,nsent);
}


// static ERL_NIF_TERM do_store_prepared_table(db_command *cmd,db_thread *thread)
// {
// 	// Delete old table of prepared statements and set new one.
// 	const ERL_NIF_TERM *versTuple;
// 	const ERL_NIF_TERM *sqlTuple;
// 	const ERL_NIF_TERM *versRow;
// 	const ERL_NIF_TERM *sqlRow;
// 	int tupleSize,rowSize,i,j;
// 	ErlNifBinary bin;

// 	memset(thread->prepVersions,0,sizeof(thread->prepVersions));
// 	for (i = 0; i < MAX_PREP_SQLS; i++)
// 	{
// 		for (j = 0; j < MAX_PREP_SQLS; j++)
// 		{
// 			free(thread->prepSqls[i][j]);
// 			thread->prepSqls[i][j] = NULL;
// 		}
// 	}

// 	// {{1,2,2,0,0,..},{0,0,0,0,...}}
// 	if (!enif_get_tuple(env, cmd->arg, &tupleSize, &versTuple))
// 		return atom_false;
// 	// {{"select....","insert into..."},{"select....","update ...."}}
// 	if (!enif_get_tuple(env, cmd->arg1, &tupleSize, &sqlTuple))
// 		return atom_false;

// 	if (tupleSize > MAX_PREP_SQLS)
// 		return atom_false;

// 	for (i = 0; i < tupleSize; i++)
// 	{
// 		if (!enif_get_tuple(env, versTuple[i], &rowSize, &versRow))
// 			break;
// 		if (!enif_get_tuple(env, sqlTuple[i], &rowSize, &sqlRow))
// 			break;

// 		thread->prepSize = rowSize;
// 		for (j = 0; j < rowSize; j++)
// 		{
// 			enif_get_int(env,versRow[j],&(thread->prepVersions[i][j]));
// 			if (enif_is_list(env,sqlRow[j]) || enif_is_binary(env,sqlRow[j]))
// 			{
// 				enif_inspect_iolist_as_binary(env,sqlRow[j],&bin);
// 				thread->prepSqls[i][j] = malloc(bin.size+1);
// 				thread->prepSqls[i][j][bin.size] = 0;
// 				memcpy(thread->prepSqls[i][j],bin.data,bin.size);
// 			}
// 		}
// 	}
// 	return atom_ok;
// }

static const char *get_sqlite3_return_code_msg(int r)
{
	switch(r) {
	case SQLITE_OK: return "ok";
	case SQLITE_ERROR : return "sqlite_error";
	case SQLITE_INTERNAL: return "internal";
	case SQLITE_PERM: return "perm";
	case SQLITE_ABORT: return "abort";
	case SQLITE_BUSY: return "busy";
	case SQLITE_LOCKED: return  "locked";
	case SQLITE_NOMEM: return  "nomem";
	case SQLITE_READONLY: return  "readonly";
	case SQLITE_INTERRUPT: return  "interrupt";
	case SQLITE_IOERR: return  "ioerror";
	case SQLITE_CORRUPT: return  "corrupt";
	case SQLITE_NOTFOUND: return  "notfound";
	case SQLITE_FULL: return  "full";
	case SQLITE_CANTOPEN: return  "cantopen";
	case SQLITE_PROTOCOL: return  "protocol";
	case SQLITE_EMPTY: return  "empty";
	case SQLITE_SCHEMA: return  "schema";
	case SQLITE_TOOBIG: return  "toobig";
	case SQLITE_CONSTRAINT: return  "constraint";
	case SQLITE_MISMATCH: return  "mismatch";
	case SQLITE_MISUSE: return  "misuse";
	case SQLITE_NOLFS: return  "nolfs";
	case SQLITE_AUTH: return  "auth";
	case SQLITE_FORMAT: return  "format";
	case SQLITE_RANGE: return  "range";
	case SQLITE_NOTADB: return  "notadb";
	case SQLITE_ROW: return  "row";
	case SQLITE_DONE: return  "done";
	}
	return  "unknown";
}

static const char *get_sqlite3_error_msg(int error_code, sqlite3 *db)
{
	// if(error_code == SQLITE_MISUSE)
	//     return "Sqlite3 was invoked incorrectly.";

	return sqlite3_errmsg(db);
}

static ERL_NIF_TERM make_sqlite3_error_tuple(ErlNifEnv *env,const char* calledfrom, int error_code,
	int pos, sqlite3 *db)
{
	if (db)
	{
		const char *error_code_msg = get_sqlite3_return_code_msg(error_code);
		const char *msg = get_sqlite3_error_msg(error_code, db);
		DBG("Returning sqlite error: %s:%s",error_code_msg, msg);

		if (error_code > 0)
			return enif_make_tuple2(env, atom_error,
				enif_make_tuple4(env,
					enif_make_int(env,pos),
					enif_make_string(env,"",ERL_NIF_LATIN1),
					make_atom(env, error_code_msg),
					enif_make_string(env, msg, ERL_NIF_LATIN1)));
		else
			return enif_make_tuple2(env, atom_error,
				enif_make_tuple4(env,
					enif_make_int(env,pos),
					enif_make_string(env,calledfrom,ERL_NIF_LATIN1),
					atom_error,
					enif_make_string(env, calledfrom, ERL_NIF_LATIN1)));
	}
	else
	{
		return enif_make_tuple2(env, atom_error,
			enif_make_tuple4(env,
				enif_make_int(env,pos),
				enif_make_string(env,calledfrom,ERL_NIF_LATIN1),
				atom_error,
				enif_make_string(env, calledfrom, ERL_NIF_LATIN1)));
	}
}

// static void
// command_destroy(db_command cmd)
// {
//     if(cmd.env != NULL)
//        enif_free_env(cmd.env);
// }

static qitem* command_create(int writeThreadNum, int readThreadNum, priv_data *p)
{
	// queue *thrCmds = NULL;
	qitem *item;

	// if (writeThreadNum == -1 && readThreadNum == -1)
	// 	thrCmds = p->wtasks[p->nEnvs*p->nWriteThreads];
	// else if (writeThreadNum >= 0)
	// 	thrCmds = p->wtasks[writeThreadNum];
	// else
	// 	thrCmds = p->rtasks[readThreadNum];
	// thrCmds

	item = queue_get_item();
	if (!item)
		return NULL;
	if (item->cmd == NULL)
	{
		item->cmd = enif_alloc(sizeof(db_command));
	}
	memset(item->cmd,0,sizeof(db_command));

	return item;
}
void close_prepared(db_connection *conn)
{
	int i;
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
	conn->staticPrepared = NULL;
}

static void destruct_connection(ErlNifEnv *env, void *arg)
{
	int rc;
	db_connection *conn = (db_connection*)arg;
#ifdef _TESTDBG_
	if (conn->wal.lastCompleteEvnum > 0)
	{
		DBG("destruct connection actor=%llu",conn->wal.index);
	}
	else
	{
		DBG("destruct memory connection");
	}
#endif
	if (conn->packetPrefix)
	{
		free(conn->packetPrefix);
		conn->packetPrefix = NULL;
		conn->packetPrefixSize = 0;
	}
	if (conn->db)
	{
		close_prepared(conn);
		rc = sqlite3_close(conn->db);
		if(rc != SQLITE_OK)
		{
			DBG("ERROR! closing %d",rc);
		}
	}
	enif_mutex_destroy(conn->wal.mtx);
}

// static void
// destruct_backup(ErlNifEnv *env, void *arg)
// {
//     db_backup *p = (db_backup *)arg;
//     if (p->b)
//     {
//         void *item = command_create(p->thread);
//         db_command *cmd = queue_get_item_data(item);
//
//         cmd->type = cmd_backup_finish;
//         cmd->p = p;
//         cmd->ref = 0;
//
//         push_command(p->thread, item);
//     }
// }

static void destruct_iterate(ErlNifEnv *env, void *arg)
{
	qitem *item;
	iterate_resource *res = (iterate_resource*)arg;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd;

	DBG("destruct iterate.");
	if (res->closed)
		return;

	while( (item = command_create(res->thread,-1,pd)) == NULL)
	{
	}
	cmd = (db_command*)item->cmd;

	cmd->type = cmd_checkpoint_lock;
	cmd->arg = enif_make_int(item->env, 0);
	// No keep. This way release for connection will be called on thread when done with command.
	// It will decrement keep on connection held by iterate.
	cmd->conn = res->conn;

	push_command(res->thread, -1, pd, item);
}


static ERL_NIF_TERM do_open(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	static u32 rThrCounter = 0;
	char filename[MAX_PATHNAME];
	unsigned int size = 0;
	int rc;
	ERL_NIF_TERM result = atom_ok;
	db_connection *conn = NULL;
	char mode[10];
	sqlite3 *db = NULL;

	track_time(20,thread);

	memset(filename,0,MAX_PATHNAME);

	enif_get_atom(env,cmd->arg1,mode,10,ERL_NIF_LATIN1);
	if (strcmp(mode,"wal") != 0 && strcmp(mode, "blob") != 0)
		return atom_false;

	size = enif_get_string(env, cmd->arg, filename, MAX_PATHNAME, ERL_NIF_LATIN1);
	// Actor path name must be written to wal. Filename slot is MAX_ACTOR_NAME bytes.
	if(size <= 0 || size >= MAX_ACTOR_NAME)
		return make_error_tuple(env, "invalid_filename");

	track_time(21,thread);
	// Blob type uses lmdb directly.
	// Stores data in binary chunks (like pages of sqlite).
	// Blob connections have db=NULL
	if (strcmp(mode, "blob") != 0)
	{
		rc = sqlite3_open(filename,&(db));
		if(rc != SQLITE_OK)
		{
			result = make_sqlite3_error_tuple(env, "sqlite3_open", rc,0, cmd->conn->db);
			sqlite3_close(db);
			cmd->conn = NULL;
			return result;
		}
	}

	conn = enif_alloc_resource(db_connection_type, sizeof(db_connection));
	if(!conn)
	{
		return make_error_tuple(env, "no_memory");
	}
	memset(conn,0,sizeof(db_connection));

	track_time(22,thread);
	cmd->conn = conn;

	// thread->curConn = cmd->conn;

	g_tsd_conn = cmd->conn;

	conn->db = db;
	if (thread->isreadonly)
	{
		conn->rthreadind = thread->nEnv * g_pd->nReadThreads + thread->nThread;
		conn->wthreadind = thread->nEnv * g_pd->nWriteThreads + ((rThrCounter++) % g_pd->nWriteThreads);
	}
	else
	{
		conn->wthreadind = thread->nEnv * g_pd->nWriteThreads + thread->nThread;
		conn->rthreadind = thread->nEnv * g_pd->nReadThreads + ((rThrCounter++) % g_pd->nReadThreads);
	}
	conn->wal.mtx = enif_mutex_create("conmutex");

	if (filename[0] != ':' && db)
	{
		// PRAGMA locking_mode=EXCLUSIVE
		sqlite3_exec(cmd->conn->db,"PRAGMA synchronous=0;PRAGMA journal_mode=wal;",NULL,NULL,NULL);
	}
	else if (!db)
	{
		// open wal directly
		sqlite3WalOpen(NULL, NULL, filename, 0, 0, NULL);
	}
	if (g_pd->nPlugins && db)
	{
		int i;
		sqlite3_db_config(db,SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,1,NULL);

		for (i = 0; i < g_pd->nPlugins; i++)
			sqlite3_load_extension(db, g_pd->pluginFiles[i], NULL, NULL);
	}
	track_time(23,thread);
	DBG("opened new thread=%d name=%s mode=%s.",(int)thread->nThread,filename,mode);
	result = enif_make_resource(env, conn);

	return result;
}

static ERL_NIF_TERM do_interrupt(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	sqlite3_interrupt(cmd->conn->db);
	// enif_release_resource(cmd->conn);
	return atom_error;
}

static ERL_NIF_TERM do_iterate(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	ErlNifBinary bin;
	ERL_NIF_TERM tBin,tHead, tDone;
	ERL_NIF_TERM res;
	u64 evnumFrom, evtermFrom;
	iterate_resource *iter;
	int nfilled = 0;
	char dorel = 0;
	u32 done = 0;
	ErlNifBinary header;
	u8 buf[PAGE_BUFF_SIZE];
	u8 hdrbuf[sizeof(u64)*2+sizeof(u32)*2];
	u64 evterm;
	char mismatch = 0;

	if (!enif_get_resource(env, cmd->arg, iterate_type, (void **) &iter))
	{
		dorel = 1;
		DBG("Create iterate %d",(int)cmd->conn->checkpointLock);

		if (!enif_get_uint64(env,cmd->arg,(ErlNifUInt64*)&evtermFrom))
			return enif_make_badarg(env);
		if (cmd->arg1 == 0 || !enif_get_uint64(env,cmd->arg1,(ErlNifUInt64*)&evnumFrom))
		{
			return enif_make_badarg(env);
		}

		iter = enif_alloc_resource(iterate_type, sizeof(iterate_resource));
		if(!iter)
			return make_error_tuple(env, "no_memory");
		memset(iter,0,sizeof(iterate_resource));
		iter->thread = thread->nEnv * g_pd->nReadThreads + thread->nThread;
		iter->evnum = evnumFrom;
		iter->evterm = evtermFrom;
		iter->conn = cmd->conn;
		// Connection must not close before iterate is closed
		enif_keep_resource(iter->conn);
		// Creating a iterator requires checkpoint lock.
		// On iterator destruct lock will be released.
		cmd->conn->checkpointLock++;

		res = enif_make_resource(env,iter);
	}
	else
	{
		res = cmd->arg;
	}

	// 4 pages of buffer size
	// This might contain many more actual db pages because data is compressed
	nfilled = wal_iterate(&cmd->conn->wal, iter, buf, PAGE_BUFF_SIZE, hdrbuf, &done);
	DBG("nfilled %d",nfilled);

	if (nfilled > 0)
	{
		enif_alloc_binary(sizeof(u64)*2+sizeof(u32)*2, &header);
		enif_alloc_binary(nfilled, &bin);
		memcpy(bin.data, buf, nfilled);
		memcpy(header.data, hdrbuf, sizeof(hdrbuf));

		tBin = enif_make_binary(env,&bin);
		tHead = enif_make_binary(env,&header);
		enif_release_binary(&bin);
		enif_release_binary(&header);
	}
	evterm = iter->evterm;
	mismatch = iter->termMismatch;

	if (dorel)
	{
		enif_release_resource(iter);
	}

	if (nfilled == 0 && done == 1)
	{
		DBG("RET DONE");
		if (mismatch)
			return enif_make_tuple2(env, atom_ok, enif_make_uint64(env,evterm));
		return atom_done;
	}
	else
	{
		tDone = enif_make_uint(env,done);
		return enif_make_tuple5(env,atom_ok,
			enif_make_tuple2(env,atom_iter,res), tBin, tHead, tDone);
	}
}


static ERL_NIF_TERM do_wal_rewind(db_command *cmd, db_thread *thr, ErlNifEnv *env)
{
	// Rewind discards a certain number of writes. This may happen due to consensus conflicts.
	// Rewind is very similar to checkpoint. Checkpoint goes from beginning
	// (from firstCompleteEvterm/Evnum),forward to some limit.
	// Rewind goes from the end (from lastCompleteEvterm/Evnum)
	// backwards to some limit evnum (including the limit).
	// DB may get shrunk in the process.
	MDB_val logKey, logVal;
	u8 logKeyBuf[sizeof(u64)*3];
	u64 limitEvnum;
	Wal *pWal = &cmd->conn->wal;
	// mdbinf * const mdb = &thr->mdb;
	mdbinf *mdb;

	enif_get_uint64(env,cmd->arg,(ErlNifUInt64*)&limitEvnum);

	DBG("do_wal_rewind, actor=%llu, limit=%llu",pWal->index, limitEvnum);

	// We can not rewind that far back
	if (limitEvnum < pWal->firstCompleteEvnum && limitEvnum > 0)
		return atom_false;
	// We have nothing to do. Actor already empty.
	if (pWal->lastCompleteEvnum == 0 && limitEvnum == 0)
		return atom_ok;
	// We have nothing to do. This limit is higher than what is saved.
	if (limitEvnum > pWal->lastCompleteEvnum)
		return atom_ok;

	if (!g_tsd_wmdb)
		lock_wtxn(thr->nEnv);
	mdb = g_tsd_wmdb;
	if (!mdb)
		return SQLITE_ERROR;

	if (pWal->inProgressTerm > 0 || pWal->inProgressEvnum > 0)
	{
		DBG("undo before rewind, %llu, %llu",pWal->inProgressTerm, pWal->inProgressEvnum);
		doundo(&cmd->conn->wal, NULL, NULL, 1);
	}

	logKey.mv_data = logKeyBuf;
	logKey.mv_size = sizeof(logKeyBuf);

	// if limitEvnum == 0, this means delete all pages for actor.
	// Delete entries in logdb and pagesdb. Keep actordb and infodb. Infodb is important
	// for replication consistency. After deleting actor may get recreated later.
	if (limitEvnum == 0)
	{
		u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
		MDB_val pgKey, pgVal;
		u32 pgno = 1;
		int pgop = MDB_SET;

		memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
		memcpy(pagesKeyBuf + sizeof(u64), &pgno,       sizeof(u32));
		pgKey.mv_data = pagesKeyBuf;
		pgKey.mv_size = sizeof(pagesKeyBuf);
		while (mdb_cursor_get(mdb->cursorPages,&pgKey,&pgVal,pgop) == MDB_SUCCESS)
		{
			u64 aindex;
			memcpy(&aindex,pgKey.mv_data,sizeof(u64));
			if (aindex != pWal->index)
				break;
			mdb_cursor_del(mdb->cursorPages, MDB_NODUPDATA);
			pgop = MDB_NEXT_NODUP;
		}
		pWal->mxPage = 0;
		pWal->allPages = 0;

		memcpy(logKeyBuf,                 &pWal->index,          sizeof(u64));
		memcpy(logKeyBuf + sizeof(u64),   &pWal->firstCompleteTerm, sizeof(u64));
		memcpy(logKeyBuf + sizeof(u64)*2, &pWal->firstCompleteEvnum,sizeof(u64));
		if (mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_SET) == MDB_SUCCESS)
		{
			u64 aindex;

			mdb_cursor_del(mdb->cursorLog, MDB_NODUPDATA);
			while ((mdb_cursor_get(mdb->cursorLog,&logKey,&logVal,MDB_NEXT_NODUP)) == MDB_SUCCESS)
			{
				memcpy(&aindex, logKey.mv_data, sizeof(u64));
				if (pWal->index != aindex)
					break;
				mdb_cursor_del(mdb->cursorLog, MDB_NODUPDATA);
			}
			pWal->firstCompleteTerm = pWal->firstCompleteTerm = pWal->lastCompleteTerm =
			pWal->lastCompleteEvnum = 0;
		}
	}
	else
	{
		wal_rewind_int(mdb,pWal,limitEvnum);
	}
	DBG("evterm = %llu, evnum=%llu",pWal->lastCompleteTerm, pWal->lastCompleteEvnum);
	// no dirty pages, but will write info
	sqlite3WalFrames(pWal, SQLITE_DEFAULT_PAGE_SIZE, NULL, pWal->mxPage, 1, 0);
	cmd->conn->changed = 1;

	if (cmd->arg1 && limitEvnum == 0)
	{
		ErlNifBinary sqlbin;
		if (enif_inspect_iolist_as_binary(env, cmd->arg1, &sqlbin))
		{
			char *sqlstr = NULL;
			u8 repl = cmd->conn->doReplicate;

			if (sqlbin.size < 10000)
			{
				sqlstr = alloca(sqlbin.size+1);
				memcpy(sqlstr, sqlbin.data, sqlbin.size);
				sqlstr[sqlbin.size] = 0;

				cmd->conn->doReplicate = 0;
				sqlite3_exec(cmd->conn->db,sqlstr,NULL,NULL,NULL);
				cmd->conn->doReplicate = repl;
			}
		}
	}

	enif_mutex_lock(pWal->mtx);
	pWal->readSafeTerm = pWal->lastCompleteTerm;
	pWal->readSafeEvnum = pWal->lastCompleteEvnum;
	pWal->readSafeMxPage = pWal->readSafeEvnum;
	enif_mutex_unlock(pWal->mtx);
	// return enif_make_tuple2(env, atom_ok, enif_make_int(env,rc));
	return atom_ok;
}

static ERL_NIF_TERM do_term_store(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	ErlNifBinary votedFor;
	u64 currentTerm;

	if (!enif_get_uint64(env,cmd->arg1,(ErlNifUInt64*)&currentTerm))
		return atom_false;
	if (!enif_inspect_binary(env,cmd->arg2,&votedFor))
		return atom_false;

	if (votedFor.size > 128)
		return atom_false;

	if (cmd->conn)
	{
		storeinfo(&cmd->conn->wal, currentTerm, (u8)votedFor.size, votedFor.data);
		thread->pagesChanged++;
	}
	else
	{
		ErlNifBinary name;
		db_connection con;
		char pth[MAX_PATHNAME];

		if (!enif_inspect_iolist_as_binary(env, cmd->arg, &name))
			return atom_false;
		if (name.size >= MAX_PATHNAME-5)
			return atom_false;

		memset(&con, 0, sizeof(db_connection));
		memset(pth, 0, MAX_PATHNAME);
		memcpy(pth, name.data, name.size);
		g_tsd_conn = &con;
		sqlite3WalOpen(NULL, NULL, pth, 0, 0, NULL);
		// con.wal.thread = thread;
		storeinfo(&con.wal, currentTerm, (u8)votedFor.size, votedFor.data);
		thread->pagesChanged++;
		// thread->curConn = NULL;
		g_tsd_conn = NULL;
	}
	return atom_ok;
}

static ERL_NIF_TERM do_actor_info(db_command *cmd, db_thread *thr, ErlNifEnv *env)
{
	MDB_val key, data;
	ErlNifBinary bin;
	mdbinf * const mdb = &thr->mdb;
	// MDB_txn *txn;
	int rc;

	if (!enif_inspect_iolist_as_binary(env, cmd->arg, &bin))
		return make_error_tuple(env, "not_iolist");
	//
	// if (mdb_txn_begin(thr->env, NULL, MDB_RDONLY, &txn) != MDB_SUCCESS)
	// 	return SQLITE_ERROR;

	key.mv_size = bin.size;
	key.mv_data = bin.data;
	rc = mdb_get(mdb->txn,mdb->actorsdb,&key,&data);

	if (rc == MDB_NOTFOUND)
		return atom_false;
	else if (rc == MDB_SUCCESS)
	{
		key = data;
		rc = mdb_get(mdb->txn,mdb->infodb,&key,&data);

		if (rc == MDB_SUCCESS)
		{
			ErlNifBinary vfbin;
			ERL_NIF_TERM vft, fct, fce, lct, lce, ipt, ipe, ct, res;
			ERL_NIF_TERM fc, lc, in, mxt, allt;
			u8 *votedFor;
			u8 vfSize = 0;
			u32 mxPage;
			u32 allPages;
			u64 firstCompleteTerm,firstCompleteEvnum,lastCompleteTerm;
			u64 lastCompleteEvnum,inProgressTerm,inProgressEvnum, currentTerm;

			// DBG("Size=%zu, should=%lu",data.mv_size, sizeof(u64)*7+2+sizeof(u32));
			if (data.mv_size < sizeof(u64)*7+2+sizeof(u32))
				return atom_error;

			memcpy(&firstCompleteTerm,  (u8*)data.mv_data+1,               sizeof(u64));
			memcpy(&firstCompleteEvnum, (u8*)data.mv_data+1+sizeof(u64),   sizeof(u64));
			memcpy(&lastCompleteTerm,   (u8*)data.mv_data+1+sizeof(u64)*2, sizeof(u64));
			memcpy(&lastCompleteEvnum,  (u8*)data.mv_data+1+sizeof(u64)*3, sizeof(u64));
			memcpy(&inProgressTerm,     (u8*)data.mv_data+1+sizeof(u64)*4, sizeof(u64));
			memcpy(&inProgressEvnum,    (u8*)data.mv_data+1+sizeof(u64)*5, sizeof(u64));
			memcpy(&mxPage,             (u8*)data.mv_data+1+sizeof(u64)*6, sizeof(u32));
			memcpy(&allPages,           (u8*)data.mv_data+1+sizeof(u64)*6+sizeof(u32), sizeof(u32));
			memcpy(&currentTerm, (u8*)data.mv_data+1+sizeof(u64)*6+sizeof(u32)*2, sizeof(u64));
			vfSize = ((u8*)data.mv_data)[1+sizeof(u64)*7+sizeof(u32)*2];
			votedFor = (u8*)data.mv_data+2+sizeof(u64)*7+sizeof(u32)*2;

			enif_alloc_binary(vfSize, &vfbin);
			memcpy(vfbin.data, votedFor, vfSize);
			vft = enif_make_binary(env,&vfbin);
			enif_release_binary(&vfbin);

			fct = enif_make_uint64(env, firstCompleteTerm);
			fce = enif_make_uint64(env, firstCompleteEvnum);
			lct = enif_make_uint64(env, lastCompleteTerm);
			lce = enif_make_uint64(env, lastCompleteEvnum);
			ipt = enif_make_uint64(env, inProgressTerm);
			ipe = enif_make_uint64(env, inProgressEvnum);
			ct  = enif_make_uint64(env, currentTerm);

			fc = enif_make_tuple2(env, fct, fce);
			lc = enif_make_tuple2(env, lct, lce);
			in = enif_make_tuple2(env, ipt, ipe);
			allt = enif_make_uint(env, allPages);
			mxt = enif_make_uint(env, mxPage);

			res = enif_make_tuple7(env,fc,lc,in,mxt,allt,ct,vft);
			return res;
		}
		else
			return atom_false;
	}
	else
		return atom_error;
}

static ERL_NIF_TERM do_actorsdb_add(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	u64 index;
	char name[MAX_PATHNAME];

	enif_get_string(env,cmd->arg, name, sizeof(name), ERL_NIF_LATIN1);
	enif_get_uint64(env,cmd->arg1,(ErlNifUInt64*)&(index));

	if (register_actor(index, name) == SQLITE_ERROR)
		return atom_false;

	return atom_ok;
}

static ERL_NIF_TERM do_sync(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	if (g_transsync || (cmd->conn && cmd->conn->syncNum < g_tsd_cursync))
		return atom_ok;

	if (!g_tsd_wmdb)
		lock_wtxn(thread->nEnv);

	return atom_false;
}

static ERL_NIF_TERM do_inject_page(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	ErlNifBinary bin;
	u32 commit;
	u64 evterm,evnum;
	int rc;
	u8 pbuf[SQLITE_DEFAULT_PAGE_SIZE];
	PgHdr page;
	int doreplicate = cmd->conn->doReplicate;
	ErlNifBinary header;
	Wal *pWal = &cmd->conn->wal;

	if (!enif_is_binary(env,cmd->arg))
		return make_error_tuple(env,"not_bin");

	if (!enif_is_binary(env,cmd->arg1))
		return make_error_tuple(env,"hdr_not_bin");

	memset(&page,0,sizeof(page));
	enif_inspect_binary(env,cmd->arg,&bin);
	page.pData = pbuf;
	enif_inspect_binary(env,cmd->arg1,&header);

	if (header.size != sizeof(u64)*2+sizeof(u32)*2)
		return make_error_tuple(env,"bad_hdr_size");

	evterm = get8byte(header.data);
	evnum = get8byte(header.data+sizeof(u64));
	page.pgno = get4byte(header.data+sizeof(u64)*2);
	commit = get4byte(header.data+sizeof(u64)*2+sizeof(u32));

	// If inprogress > 0 and does not match input evterm/evnum, than we have leftowers
	// from a failed inject. We must clean up. During a single iteration or write, evnum/evterm does not change.
	if (pWal->inProgressTerm + pWal->inProgressEvnum > 0 &&
		(pWal->inProgressTerm != evterm || pWal->inProgressEvnum != evnum))
	{
		doundo(&cmd->conn->wal,NULL,NULL,1);
	}
	else if (pWal->inProgressTerm + pWal->inProgressEvnum == 0)
	{
		pWal->inProgressTerm = evterm;
		pWal->inProgressEvnum = evnum;
	}

	rc = LZ4_decompress_safe((char*)(bin.data),(char*)pbuf,bin.size,sizeof(pbuf));
	if (rc != sizeof(pbuf))
	{
		if (bin.size == sizeof(pbuf))
		{
			memcpy(pbuf,bin.data,sizeof(pbuf));
		}
		else
		{
			DBG("Unable to decompress inject page!! %ld",bin.size);
			return make_error_tuple(env,"cant_decompress");
		}
	}
	cmd->conn->doReplicate = 0;
	rc = sqlite3WalFrames(pWal, sizeof(pbuf), &page, commit, commit, 0);
	cmd->conn->doReplicate = doreplicate;
	if (rc != SQLITE_OK)
	{
		DBG("Unable to write inject page");
		return make_error_tuple(env,"cant_inject");
	}
	enif_mutex_lock(pWal->mtx);
	pWal->readSafeTerm = cmd->conn->wal.lastCompleteTerm;
	pWal->readSafeEvnum = cmd->conn->wal.lastCompleteEvnum;
	pWal->readSafeMxPage = cmd->conn->wal.mxPage;
	enif_mutex_unlock(pWal->mtx);
	cmd->conn->changed = 1;
	return atom_ok;
}

static ERL_NIF_TERM do_checkpoint(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	ErlNifUInt64 evnum;
	db_connection *con = cmd->conn;
	int rc;

	enif_get_uint64(env,cmd->arg,(ErlNifUInt64*)&(evnum));

	if (con->wal.firstCompleteEvnum >= evnum || con->checkpointLock)
	{
		cmd->type = cmd_unknown;
		return atom_ok;
	}

	rc = checkpoint(&con->wal, evnum);
	if (rc == SQLITE_OK)
		return atom_ok;
	else if (rc == SQLITE_DONE)
	{
		cmd->type = cmd_unknown;
		return atom_ok;
	}
	else
		return atom_false;
}

static ERL_NIF_TERM do_stmt_info(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	int rc;
	ErlNifBinary bin;
	int ncols = 0, nparam = 0;
	ERL_NIF_TERM res;
	sqlite3_stmt *statement = NULL;

	enif_inspect_iolist_as_binary(env,cmd->arg,&bin);
	if (bin.size == 0)
		return enif_make_int(env,0);

	if (bin.size >= 6 && bin.data[0] == '#' && (bin.data[1] == 'r' || bin.data[1] == 'w'))
	{
		int i,rowLen;

		i = (bin.data[2] - '0')*10 + (bin.data[3] - '0');
		rowLen = (bin.data[4] - '0')*10 + (bin.data[5] - '0');

		enif_mutex_lock(g_pd->prepMutex);

		if (g_pd->prepSize <= i)
		{
			enif_mutex_unlock(g_pd->prepMutex);
			return atom_false;
		}

		if (g_pd->prepSqls[i][rowLen] == NULL)
		{
			DBG("Did not find sql in prepared statement");
			enif_mutex_unlock(g_pd->prepMutex);
			return atom_false;
		}

		rc = sqlite3_prepare_v2(cmd->conn->db, g_pd->prepSqls[i][rowLen], -1, &statement, NULL);

		enif_mutex_unlock(g_pd->prepMutex);
	}
	else
	{
		rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(bin.data), bin.size, &statement, NULL);
	}

	if (rc != SQLITE_OK)
		return atom_false;

	nparam = sqlite3_bind_parameter_count(statement);
	ncols = sqlite3_column_count(statement);

	sqlite3_finalize(statement);

	res = enif_make_tuple3(env,atom_ok,enif_make_int(env,nparam),enif_make_int(env,ncols));

	return res;
}

// static ERL_NIF_TERM do_set_socket(db_command *cmd, db_thread *thread, ErlNifEnv *env)
static ERL_NIF_TERM do_set_socket(db_thread *thread)
{
	while (1)
	{
		int opts;
		int fd = 0;
		int pos = -1;
		int type = 1;
		int i;
		struct timeval send_time;

		enif_mutex_lock(g_pd->sockMutex);
		for (i = 0; i < MAX_CONNECTIONS; i++)
		{
			int index = thread->nEnv * g_pd->nWriteThreads * MAX_CONNECTIONS + thread->nThread * MAX_CONNECTIONS + i;
			int sfd = g_pd->sockets[index];
			if (thread->sockets[i] != sfd)
			{
				fd = sfd;
				pos = i;
				type = g_pd->socketTypes[index];
				break;
			}
		}
		enif_mutex_unlock(g_pd->sockMutex);
		if (fd == 0)
		{
			return atom_ok;
		}
		DBG("Write thread %d update socket %d %d", thread->nThread, pos, fd);

		// if (!enif_get_int(env,cmd->arg,&fd))
		// 	return atom_error;
		// if (!enif_get_int(env,cmd->arg1,&pos))
		// 	return atom_error;
		// if (!enif_get_int(env,cmd->arg2,&type))
		// 	return atom_error;

	#ifndef _WIN32
		opts = fcntl(fd,F_GETFL);
		if (fcntl(fd, F_SETFL, opts & (~O_NONBLOCK)) == -1 || fcntl(fd,F_GETFL) & O_NONBLOCK)
	#else
		opts = 0;
		if (ioctlsocket(fd, FIONBIO, &opts) != 0)
	#endif
		{
			DBG("Can not set to blocking socket");
			fail_send(pos);
			return atom_false;
		}
	#ifdef SO_NOSIGPIPE
		opts = 1;
		if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&opts, sizeof(int)) != 0)
		{
			DBG("Unable to set nosigpipe");
			fail_send(pos);
			return atom_false;
		}
	#endif
		if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&opts, sizeof(int)) != 0)
		{
			DBG("Unable to set socket nodelay");
			fail_send(pos);
			return atom_false;
		}
		send_time.tv_sec=2;
		send_time.tv_usec=0;
		if (setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&send_time,sizeof(send_time)) != 0)
		{
			DBG("Unable to set socket send timeout");
			fail_send(pos);
			return atom_false;
		}

		thread->sockets[pos] = fd;
		thread->socket_types[pos] = type;
	}


	return atom_ok;
}

static ERL_NIF_TERM do_exec_script(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	u32 pagesPre = thread->pagesChanged;
	int rc = 0,i;
	u64 newTerm,newEvnum;
	ERL_NIF_TERM *stackArray = NULL;
	sqlite3_stmt *statement = NULL;
	char *errat = "";
	ERL_NIF_TERM results;
	const ERL_NIF_TERM *inputTuple = NULL;
	const ERL_NIF_TERM *tupleRecs = NULL;
	ERL_NIF_TERM *tupleResult = NULL;
	ERL_NIF_TERM response = atom_ok;
	int tupleSize = 0, tuplePos = 0, tupleRecsSize = 0;
	int skip = 0;
	u32 mxPage = cmd->conn->wal.mxPage;
	char dofinalize = 1;

	thread->execConn = cmd->conn;

	if (cmd->arg1)
	{
		enif_get_uint64(env,cmd->arg1,(ErlNifUInt64*)&(newTerm));
		enif_get_uint64(env,cmd->arg2,(ErlNifUInt64*)&(newEvnum));
		enif_inspect_binary(env,cmd->arg3,&(cmd->conn->packetVarPrefix));
	}

	if ((cmd->conn->wal.inProgressTerm > 0 || cmd->conn->wal.inProgressEvnum > 0) && cmd->arg1)
	{
		DBG("undo before exec, %llu, %llu",
			cmd->conn->wal.inProgressTerm,cmd->conn->wal.inProgressEvnum);
		doundo(&cmd->conn->wal, NULL, NULL, 1);
		// mdb_txn_abort(thread->wtxn);
		// open_wtxn(thread);
	}
	if (cmd->arg1)
	{
		cmd->conn->wal.inProgressTerm = newTerm;
		cmd->conn->wal.inProgressEvnum = newEvnum;
	}
	// else
	// {
	// 	// Copy over safe read limits.
	// 	enif_mutex_lock(cmd->conn->wal.mtx);
	// 	thread->readSafeTerm = cmd->conn->wal.readSafeTerm;
	// 	thread->readSafeEvnum = cmd->conn->wal.readSafeEvnum;
	// 	enif_mutex_unlock(cmd->conn->wal.mtx);
	// }

	if (enif_get_tuple(env, cmd->arg, &tupleSize, &inputTuple))
	{
		if (cmd->arg4 != 0 && !enif_get_tuple(env, cmd->arg4, &tupleRecsSize, &tupleRecs))
			return atom_false;
		if (cmd->arg4 != 0 && tupleRecsSize != tupleSize)
			return atom_false;

		if (tupleSize > 200)
			tupleResult = malloc(sizeof(ERL_NIF_TERM)*tupleSize);
		else
			tupleResult = alloca(sizeof(ERL_NIF_TERM)*tupleSize);
	}

	do{
		ErlNifBinary bin;
		unsigned int rowcount = 0;
		const char *readpoint;
		const char *end;
		int column_count;
		ERL_NIF_TERM column_names = 0;
		// insert records are a list of lists.
		ERL_NIF_TERM listTop = 0, headTop = 0, headBot = 0;
		int statementlen = 0;
		ERL_NIF_TERM rows;
		const ERL_NIF_TERM *insertRow;
		int rowLen = 0;
		dofinalize = 1;

		track_flag(thread,1);
		track_time(8,thread);

		results = enif_make_list(env,0);

		if (inputTuple)
		{
			if (tupleRecs)
				listTop = tupleRecs[tuplePos];
			else
				listTop = 0;

			// use headTop as a tmp variable to store input sql
			headTop = inputTuple[tuplePos];
		}
		else
		{
			listTop = cmd->arg4;
			headTop = cmd->arg;
		}

		if (cmd->conn->db)
		{
			if (!enif_inspect_iolist_as_binary(env, headTop, &bin))
				// return make_error_tuple(env, "not_iolist");
				return make_sqlite3_error_tuple(env, "not_iolist", 0, tuplePos, cmd->conn->db);
		}
		else
		{
			PgHdr pg;
			rc = SQLITE_OK;
			skip = 1;
			memset(&pg,0,sizeof(PgHdr));

			if (!enif_get_uint(env,headTop, &pg.pgno))
				// return make_error_tuple(env,"not_pgno");
				return make_sqlite3_error_tuple(env, "not_pgno", 0, tuplePos, cmd->conn->db);

			// Blob storage
			if (listTop == 0)
			{
				// this is read
				u32 foundFrame = 0;
				ErlNifBinary binOut;
				ERL_NIF_TERM termbin;
				int actualsize;

				DBG("BLOB STORAGE READ!");

				results = enif_make_list(env,0);
				sqlite3WalFindFrame(&cmd->conn->wal, pg.pgno, &foundFrame);
				if (foundFrame)
				{
					enif_alloc_binary(SQLITE_DEFAULT_PAGE_SIZE,&binOut);
					actualsize = readframe(&cmd->conn->wal, 1, binOut.size, binOut.data);
					if (actualsize != SQLITE_DEFAULT_PAGE_SIZE)
						enif_realloc_binary(&binOut, actualsize);

					termbin = enif_make_binary(env,&binOut);
					enif_release_binary(&binOut);

					results = enif_make_list_cell(env,termbin,results);
				}

				if (tuplePos < tupleSize)
					tupleResult[tuplePos] = results;
			}
			else
			{
				ErlNifBinary pageBody;
				u32 commit = 0;

				DBG("BLOB STORAGE WRITE!");

				// this is write
				if (!enif_inspect_iolist_as_binary(env,listTop,&pageBody))
					return make_error_tuple(env,"not_iolist");

				if (pageBody.size > SQLITE_DEFAULT_PAGE_SIZE)
					return make_error_tuple(env,"too_large");

				pg.pData = pageBody.data;
				mxPage = pg.pgno > mxPage ? pg.pgno : mxPage;
				if (tupleSize <= tuplePos+1)
					commit = 1;

				//DBG("write blob page pos=%d mxpage=%u, commit=%u, sz=%d",
					//tuplePos,mxPage,commit, pageBody.size);

				if (sqlite3WalFrames(&cmd->conn->wal, pageBody.size, &pg, mxPage, commit, 0) != SQLITE_OK)
					return make_error_tuple(env,"write_error");

				if (tuplePos < tupleSize)
					tupleResult[tuplePos] = results;
			}
			tuplePos++;
			if (tupleSize <= tuplePos)
				break;
			continue;
		}
		headTop = 0;

	#ifdef _TESTDBG_
		if (bin.size > 1024*10)
		{
			DBG("Executing actor=%llu %.*s...",cmd->conn->wal.index, 1024*10,bin.data);
		}
		else
		{
			DBG("Executing actor=%llu %.*s",cmd->conn->wal.index,(int)bin.size,bin.data);
		}
	#endif
		end = (char*)bin.data + bin.size;
		readpoint = (char*)bin.data;

		while (readpoint < end || headTop != 0)
		{
			if (readpoint[0] == '$')
				skip = 1;
			else if (headTop == 0)
				skip = 0;
			statementlen = end-readpoint;

			// if _insert, then this is a prepared statement with multiple rows in arg4
			if (headTop == 0 && statementlen >= 8 && cmd->arg4 && readpoint[skip] == '_' &&
				(readpoint[skip+1] == 'i' || readpoint[skip+1] == 'I') &&
				(readpoint[skip+2] == 'n' || readpoint[skip+2] == 'N'))
			{
				skip++;
				rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(readpoint+skip), statementlen,
					&(statement), &readpoint);
				if(rc != SQLITE_OK)
				{
					errat = "_prepare";
					sqlite3_finalize(statement);
					statement = NULL;
					break;
				}
				rc = SQLITE_DONE;

				if (!enif_get_list_cell(env, listTop, &headTop, &listTop))
				{
					rc = SQLITE_INTERRUPT;
					sqlite3_finalize(statement);
					statement = NULL;
					break;
				}

				// Move over a list of records.
				// First element is ignored as it is presumed to be record name.
				while (rc == SQLITE_DONE && enif_get_list_cell(env, headTop, &headBot, &headTop))
				{
					if (!enif_get_tuple(env, headBot, &rowLen, &insertRow) && rowLen > 1 && rowLen < 100)
					{
						rc = SQLITE_INTERRUPT;
						break;
					}

					for (i = 1; i < rowLen; i++)
					{
						if (bind_cell(env, insertRow[i], statement, i) == -1)
						{
							errat = "cant_bind";
							sqlite3_finalize(statement);
							statement = NULL;
							break;
						}
					}
					rc = sqlite3_step(statement);
					sqlite3_reset(statement);
				}
			}
			else
			{
				// static prepared statements
				if (headTop == 0 && statementlen >= 5 && readpoint[skip] == '#' &&
					(readpoint[skip+1] == 's' || readpoint[skip+1] == 'd') && readpoint[skip+4] == ';')
				{
					dofinalize = 0;
					i = (readpoint[skip+2] - '0')*10 + (readpoint[skip+3] - '0');
					if (readpoint[skip+1] == 's')
						skip = 1;

					if (cmd->conn->staticPrepared == NULL)
					{
						cmd->conn->staticPrepared = malloc(sizeof(sqlite3_stmt*)*MAX_STATIC_SQLS);
						memset(cmd->conn->staticPrepared,0,sizeof(sqlite3_stmt*)*MAX_STATIC_SQLS);
					}

					if (cmd->conn->staticPrepared[i] == NULL)
					{
						rc = sqlite3_prepare_v2(cmd->conn->db, (char *)thread->staticSqls[i], -1,
							&(cmd->conn->staticPrepared[i]), NULL);
						if(rc != SQLITE_OK)
						{
							errat = "prepare";
							break;
						}
					}
					readpoint += 5;
					statement = cmd->conn->staticPrepared[i];
				}
				// user set prepared statements
				else if (headTop == 0  && statementlen >= 6 && readpoint[skip] == '#' &&
					(readpoint[skip+1] == 'r' || readpoint[skip+1] == 'w') && readpoint[skip+6] == ';')
				{
					dofinalize = 0;
					// actor type index
					i = (readpoint[skip+2] - '0')*10 + (readpoint[skip+3] - '0');
					// statement index
					rowLen = (readpoint[skip+4] - '0')*10 + (readpoint[skip+5] - '0');

					enif_mutex_lock(g_pd->prepMutex);

					if (g_pd->prepSize <= i)
					{
						errat = "prepare";
						enif_mutex_unlock(g_pd->prepMutex);
						break;
					}

					if (cmd->conn->prepared == NULL)
					{
						cmd->conn->prepared = malloc(MAX_PREP_SQLS*sizeof(sqlite3_stmt*));
						memset(cmd->conn->prepared,0,MAX_PREP_SQLS*sizeof(sqlite3_stmt*));
						cmd->conn->prepVersions = malloc(MAX_PREP_SQLS*sizeof(int));
						memset(cmd->conn->prepVersions,0,MAX_PREP_SQLS*sizeof(int));
					}

					if (cmd->conn->prepared[rowLen] == NULL ||
						cmd->conn->prepVersions[rowLen] != g_pd->prepVersions[i][rowLen])
					{
						if (g_pd->prepSqls[i][rowLen] == NULL)
						{
							DBG("Did not find sql in prepared statement");
							errat = "prepare";
							enif_mutex_unlock(g_pd->prepMutex);
							break;
						}
						if (cmd->conn->prepared[rowLen] != NULL)
							sqlite3_finalize(cmd->conn->prepared[rowLen]);

						rc = sqlite3_prepare_v2(cmd->conn->db, g_pd->prepSqls[i][rowLen], -1,
							&(cmd->conn->prepared[rowLen]), NULL);
						if(rc != SQLITE_OK)
						{
							DBG("Prepared statement failed");
							errat = "prepare";
							enif_mutex_unlock(g_pd->prepMutex);
							break;
						}
						cmd->conn->prepVersions[rowLen] = g_pd->prepVersions[i][rowLen];
					}
					enif_mutex_unlock(g_pd->prepMutex);
					readpoint += 7;
					statement = cmd->conn->prepared[rowLen];
				}
				else if (headTop == 0)
				{
					dofinalize = 1;
					// #ifdef _TESTDBG_
					//     if (statementlen > 1024)
					//     {
					//         DBG("Executing %.*s",1024,readpoint+skip);
					//     }
					//     else
					//     {
					//         DBG("Executing %.*s",(int)statementlen,readpoint+skip);
					//     }
					// #endif
					rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(readpoint+skip), statementlen,
						&statement, &readpoint);
					if(rc != SQLITE_OK)
					{
						DBG("Prepare statement failed");
						errat = "prepare";
						sqlite3_finalize(statement);
						statement = NULL;
						break;
					}
				}

				if (headTop != 0 || sqlite3_bind_parameter_count(statement))
				{
					// Single prepared statement can have multiple rows that it wants to execute
					// We execute one at a time. If headTop /= 0, we are still in previous statement.
					if (headTop == 0)
					{
						// List can be:
						// [[Tuple1,Tuple2],...] -> for every statement, list of rows.
						// [[[Column1,Column2,..],..],...] -> for every statement,
						// 		for every row, list of columns.
						if (!enif_get_list_cell(env, listTop, &headTop, &listTop))
						{
							rc = SQLITE_INTERRUPT;
							break;
						}
					}

					// Every row is a list. It can be a list of tuples (which implies records
					// thus we start at offset 1), or a list of columns.
					if (enif_get_list_cell(env, headTop, &headBot, &headTop))
					{
						// If tuple bind from tuple
						if (enif_is_tuple(env, headBot))
						{
							if (!enif_get_tuple(env, headBot, &rowLen, &insertRow) &&
								rowLen > 1 && rowLen < 100)
							{
								rc = SQLITE_INTERRUPT;
								break;
							}

							for (i = 1; i < rowLen; i++)
							{
								if (bind_cell(env, insertRow[i], statement, i) == -1)
								{
									errat = "cant_bind";
									rc = SQLITE_INTERRUPT;
									break;
								}
							}
						}
						// If list, bind from list
						else if (enif_is_list(env,headBot))
						{
							ERL_NIF_TERM rowHead;
							// Index is from 1 because sqlite bind param start with 1 not 0
							for (i = 1; enif_get_list_cell(env, headBot, &rowHead, &headBot); i++)
							{
								if (bind_cell(env, rowHead, statement, i) == -1)
								{
									errat = "cant_bind";
									rc = SQLITE_INTERRUPT;
									break;
								}
							}
						}
						if (rc == SQLITE_INTERRUPT)
						{
							break;
						}
					}
					else
					{
						dofinalize ? sqlite3_finalize(statement) : sqlite3_reset(statement);
						headTop = 0;
						column_names = 0;
						statement = NULL;
						continue;
					}
				}

				column_count = sqlite3_column_count(statement);
				if (column_count > 0 && column_names == 0)
				{
					ERL_NIF_TERM *array;
					if (column_count > 30)
					{
						if (thread->columnSpaceSize < column_count)
						{
							thread->columnSpace = realloc(thread->columnSpace,
								column_count*sizeof(ERL_NIF_TERM));
							thread->columnSpaceSize = column_count;
						}
						array = thread->columnSpace;
					}
					else
					{
						if (!stackArray)
							stackArray = alloca(sizeof(ERL_NIF_TERM)*30);
						array = stackArray;
					}

					for(i = 0; i < column_count; i++)
					{
						const char* cname = sqlite3_column_name(statement, i);
						array[i] = make_binary(env, cname,strlen(cname));
					}

					column_names = enif_make_tuple_from_array(env, array, column_count);
				}
				if (!sqlite3_stmt_readonly(statement) && thread->isreadonly)
				{
					errat = "notreadonly";
					rc = 1;
					break;
				}

				rows = enif_make_list(env,0);
				rowcount = 0;
				track_time(1,thread);
				while ((rc = sqlite3_step(statement)) == SQLITE_ROW)
				{
					ERL_NIF_TERM *array = NULL;
					if (column_count > 30)
					{
						array = thread->columnSpace;
					}
					else
					{
						array = stackArray;
					}

					for(i = 0; i < column_count; i++)
						array[i] = make_cell(env, statement, i);

					rows = enif_make_list_cell(env, enif_make_tuple_from_array(env,
						array, column_count), rows);
					rowcount++;
				}
				track_time(6,thread);
				// track_flag(thread,0);
			}
			DBG("exec rc=%d, rowcount=%d, column_count=%d, skip=%d", rc, rowcount, column_count,(int)skip);
			if (rc > 0 && rc < 100)
			{
				errat = "step";
				dofinalize ? sqlite3_finalize(statement) : sqlite3_reset(statement);
				statement = NULL;
				break;
			}
			if (skip == 0 && (rowcount > 0 || column_count > 0))
			{
				ERL_NIF_TERM cols = enif_make_tuple2(env,atom_columns,column_names);
				ERL_NIF_TERM rowst = enif_make_tuple2(env,atom_rows,rows);
				ERL_NIF_TERM res1 = enif_make_list2(env,cols,rowst);
				results = enif_make_list_cell(env, res1,results);
			}
			else if (skip == 0)
			{
				ERL_NIF_TERM nchanges = enif_make_int(env,sqlite3_changes(cmd->conn->db));
				ERL_NIF_TERM lirowid = enif_make_int64(env,sqlite3_last_insert_rowid(cmd->conn->db));
				ERL_NIF_TERM changes = enif_make_tuple3(env,atom_changes,lirowid,nchanges);
				results = enif_make_list_cell(env, changes, results);
			}
			// if we are not done with prepared statement (because it has multiple invocations with prepared statements)
			if (headTop != 0)
			{
				sqlite3_reset(statement);
				continue;
			}
			dofinalize ? sqlite3_finalize(statement) : sqlite3_reset(statement);
			statement = NULL;
			column_names = 0;
		}
		if (rc > 0 && rc < 100)
			break;
		if (tupleResult)
		{
			tupleResult[tuplePos] = results;
		}
		tuplePos++;
		DBG("Tuple pos=%d, size=%d",tuplePos, tupleSize);
	} while (tuplePos < tupleSize);
	track_time(12,thread);

	thread->execConn = NULL;

	if (tupleResult && !(rc > 0 && rc < 100))
	{
		results = enif_make_tuple_from_array(env, tupleResult, tupleSize);
		if (tupleSize > 200)
			free(tupleResult);
	}
	else if (rc > 0 && rc < 100)
	{
		if (rc == SQLITE_INTERRUPT)
			response = make_sqlite3_error_tuple(env, "query_aborted", rc, tuplePos, cmd->conn->db);
		else
			response = make_sqlite3_error_tuple(env, errat, rc, tuplePos, cmd->conn->db);
	}

	if (statement != NULL)
		dofinalize ? sqlite3_finalize(statement) : sqlite3_reset(statement);

	if (rc > 0 && rc < 100 && !thread->isreadonly && cmd->conn->db)
	{
		DBG("Rollback err=%d",rc);
		sqlite3_prepare_v2(cmd->conn->db, "ROLLBACK;", strlen("ROLLBACK;"), &statement, NULL);
		sqlite3_step(statement);
		sqlite3_finalize(statement);
		statement = NULL;
	}

	// enif_release_resource(cmd->conn);
	if (response != atom_ok)
	{
		return response;
	}
	else
	{
		if (pagesPre != thread->pagesChanged)
		{
			// cmd->conn->syncNum = g_pd->syncNumbers[thread->nEnv];
			cmd->conn->syncNum = g_tsd_cursync;
		}
		track_time(13,thread);
		return make_ok_tuple(env,results);
	}
}


static int bind_cell(ErlNifEnv *env, const ERL_NIF_TERM cell, sqlite3_stmt *stmt, unsigned int i)
{
	int the_int;
	ErlNifSInt64 the_long_int;
	double the_double;
	char the_atom[MAX_ATOM_LENGTH+1];
	ErlNifBinary the_blob;
	int arity;
	const ERL_NIF_TERM* tuple;

	if(enif_get_int(env, cell, &the_int))
	{
		return sqlite3_bind_int(stmt, i, the_int);
	}


	if(enif_get_int64(env, cell, &the_long_int))
		return sqlite3_bind_int64(stmt, i, the_long_int);

	if(enif_get_double(env, cell, &the_double))
		return sqlite3_bind_double(stmt, i, the_double);

	if(enif_get_atom(env, cell, the_atom, sizeof(the_atom), ERL_NIF_LATIN1))
	{
		if(strcmp("undefined", the_atom) == 0)
			return sqlite3_bind_null(stmt, i);
		else if (strcmp("false",the_atom) == 0)
			return sqlite3_bind_int(stmt, i, 0);
		else if (strcmp("true",the_atom) == 0)
			return sqlite3_bind_int(stmt, i, 1);

		return sqlite3_bind_text(stmt, i, the_atom, strlen(the_atom), SQLITE_TRANSIENT);
	}

	/* Bind as text assume it is utf-8 encoded text */
	if(enif_inspect_iolist_as_binary(env, cell, &the_blob))
	{
		return sqlite3_bind_text(stmt, i, (char *) the_blob.data, the_blob.size, SQLITE_TRANSIENT);
	}


	/* Check for blob tuple */
	if(enif_get_tuple(env, cell, &arity, &tuple))
	{
		if(arity != 2)
			return -1;

		if(enif_get_atom(env, tuple[0], the_atom, sizeof(the_atom), ERL_NIF_LATIN1))
		{
			if(0 == strncmp("blob", the_atom, strlen("blob")))
			{
				if(enif_inspect_iolist_as_binary(env, tuple[1], &the_blob))
				{
					return sqlite3_bind_blob(stmt, i, the_blob.data, the_blob.size, SQLITE_TRANSIENT);
				}
			}
		}
	}

	return -1;
}



static ERL_NIF_TERM make_binary(ErlNifEnv *env, const void *bytes, unsigned int size)
{
	ErlNifBinary blob;
	ERL_NIF_TERM term;

	if(!enif_alloc_binary(size, &blob))
	{
		return atom_error;
	}

	memcpy(blob.data, bytes, size);
	term = enif_make_binary(env, &blob);
	enif_release_binary(&blob);

	return term;
}

static ERL_NIF_TERM
make_cell(ErlNifEnv *env, sqlite3_stmt *statement, unsigned int i)
{
	int type = sqlite3_column_type(statement, i);

	switch(type) {
	case SQLITE_INTEGER:
		return enif_make_int64(env, sqlite3_column_int64(statement, i));
	case SQLITE_FLOAT:
		return enif_make_double(env, sqlite3_column_double(statement, i));
	case SQLITE_BLOB:
		return enif_make_tuple2(env, atom_blob,
				make_binary(env, sqlite3_column_blob(statement, i),
					sqlite3_column_bytes(statement, i)));
	case SQLITE_NULL:
		return atom_undefined;
	case SQLITE_TEXT:
		return make_binary(env, sqlite3_column_text(statement, i),
			sqlite3_column_bytes(statement, i));
	default:
		return make_atom(env, "should_not_happen");
	}
}

static ERL_NIF_TERM do_checkpoint_lock(db_command *cmd,db_thread *thread, ErlNifEnv *env)
{
	int lock;
	enif_get_int(env,cmd->arg,&lock);

	DBG("Checkpoint lock now %d, lockin=%d.",cmd->conn->checkpointLock,lock);

	if (lock > 0)
		cmd->conn->checkpointLock++;
	else if (cmd->conn->checkpointLock > 0)
		cmd->conn->checkpointLock--;
	return atom_ok;
}

static ERL_NIF_TERM evaluate_command(db_command *cmd, db_thread *thread, ErlNifEnv *env)
{
	// thread->curConn = cmd->conn;
	g_tsd_conn = cmd->conn;

	switch(cmd->type)
	{
	case cmd_open:
	{
		ERL_NIF_TERM connres = do_open(cmd, thread,env);
		if (cmd->conn != NULL && cmd->arg2 == 0)
			return enif_make_tuple2(env,atom_ok,connres);
		else if (cmd->conn == NULL)
			return connres;
		else if (cmd->arg2 != 0)
		{
			cmd->arg = cmd->arg2;
			cmd->arg1 = 0;
			cmd->arg2 = 0;
			return enif_make_tuple3(env,atom_ok,connres,do_exec_script(cmd,thread,env));
		}
	}
	case cmd_exec_script:
		return do_exec_script(cmd,thread,env);
	// case cmd_store_prepared:
	// 	return do_store_prepared_table(cmd,thread);
	case cmd_checkpoint:
		return do_checkpoint(cmd,thread,env);
	case cmd_sync:
		return do_sync(cmd,thread,env);
	case cmd_inject_page:
		return do_inject_page(cmd,thread,env);
	case cmd_actor_info:
		return do_actor_info(cmd,thread,env);
	case cmd_wal_rewind:
		return do_wal_rewind(cmd,thread,env);
	case cmd_stmt_info:
		return do_stmt_info(cmd,thread,env);
	case cmd_interrupt:
		return do_interrupt(cmd,thread,env);
	case cmd_iterate:
		return do_iterate(cmd,thread,env);
	case cmd_term_store:
		return do_term_store(cmd,thread,env);
	case cmd_unknown:
		return atom_ok;
	case cmd_alltunnel_call:
		return do_all_tunnel_call(cmd,thread,env);
	case cmd_checkpoint_lock:
		return do_checkpoint_lock(cmd,thread,env);
	case cmd_actorsdb_add:
		return do_actorsdb_add(cmd,thread,env);
	case cmd_synced:
		return atom_ok;
	// case cmd_set_socket:
	// 	return do_set_socket(cmd, thread, env);
	default:
		return make_error_tuple(env, "invalid_command");
	}
}

static ERL_NIF_TERM push_command(int writeThreadNum, int readThreadNum, priv_data *pd, qitem *item)
{
	queue *thrCmds = NULL;

	if (writeThreadNum == -1 && readThreadNum == -1)
		thrCmds = pd->wtasks[pd->nEnvs * pd->nWriteThreads];
	else if (writeThreadNum >= 0)
		thrCmds = pd->wtasks[writeThreadNum];
	else
		thrCmds = pd->rtasks[readThreadNum];

	if(!queue_push(thrCmds, item))
	{
		return make_error_tuple(item->env, "command_push_failed");
	}
	return atom_ok;
}

static ERL_NIF_TERM make_answer(qitem *item, ERL_NIF_TERM answer)
{
	db_command *cmd = (db_command*)item->cmd;
	return enif_make_tuple2(item->env, cmd->ref, answer);
}

static void check_stalled_exec(u64 rtm, int n, db_thread **thrs, u64 *rThrReqs, u8 *rThrCmds, u64 *rTimes)
{
	int i;
	for (i = 0; i < n; i++)
	{
		u8 cmd = atomic_load_explicit(&thrs[i]->reqRunning,memory_order_relaxed);
		u64 nrq = atomic_load_explicit(&thrs[i]->nReqs,memory_order_relaxed);
		if (cmd && cmd == rThrCmds[i] && nrq == rThrReqs[i])
		{
			// We are still on the same command
			if ((rtm - rTimes[i]) > g_pd->maxReqTime && cmd == cmd_exec_script)
			{
				db_connection *con = thrs[i]->execConn;
				if (con)
					sqlite3_interrupt(con->db);
			}
		}
		else
		{
			rTimes[i] = rtm;
			rThrReqs[i] = nrq;
			rThrCmds[i] = cmd;
		}
	}
}

static void *ctrl_thread_func(void *arg)
{
	u64 rtm = 0;
	db_thread* data = (db_thread*)arg;
	u64 *rThrReqs = (u64*)calloc(g_pd->nEnvs * g_pd->nReadThreads, sizeof(u64));
	u64 *wThrReqs = (u64*)calloc(g_pd->nEnvs * g_pd->nWriteThreads, sizeof(u64));
	u8 *rThrCmds = (u8*)calloc(g_pd->nEnvs * g_pd->nReadThreads, sizeof(u8));
	u8 *wThrCmds = (u8*)calloc(g_pd->nEnvs * g_pd->nWriteThreads, sizeof(u8));
	u64 *rTimes = (u64*)calloc(g_pd->nEnvs * g_pd->nReadThreads, sizeof(u64));
	u64 *wTimes = (u64*)calloc(g_pd->nEnvs * g_pd->nWriteThreads, sizeof(u64));
	g_tsd_thread = data;
	data->isopen = 1;
	while(1)
	{
		db_command *cmd;
		ERL_NIF_TERM res;
		qitem *item = queue_timepop(data->tasks,100);
		if (item == NULL)
		{
			// Trust erlangs monotonic timer implementation more than semaphore.
			if (g_timeCounter >= 0)
				rtm = atomic_load_explicit(&g_counters[g_timeCounter*8], memory_order_relaxed);
			else
				rtm += 100;
			check_stalled_exec(rtm, g_pd->nEnvs * g_pd->nReadThreads,g_pd->rthreads, rThrReqs, rThrCmds, rTimes);
			check_stalled_exec(rtm, g_pd->nEnvs * g_pd->nWriteThreads,g_pd->wthreads, wThrReqs, wThrCmds, wTimes);
			continue;
		}
		cmd = (db_command*)item->cmd;
		// track_flag(data,1);
		// track_time(0,data);

		if (cmd->type == cmd_stop)
		{
			DBG("CTRL THREAD STOP");
			//queue_recycle(item);
			break;
		}

		res = evaluate_command(cmd,data,item->env);

		if (cmd->ref != 0)
		{
			enif_send(NULL, &cmd->pid, item->env, make_answer(item, res));
		}
		enif_clear_env(item->env);
		if (cmd->conn != NULL)
		{
			cmd->conn->dirty = 0;
			enif_release_resource(cmd->conn);
		}
		// thread_ex(data, item);
	}

	free(wTimes);
	free(rTimes);
	free(rThrReqs);
	free(wThrReqs);
	free(rThrCmds);
	free(wThrCmds);
	data->isopen = 0;

	DBG("ctrl thread=%d stopping.",data->nThread);

	if (data->columnSpace)
		free(data->columnSpace);
// #ifdef TRACK_TIME
// 	{
// 		mach_timebase_info_data_t info;
// 		if (mach_timebase_info (&info) == KERN_SUCCESS)
// 		{
// 			u64 n;
// 			FILE *tm = fopen("time.bin","wb");

// 			n = info.numer;
// 			fwrite(&n,sizeof(n),1,tm);
// 			n = info.denom;
// 			fwrite(&n,sizeof(n),1,tm);
// 			fwrite(data->timeBuf,data->timeBufPos, 1, tm);
// 			fclose(tm);
// 		}
// 	}
// #endif
	free(data);
	return NULL;
}

static void respond_cmd(db_thread *data, qitem *item)
{
	db_command *cmd = (db_command*)item->cmd;
	if (cmd->ref)
	{
		enif_send(NULL, &cmd->pid, item->env, make_answer(item, cmd->answer));
	}
	enif_clear_env(item->env);
	if (cmd->conn != NULL)
	{
		enif_release_resource(cmd->conn);
	}
	queue_recycle(item);
}

static void respond_items(db_thread *data, qitem *itemsWaiting)
{
	while (itemsWaiting != NULL)
	{
		qitem *next = itemsWaiting->next;
		respond_cmd(data, itemsWaiting);
		itemsWaiting = next;
	}
}

static void *processing_thread_func(void *arg)
{
	db_thread* data   = (db_thread*)arg;
	mdbinf* mdb 	  = &data->mdb;
	qitem *itemsWaiting = NULL;
	u64 waitingCommit = 0;
	int rc;
	u64 knownSockIndex = 0;
	qitem *nitem = NULL;
	g_tsd_cursync = 0;
	g_tsd_conn    = NULL;
	g_tsd_wmdb    = NULL;
	g_tsd_thread  = data;

	data->isopen = 1;
	data->env = enif_alloc_env();

	data->maxvalsize = mdb_env_get_maxkeysize(mdb->env);
	data->resFrames = alloca((SQLITE_DEFAULT_PAGE_SIZE/data->maxvalsize + 1)*sizeof(MDB_val));

	while (1)
	{
		db_command *cmd;
		qitem *item = nitem;

		atomic_store(&data->reqRunning, 0);
		if (!item)
			item = queue_pop(data->tasks);
		else
			nitem = NULL;
		cmd = (db_command*)item->cmd;
		data->pagesChanged = 0;
		atomic_store(&data->reqRunning, cmd->type);
		atomic_fetch_add(&data->nReqs, 1);

		if (!data->isreadonly)
		{
			u64 sockUpdate = atomic_load_explicit(&g_pd->sockUpdate, memory_order_relaxed);
			if (sockUpdate != knownSockIndex)
			{
				knownSockIndex = sockUpdate;
				do_set_socket(data);
			}
		}

		DBG("rthread=%d command=%d.",data->nThread,cmd->type);

		if (cmd->type == cmd_stop)
		{
			queue_recycle(item);
			mdb_txn_abort(mdb->txn);

			if (g_tsd_wmdb != NULL)
			{
				char commit = 1;
				unlock_write_txn(data->nEnv, 1, &commit, data->pagesChanged > 0);
			}
			break;
		}
		else
		{
			if (!mdb->txn)
			{
				DBG("Open read transaction");
				if (open_txn(mdb, MDB_RDONLY) == NULL)
				{
					ERL_NIF_TERM errterm;
					DBG("Can not open read transaction");

					errterm = make_error_tuple(item->env, "lmdb_unreadable_1");
					enif_send(NULL, &cmd->pid, item->env, make_answer(item, errterm));
					enif_clear_env(item->env);
					continue;
				}
			}
			else
			{
				if ((rc = mdb_txn_renew(mdb->txn)) != MDB_SUCCESS)
					break;
				if ((rc = mdb_cursor_renew(mdb->txn, mdb->cursorLog)) != MDB_SUCCESS)
				{
					DBG("Unable to renew cursor, reopening read txn");
					mdb_cursor_close(mdb->cursorLog);
					mdb_cursor_close(mdb->cursorPages);
					mdb_cursor_close(mdb->cursorInfo);
					mdb_txn_abort(mdb->txn);
					if (open_txn(mdb,MDB_RDONLY) == NULL)
					{
						ERL_NIF_TERM errterm;
						DBG("Unable to open read transaction");
						data = NULL;

						errterm = make_error_tuple(item->env, "lmdb_unreadable_2");
						enif_send(NULL, &cmd->pid, item->env, make_answer(item, errterm));
						enif_clear_env(item->env);
						continue;
					}
				}
				else
				{
					mdb_cursor_renew(mdb->txn, mdb->cursorPages);
					mdb_cursor_renew(mdb->txn, mdb->cursorInfo);
				}
			}
			cmd->answer = evaluate_command(cmd,data,item->env);
			mdb_txn_reset(mdb->txn);

			if (g_tsd_wmdb != NULL)
			{
				u64 curCommit;
				char commit;
				char syncForce = (cmd->type == cmd_sync && cmd->answer == atom_false);
				nitem = queue_trypop(data->tasks);
				commit = nitem == NULL; //queue_size(data->tasks) == 0;
				DBG("COMMIT %d",(int)commit);
				curCommit = g_tsd_wmdb->commitCount;
				unlock_write_txn(data->nEnv, syncForce, &commit, data->pagesChanged > 0);

				if (syncForce)
					cmd->answer = atom_ok;

				if (commit && itemsWaiting)
				{
					respond_items(data, itemsWaiting);
					itemsWaiting = NULL;
					waitingCommit = 0;
				}
				if (commit && !itemsWaiting)
				{
					respond_cmd(data, item);
					waitingCommit = 0;
				}
				else
				{
					if (!waitingCommit)
						waitingCommit = curCommit;
					else if (waitingCommit < curCommit)
					{
						respond_items(data, itemsWaiting);
						itemsWaiting = NULL;
						waitingCommit = curCommit;
					}
					item->next = itemsWaiting;
					itemsWaiting = item;
				}
			}
			else
			{
				respond_cmd(data, item);
				if (itemsWaiting)
				{
					char commit;
					nitem = queue_trypop(data->tasks);
					commit = nitem == NULL; //queue_size(data->tasks) == 0;
					if (commit)
					{
						u64 curCommit;
						if (!g_tsd_wmdb)
							lock_wtxn(data->nEnv);

						curCommit = g_tsd_wmdb->commitCount;

						unlock_write_txn(data->nEnv, 0, &commit, data->pagesChanged > 0);
						if (commit || waitingCommit < curCommit)
						{
							respond_items(data, itemsWaiting);
							itemsWaiting = NULL;
							waitingCommit = 0;
						}
					}
				}
			}

			DBG("rthread=%d command done 2. haveWaiting=%d, havenext=%d.",
				data->nThread,(int)(itemsWaiting != 0), (int)(nitem != NULL));
		}
	}
	if (!data->isreadonly && data->finish)
	{
		char commit = 1;
		lock_wtxn(data->nEnv);
		unlock_write_txn(data->nEnv, 1, &commit, 1);
	}
	DBG("rthread=%d stopping.",data->nThread);

	if (data->columnSpace)
		free(data->columnSpace);

	free(data);
	return NULL;
}

static ERL_NIF_TERM parse_helper(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary bin;
	unsigned int offset = 0;
	char instr = 0;

	if (argc != 2)
		return enif_make_badarg(env);

	if (!enif_inspect_binary(env, argv[0], &bin))
		return enif_make_badarg(env);
	if (!enif_get_uint(env, argv[1], &offset))
		return enif_make_badarg(env);

	for (;offset < bin.size;offset++)
	{
		if (bin.data[offset] == '\'' || bin.data[offset] == '`')
			instr = !instr;
		// If ; outside of string, return offset
		else if (bin.data[offset] == ';' && !instr)
		{
			return enif_make_uint(env,offset);
		}
		// If {{ return offset
		else if (bin.data[offset] == '{' && offset+1 < bin.size)
		{
			if (bin.data[offset+1] == '{')
				return enif_make_uint(env,offset);
		}
		else if (bin.data[offset] == '/' && offset+1 < bin.size && !instr)
		{
			if (bin.data[offset+1] == '*')
				return enif_make_uint(env,offset);
		}
	}

	enif_consume_timeslice(env,90);

	return atom_ok;
}

static ERL_NIF_TERM term_store(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	qitem *item;
	u32 thread;
	db_connection *res = NULL;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("term_store");

	if (argc != 3 && argc != 4)
		return enif_make_badarg(env);

	if (!enif_is_number(env,argv[1]))
		return enif_make_badarg(env);
	if (!enif_is_binary(env,argv[2]))
		return enif_make_badarg(env);

	if (argc == 4)
	{
		if (!enif_is_binary(env,argv[0]) && !enif_is_list(env,argv[0]))
			return enif_make_badarg(env);
		if (!enif_get_uint(env, argv[3], &thread))
			return enif_make_badarg(env);
		thread = (thread % pd->nEnvs)*pd->nWriteThreads + (thread % pd->nWriteThreads);
		item = command_create(thread,-1,pd);
		if (!item)
			return atom_again;
		cmd = (db_command*)item->cmd;
	}
	else
	{
		if (!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
			return enif_make_badarg(env);
		thread = res->wthreadind;
		item = command_create(thread,-1,pd);
		if (!item)
			return atom_again;
		cmd = (db_command*)item->cmd;
		cmd->conn = res;
		enif_keep_resource(res);
	}
	cmd->type = cmd_term_store;
	if (argc == 4)
		cmd->arg = enif_make_copy(item->env,argv[0]); // actor path
	cmd->arg1 = enif_make_copy(item->env,argv[1]); // evterm
	cmd->arg2 = enif_make_copy(item->env,argv[2]); // votedfor

	enif_consume_timeslice(env,90);
	return push_command(thread, -1, pd, item);
}

static ERL_NIF_TERM get_actor_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	qitem *item;
	ErlNifPid pid;
	u32 thread;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("get_actor_info");

	if (argc != 4)
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[0]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[1], &pid))
		return make_error_tuple(env, "invalid_pid");
	if(!enif_get_uint(env, argv[3], &thread))
		return make_error_tuple(env, "invalid_pid");

	thread = ((thread % pd->nEnvs) * pd->nReadThreads) + (thread % pd->nReadThreads);
	item = command_create(-1, thread, pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_actor_info;
	cmd->ref = enif_make_copy(item->env, argv[0]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env,argv[2]);

	enif_consume_timeslice(env,90);
	return push_command(-1, thread, pd, item);
}


// argv[0] - Ref
// argv[1] - Pid to respond to
// argv[2] - Relative path to db
// argv[3] - Thread number
// argv[4] - Sql to execute on open. Optional.
static ERL_NIF_TERM db_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifPid pid;
	qitem *item;
	u32 thread;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("db_open");

	if(!(argc == 5 || argc == 6))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[0]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[1], &pid))
		return make_error_tuple(env, "invalid_pid");
	if(!enif_get_uint(env, argv[3], &thread))
		return make_error_tuple(env, "invalid_pid");

	thread = ((thread % pd->nEnvs) * pd->nReadThreads) + (thread % pd->nReadThreads);
	item = command_create(-1,thread,pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_open;
	cmd->ref = enif_make_copy(item->env, argv[0]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env, argv[2]);
	cmd->arg1 = enif_make_copy(item->env, argv[4]);
	if (argc == 6)
	{
		cmd->arg2 = enif_make_copy(item->env, argv[5]);
	}
	enif_consume_timeslice(env,90);
	// return enif_make_tuple2(env,push_command(conn->thread, item),db_conn);
	return push_command(-1, thread, pd, item);
}

static ERL_NIF_TERM sync_num(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;

	DBG("sync_num");

	if (argc != 1)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return make_error_tuple(env, "invalid_connection");

	return enif_make_uint64(env,res->syncNum);
}

static ERL_NIF_TERM replication_done(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;

	if (argc != 1)
		return atom_false;

	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return make_error_tuple(env, "invalid_connection");

	enif_mutex_lock(res->wal.mtx);
	res->wal.readSafeTerm = res->wal.lastCompleteTerm;
	res->wal.readSafeEvnum = res->wal.lastCompleteEvnum;
	res->wal.readSafeMxPage = res->wal.mxPage;
	enif_mutex_unlock(res->wal.mtx);

	return atom_ok;
}

static ERL_NIF_TERM replicate_opts(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifBinary bin;

	DBG("replicate_opts");

	if (!(argc == 3))
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return make_error_tuple(env, "invalid_connection");
	if (!enif_inspect_iolist_as_binary(env, argv[1], &bin))
		return make_error_tuple(env, "not_iolist");

	DBG("do_replicate_opts %zu", bin.size);
	if (res->packetPrefixSize < bin.size)
	{
		free(res->packetPrefix);
		res->packetPrefixSize = 0;
		res->packetPrefix = NULL;
	}

	if (bin.size > 0)
	{
		int dorepl;
		if (!enif_get_int(env,argv[2],&(dorepl)))
			return make_error_tuple(env, "repltype_not_int");
		if (!res->packetPrefix)
			res->packetPrefix = malloc(bin.size);

		res->doReplicate = dorepl;
		memcpy(res->packetPrefix,bin.data,bin.size);
		res->packetPrefixSize = bin.size;
	}
	else
	{
		if (!res->packetPrefix)
			free(res->packetPrefix);
		res->packetPrefix = NULL;
		res->packetPrefixSize = 0;
		res->doReplicate = 0;
	}
	return atom_ok;
}

static ERL_NIF_TERM set_tunnel_connector(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	priv_data *pd = (priv_data*)enif_priv_data(env);

	enif_self(env, &pd->tunnelConnector);

	return atom_ok;
}

static ERL_NIF_TERM counter_inc(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	i64 val;
	int i;

	if (argc != 2)
		return atom_false;

	if (!enif_get_int(env,argv[0],&i))
		return make_error_tuple(env, "not_int");
	if (!enif_get_int64(env,argv[1],(ErlNifSInt64*)&val))
		return make_error_tuple(env, "not_int");

	if (val < 0)
	{
		val = atomic_fetch_sub_explicit(&g_counters[i*8], -val, memory_order_relaxed);
	}
	else if (val > 0)
	{
		val = atomic_fetch_add_explicit(&g_counters[i*8], val, memory_order_relaxed);
	}
	else
	{
		val = atomic_load_explicit(&g_counters[i*8], memory_order_relaxed);
	}
	return enif_make_int64(env, val);
}

static ERL_NIF_TERM set_thread_fd(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	int thread, fd, type, pos;
	// qitem *item;
	// db_command *cmd;

	if (!enif_get_int(env,argv[0],&thread))
		return make_error_tuple(env, "not_int");
	if (!enif_get_int(env,argv[1],&fd))
		return make_error_tuple(env, "not_int");
	if (!enif_get_int(env,argv[2],&pos))
		return make_error_tuple(env, "not_int");
	if (!enif_get_int(env,argv[3],&type))
		return make_error_tuple(env, "not_int");

	if (pos > 8 || pos < 0 || fd < 3 || thread >= g_pd->nWriteThreads * g_pd->nEnvs)
		return atom_false;

	DBG("Set thread fd %d %d %d", thread, pos, fd);
	
	enif_mutex_lock(g_pd->sockMutex);
	g_pd->sockets[thread*MAX_CONNECTIONS + pos] = fd;
	g_pd->socketTypes[thread*MAX_CONNECTIONS + pos] = type;
	atomic_fetch_add(&g_pd->sockUpdate, 1);
	enif_mutex_unlock(g_pd->sockMutex);

	// item = command_create(thread,-1,g_pd);
	// if (!item)
	// 	return atom_again;
	// cmd = (db_command*)item->cmd;
	// cmd->type = cmd_set_socket;
	// cmd->arg = enif_make_int(item->env,fd);
	// cmd->arg1 = enif_make_int(item->env,pos);
	// cmd->arg2 = enif_make_int(item->env,type);
	// push_command(thread, -1, g_pd, item);
	enif_consume_timeslice(env,90);
	return atom_ok;
}

// static ERL_NIF_TERM interrupt_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
// {
// 	db_connection *res;
// 	qitem *item;
// 	priv_data *pd = (priv_data*)enif_priv_data(env);
// 	db_command *cmd = NULL;

// 	DBG( "interrupt");

// 	if(argc != 1)
// 		return enif_make_badarg(env);

// 	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
// 	{
// 		return enif_make_badarg(env);
// 	}
// 	item = command_create(-1,-1,pd);
// 	if (!item)
// 		return atom_again;
// 	cmd = (db_command*)item->cmd;
// 	cmd->type = cmd_interrupt;
// 	cmd->conn = res;
// 	enif_keep_resource(res);

// 	enif_consume_timeslice(env,90);

// 	return push_command(-1,-1,pd, item);
// }

static ERL_NIF_TERM lz4_compress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary binIn;
	ErlNifBinary binOut;
	int size;
	ERL_NIF_TERM termbin;

	// DBG( "lz4_compress");

	if (argc != 1)
		return enif_make_badarg(env);

	if (!enif_inspect_iolist_as_binary(env, argv[0], &binIn))
		return enif_make_badarg(env);

	enif_alloc_binary(LZ4_COMPRESSBOUND(binIn.size),&binOut);

	size = LZ4_compress((char*)binIn.data,(char*)binOut.data,binIn.size);
	termbin = enif_make_binary(env,&binOut);
	enif_release_binary(&binOut);

	enif_consume_timeslice(env,500);
	return enif_make_tuple2(env,termbin,enif_make_int(env,size));
}

static ERL_NIF_TERM lz4_decompress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary binIn;
	ErlNifBinary binOut;
	int sizeOriginal;
	int sizeReadNum;
	int rt;

	DBG( "lz4_decompress %d",argc);

	if (argc != 2 && argc != 3)
		return make_error_tuple(env, "argc");

	if (!enif_inspect_iolist_as_binary(env, argv[0], &binIn))
		return make_error_tuple(env, "not_iolist");

	if (!enif_get_int(env,argv[1],&sizeOriginal))
		return make_error_tuple(env, "size_not_int");

	if (argc == 3)
	{
		if (!enif_get_int(env,argv[2],&sizeReadNum))
			return make_error_tuple(env, "readnum_not_int");;
	}
	else
		sizeReadNum = binIn.size;

	enif_alloc_binary(sizeOriginal,&binOut);
	rt = LZ4_decompress_safe((char*)binIn.data,(char*)binOut.data,sizeReadNum,sizeOriginal);
	enif_consume_timeslice(env,90);
	if (rt > 0)
	{
		ERL_NIF_TERM termout = enif_make_binary(env,&binOut);
		enif_release_binary(&binOut);
		return termout;
	}
	else
	{
		enif_release_binary(&binOut);
		return atom_error;
	}
}

static ERL_NIF_TERM all_tunnel_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;
	int i;
	// int nthreads = pd->nEnvs;

	DBG("all_tunnel_call");

	if (argc != 3 && argc != 4)
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[0]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[1], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!(enif_is_binary(env,argv[2]) || enif_is_list(env,argv[2])))
		return make_error_tuple(env, "invalid_bin");
	if (argc == 4 && !(enif_is_binary(env,argv[3]) || enif_is_list(env,argv[3])))
		return make_error_tuple(env, "invalid_bin2");

	for (i = 0; i < pd->nEnvs*pd->nWriteThreads; i++)
	{
		item = command_create(i,-1,pd);
		if (!item)
			return atom_again;
		cmd = (db_command*)item->cmd;
		cmd->type = cmd_alltunnel_call;
		cmd->ref = enif_make_copy(item->env, argv[0]);
		cmd->pid = pid;
		cmd->arg = enif_make_copy(item->env, argv[2]);
		if (argc == 4)
			cmd->arg1 = enif_make_copy(item->env, argv[3]);

		push_command(i, -1, pd, item);
	}
	enif_consume_timeslice(env,90);

	return atom_ok;
}


static ERL_NIF_TERM store_prepared_table(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	priv_data *pd = (priv_data*)enif_priv_data(env);
	const ERL_NIF_TERM *versTuple;
	const ERL_NIF_TERM *sqlTuple;
	const ERL_NIF_TERM *versRow;
	const ERL_NIF_TERM *sqlRow;
	int tupleSize,rowSize,i,j;
	ErlNifBinary bin;

	DBG("store_prepared_table");

	if (argc != 2)
		return enif_make_badarg(env);

	// {{1,2,2,0,0,..},{0,0,0,0,...}}
	if (!enif_get_tuple(env, argv[0], &tupleSize, &versTuple))
		return atom_false;
	// {{"select....","insert into..."},{"select....","update ...."}}
	if (!enif_get_tuple(env, argv[1], &tupleSize, &sqlTuple))
		return atom_false;

	if (tupleSize > MAX_PREP_SQLS)
		return atom_false;

	enif_mutex_lock(pd->prepMutex);
	// Delete old table of prepared statements and set new one.
	memset(pd->prepVersions,0,sizeof(pd->prepVersions));
	for (i = 0; i < MAX_PREP_SQLS; i++)
	{
		for (j = 0; j < MAX_PREP_SQLS; j++)
		{
			free(pd->prepSqls[i][j]);
			pd->prepSqls[i][j] = NULL;
		}
	}

	for (i = 0; i < tupleSize; i++)
	{
		if (!enif_get_tuple(env, versTuple[i], &rowSize, &versRow))
			break;
		if (!enif_get_tuple(env, sqlTuple[i], &rowSize, &sqlRow))
			break;

		pd->prepSize = rowSize;
		for (j = 0; j < rowSize; j++)
		{
			enif_get_int(env,versRow[j],&(pd->prepVersions[i][j]));
			if (enif_is_list(env,sqlRow[j]) || enif_is_binary(env,sqlRow[j]))
			{
				enif_inspect_iolist_as_binary(env,sqlRow[j],&bin);
				pd->prepSqls[i][j] = malloc(bin.size+1);
				pd->prepSqls[i][j][bin.size] = 0;
				memcpy(pd->prepSqls[i][j],bin.data,bin.size);
			}
		}
	}
	enif_mutex_unlock(pd->prepMutex);
	enif_consume_timeslice(env,99);
	DBG("store_prepared_table done");
	return atom_ok;
}

static ERL_NIF_TERM db_checkpoint(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("Checkpoint");

	if (argc != 4)
		return enif_make_badarg(env);

	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_is_number(env,argv[3]))
		return make_error_tuple(env, "evnum NaN");

	item = command_create(res->wthreadind,-1,pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;

	cmd->type = cmd_checkpoint;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env, argv[3]);  // evnum
	cmd->conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,90);
	return push_command(res->wthreadind, -1, pd, item);
}

static ERL_NIF_TERM stmt_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("stmt_info");

	if (argc != 4)
		return enif_make_badarg(env);

	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!(enif_is_binary(env,argv[3]) || enif_is_list(env,argv[3])))
		return make_error_tuple(env, "sql not an iolist");

	item = command_create(-1,res->rthreadind,pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;

	cmd->type = cmd_stmt_info;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env, argv[3]);  // sql
	cmd->conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,90);
	return push_command(-1, res->rthreadind, pd, item);
}

static ERL_NIF_TERM exec_read(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("exec_read %d",argc);

	if(argc != 4 && argc != 5)
		return enif_make_badarg(env);

	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!(enif_is_binary(env,argv[3]) || enif_is_list(env,argv[3]) || enif_is_tuple(env,argv[3])))
		return make_error_tuple(env,"sql");

	item = command_create(-1,res->rthreadind,pd);
	if (!item)
	{
		DBG("exec_read again!");
		return atom_again;
	}
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_exec_script;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env, argv[3]);  // sql string
	if (argc == 5)
		cmd->arg4 = enif_make_copy(item->env, argv[4]);  // records for bulk insert
	cmd->conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,90);
	return push_command(-1, res->rthreadind, pd, item);
}


static ERL_NIF_TERM exec_script(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("exec_script %d",argc);

	if(argc != 7 && argc != 8)
		return enif_make_badarg(env);

	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	// blob actor type, sql = page number, records = iolist
	if (res->db == NULL && !enif_is_number(env,argv[3]) && !enif_is_tuple(env,argv[3]))
		return make_error_tuple(env,"sql_not_pagenumber");
	else if (!(enif_is_binary(env,argv[3]) || enif_is_list(env,argv[3]) || enif_is_tuple(env,argv[3])))
		return make_error_tuple(env,"sql");
	if (!enif_is_number(env,argv[4]))
		return make_error_tuple(env, "term");
	if (!enif_is_number(env,argv[5]))
		return make_error_tuple(env, "index");
	if (!enif_is_binary(env,argv[6]))
		return make_error_tuple(env, "appendparam");


	item = command_create(res->wthreadind,-1,pd);
	if (!item)
	{
		DBG("exec_script again!");
		return atom_again;
	}
	cmd = (db_command*)item->cmd;

	cmd->type = cmd_exec_script;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env, argv[3]);  // sql string
	cmd->arg1 = enif_make_copy(item->env, argv[4]); // term
	cmd->arg2 = enif_make_copy(item->env, argv[5]); // index
	cmd->arg3 = enif_make_copy(item->env, argv[6]); // appendentries param binary
	if (argc == 8)
		cmd->arg4 = enif_make_copy(item->env, argv[7]);  // records for bulk insert
	cmd->conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,90);
	return push_command(res->wthreadind, -1, pd, item);
}

static ERL_NIF_TERM db_sync(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("db_sync");

	if(argc != 3 && argc != 0)
		return enif_make_badarg(env);

	if (argc == 3)
	{
		if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
			return enif_make_badarg(env);
		if(!enif_is_ref(env, argv[1]))
			return make_error_tuple(env, "invalid_ref");
		if(!enif_get_local_pid(env, argv[2], &pid))
			return make_error_tuple(env, "invalid_pid");

		item = command_create(res->wthreadind, -1, pd);
		if (!item)
			return atom_again;
		cmd = (db_command*)item->cmd;
		cmd->type = cmd_sync;
		cmd->ref = enif_make_copy(item->env, argv[1]);
		cmd->pid = pid;
		cmd->conn = res;
		enif_keep_resource(res);

		enif_consume_timeslice(env,90);
		return push_command(res->wthreadind, -1, pd, item);
	}
	else
	{
		int i;
		for (i = 0; i < pd->nEnvs; i++)
		{
			item = command_create(i*pd->nWriteThreads,-1,pd);
			if (!item)
				return atom_again;
			cmd = (db_command*)item->cmd;
			cmd->type = cmd_sync;
			push_command(i*pd->nWriteThreads, -1, pd, item);
		}

		return atom_ok;
	}
}

static ERL_NIF_TERM checkpoint_lock(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("checkpoint_lock");

	if(argc != 4)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_is_number(env,argv[3]))
		return make_error_tuple(env, "term");

	item = command_create(res->wthreadind,-1,pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_checkpoint_lock;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env, argv[3]);  // 1 - lock, 0 - unlock
	cmd->conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,90);
	return push_command(res->wthreadind, -1, pd, item);
}

static ERL_NIF_TERM page_size(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	return enif_make_int(env,SQLITE_DEFAULT_PAGE_SIZE);
}

static ERL_NIF_TERM iterate_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("iterate_db %d",argc);

	if(argc != 4 && argc != 5)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");

	item = command_create(-1,res->rthreadind,pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_iterate;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;
	cmd->conn = res;
	enif_keep_resource(res);
	cmd->arg = enif_make_copy(item->env,argv[3]); // evterm or iterator resource
	if (argc == 5)
		cmd->arg1 = enif_make_copy(item->env,argv[4]); // evnum

	enif_consume_timeslice(env,90);
	return push_command(-1, res->rthreadind, pd, item);
}

static ERL_NIF_TERM iterate_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	iterate_resource *iter;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("iterate_close %d",argc);

	if(argc != 1)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], iterate_type, (void **) &iter))
		return enif_make_badarg(env);

	item = command_create(-1, iter->thread, pd);
	if (!item)
		return atom_again;
	iter->closed = 1;
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_checkpoint_lock;
	cmd->arg = enif_make_int(item->env, 0);
	cmd->conn = iter->conn;
	// No keep. This way release for connection will be called on thread when done with command.
	// It will decrement keep on connection held by iterate.
	// enif_keep_resource(res);

	enif_consume_timeslice(env,90);
	return push_command(-1, iter->thread, pd, item);
}

static ERL_NIF_TERM inject_page(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("inject_page");

	if(argc != 5)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_is_binary(env,argv[3]))
		return make_error_tuple(env,"page");
	// if (argc == 7 && (!enif_is_number(env,argv[4]) || !enif_is_number(env,argv[5]) || !enif_is_number(env,argv[6])))
	// 	return make_error_tuple(env,"NaN");
	else if (!enif_is_binary(env,argv[4]))
		return make_error_tuple(env,"header");

	item = command_create(res->wthreadind,-1,pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_inject_page;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env,argv[3]);  // bin
	cmd->arg1 = enif_make_copy(item->env,argv[4]); // header bin
	cmd->conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,90);
	return push_command(res->wthreadind, -1, pd, item);
}

static ERL_NIF_TERM wal_rewind(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	DBG("wal_rewind");

	if(argc != 4 && argc != 5)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_is_number(env,argv[3]))
		return make_error_tuple(env,"evnum");
	if (argc == 5 && (!enif_is_list(env,argv[4]) && !enif_is_binary(env,argv[4])))
		return make_error_tuple(env,"not_iolist");

	item = command_create(res->wthreadind,-1,pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_wal_rewind;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;
	cmd->arg = enif_make_copy(item->env,argv[3]);
	if (argc == 5)
		cmd->arg1 = enif_make_copy(item->env, argv[4]);
	cmd->conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,90);
	return push_command(res->wthreadind, -1, pd, item);
}


static ERL_NIF_TERM noop(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;

	if(argc != 3)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");

	item = command_create(res->wthreadind,-1,pd);
	if (!item)
		return atom_again;
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_unknown;
	cmd->ref = enif_make_copy(item->env, argv[1]);
	cmd->pid = pid;

	return push_command(res->wthreadind, -1, pd, item);
}


static int start_threads(priv_data *priv)
{
	db_thread *controlThread = NULL;
	int i,flags = 0;

	if (g_transsync == 0)
		flags = MDB_NOSYNC;
	else
		flags = 0;

	controlThread = malloc(sizeof(db_thread));
	memset(controlThread,0,sizeof(db_thread));
	controlThread->nThread = -1;
	controlThread->tasks = queue_create();
	priv->wtasks[priv->nEnvs*priv->nWriteThreads] = controlThread->tasks;
	priv->syncNumbers = malloc(sizeof(u64)*priv->nEnvs);
	priv->wthrMutexes = malloc(sizeof(ErlNifMutex*)*priv->nEnvs);
	memset(priv->syncNumbers, 0, sizeof(sizeof(u64)*priv->nEnvs));

	if(enif_thread_create("db_connection", &(priv->tids[priv->nEnvs*priv->nWriteThreads]),
		ctrl_thread_func, controlThread, NULL) != 0)
	{
		return -1;
	}

	priv->prepMutex = enif_mutex_create("prepmutex");
	priv->sockMutex = enif_mutex_create("sockmutex");

	DBG("Driver starting, paths=%d, threads (w=%d, r=%d). Dbsize %llu, nbatch=%d, tsy=%d",
		priv->nEnvs,priv->nWriteThreads,priv->nReadThreads,priv->dbsize,g_nbatch,(int)g_transsync);

	for (i = 0; i < priv->nEnvs; i++)
	{
		MDB_env *menv = NULL;
		MDB_dbi infodb;
		MDB_dbi logdb;
		MDB_dbi pagesdb;
		MDB_dbi actorsdb;
		int j,k;

		// atomic_init(&priv->syncNumbers[i],0);
		// priv->writeBufs[i] = (u8*)wbuf_init(2496);
		for (k = 0; k < priv->nReadThreads+priv->nWriteThreads; k++)
		{
			char lmpath[MAX_PATHNAME];
			db_thread *curThread = malloc(sizeof(db_thread));

			memset(curThread,0,sizeof(db_thread));

			sprintf(lmpath,"%s/lmdb",priv->paths[i]);

			if (k == 0)
			{
				MDB_val key = {1,(void*)"?"}, data = {0,NULL};
				u64 index = 0;
				int rc;
				MDB_txn *txn;

				// MDB INIT
				if (mdb_env_create(&menv) != MDB_SUCCESS)
					return -1;
				if (mdb_env_set_maxdbs(menv,5) != MDB_SUCCESS)
					return -1;
				if (mdb_env_set_mapsize(menv,priv->dbsize) != MDB_SUCCESS)
					return -1;
				// Syncs are handled from erlang.
				if (mdb_env_open(menv, lmpath, MDB_NOSUBDIR|MDB_NOTLS|MDB_NORDAHEAD|flags, 0664) != MDB_SUCCESS) //MDB_NOSYNC
					return -1;

				// Create databases if they do not exist yet
				if (mdb_txn_begin(menv, NULL, 0, &txn) != MDB_SUCCESS)
					return -1;
				if (mdb_dbi_open(txn, "info", MDB_INTEGERKEY | MDB_CREATE, &infodb) != MDB_SUCCESS)
					return -1;
				if (mdb_dbi_open(txn, "actors", MDB_CREATE, &actorsdb) != MDB_SUCCESS)
					return -1;
				if (mdb_dbi_open(txn, "log", MDB_CREATE | MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERDUP, &logdb) != MDB_SUCCESS)
					return -1;
				if (mdb_dbi_open(txn, "pages", MDB_CREATE | MDB_DUPSORT, &pagesdb) != MDB_SUCCESS)
					return -1;

				rc = mdb_get(txn,actorsdb,&key,&data);
				if (rc == MDB_SUCCESS)
					memcpy(&index,data.mv_data,sizeof(u64));

				atomic_init(&priv->actorIndexes[i],index);

				if (mdb_txn_commit(txn) != MDB_SUCCESS)
					return -1;

				priv->wmdb[i].env = menv;
				priv->wmdb[i].infodb = infodb;
				priv->wmdb[i].actorsdb = actorsdb;
				priv->wmdb[i].logdb = logdb;
				priv->wmdb[i].pagesdb = pagesdb;
			}

			if (k == 0)
			{
				priv->wthrMutexes[i] = enif_mutex_create("envmutex");
				curThread->finish = 1;
			}

			curThread->nEnv = i;
			if (k < priv->nWriteThreads)
			{
				curThread->nThread = k;
				// for (j = 0; j < MAX_CONNECTIONS; j++)
				// {
				// 	atomic_init(&priv->sockets[i*priv->nWriteThreads*MAX_CONNECTIONS + k*MAX_CONNECTIONS + j], 0);
				// 	atomic_init(&priv->socketTypes[i*priv->nWriteThreads*MAX_CONNECTIONS + k*MAX_CONNECTIONS + j], 0);
				// }
			}
			else
			{
				curThread->isreadonly = 1;
				curThread->nThread = k - priv->nWriteThreads;
			}

			curThread->tasks = queue_create();
			curThread->mdb.env = menv;
			curThread->mdb.infodb = infodb;
			curThread->mdb.actorsdb = actorsdb;
			curThread->mdb.logdb = logdb;
			curThread->mdb.pagesdb = pagesdb;
			if (k < priv->nWriteThreads)
			{
				priv->wtasks[i*priv->nWriteThreads + k] = curThread->tasks;
				priv->wthreads[i*priv->nWriteThreads + k] = curThread;
			}
			else
			{
				priv->rtasks[i*priv->nReadThreads + (k - priv->nWriteThreads)] = curThread->tasks;
				priv->rthreads[i*priv->nReadThreads + (k - priv->nWriteThreads)] = curThread;
			}

			curThread->nstaticSqls = priv->nstaticSqls;
			for (j = 0; j < priv->nstaticSqls; j++)
				memcpy(curThread->staticSqls[j], priv->staticSqls[j], 256);

			if (k < priv->nWriteThreads)
			{
				if (enif_thread_create("wthr", &(priv->tids[i*priv->nWriteThreads+k]), processing_thread_func, curThread, NULL) != 0)
				{
					return -1;
				}
			}
			else
			{
				if (enif_thread_create("rthr", &(priv->rtids[i*priv->nReadThreads + (k - priv->nWriteThreads)]),
					processing_thread_func, curThread, NULL) != 0)
				{
					return -1;
				}
			}
		}
	}
	return 0;
}


static ERL_NIF_TERM db_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	return atom_ok;
}

void errLogCallback(void *pArg, int iErrCode, const char *zMsg)
{
}


static int on_load(ErlNifEnv* env, void** priv_out, ERL_NIF_TERM info)
{
	int i = 0;
	const ERL_NIF_TERM *param;
	const ERL_NIF_TERM *pathtuple;
	ERL_NIF_TERM value;
	char nodename[512];
	priv_data *priv;

#ifdef _WIN32
	WSADATA wsd;
	if (WSAStartup(MAKEWORD(1, 1), &wsd) != 0)
		return -1;
#endif
	priv = malloc(sizeof(priv_data));
	memset(priv,0,sizeof(priv_data));
	*priv_out = priv;
	g_pd = priv;
	priv->nReadThreads = 1;
	priv->nWriteThreads = 1;
// Apple/Win get smaller max dbsize because they are both fucked when it comes to mmap.
// They are just dev platforms anyway.
#if defined(__APPLE__) || defined(_WIN32)
	priv->dbsize = 4096LL*1024LL*1024LL;
#else
	// 1TB def size on linux
	priv->dbsize = 1099511627776LL;
#endif
	memset(nodename,0,512);
	priv->maxReqTime = 60000;

	// scratchSize = 1024*1024*10;
	// while (scratchSize % (SQLITE_DEFAULT_PAGE_SIZE*6) > 0)
	// 	scratchSize += SQLITE_DEFAULT_PAGE_SIZE;
	// priv->sqlite_pgcache = malloc((4096+128) * 4096*6);
	// sqlite3_config(SQLITE_CONFIG_SCRATCH, priv->sqlite_scratch, 6*SQLITE_DEFAULT_PAGE_SIZE, scratchSize / (6*SQLITE_DEFAULT_PAGE_SIZE));
	// sqlite3_config(SQLITE_CONFIG_PAGECACHE, priv->sqlite_pgcache, 4096+128, 4096*6);
	// sqlite3_config(SQLITE_CONFIG_LOG, errLogCallback, NULL);
	sqlite3_initialize();
	sqlite3_vfs_register(sqlite3_nullvfs(), 1);
	// This must not be enabled. It might cause Wal structure to be shared
	// on two connections without them knowing. If first gets deleted (which will happen)
	// second will crash the server because it will try to access a Wal structure that is dealocated.
	// There is no reason to have more than 1 connection per actor.
	// sqlite3_enable_shared_cache(1);

	atom_false = enif_make_atom(env,"false");
	atom_ok = enif_make_atom(env,"ok");
	atom_rows = enif_make_atom(env,"rows");
	atom_columns = enif_make_atom(env,"columns");
	atom_error = enif_make_atom(env,"error");
	atom_undefined = enif_make_atom(env,"undefined");
	atom_rowid = enif_make_atom(env,"rowid");
	atom_changes = enif_make_atom(env,"changes");
	atom_done = enif_make_atom(env,"done");
	atom_iter = enif_make_atom(env,"iter");
	atom_blob = enif_make_atom(env, "blob");
	atom_wthreads = enif_make_atom(env, "wthreads");
	atom_rthreads = enif_make_atom(env, "rthreads");
	atom_paths = enif_make_atom(env, "paths");
	atom_staticsqls = enif_make_atom(env, "staticsqls");
	atom_dbsize = enif_make_atom(env, "dbsize");
	atom_logname = enif_make_atom(env, "logname");
	atom_nbatch = enif_make_atom(env, "nbatch");
	atom_lmdbsync = enif_make_atom(env, "lmdbsync");
	atom_tcpfail = enif_make_atom(env, "tcpfail");
	atom_drivername = enif_make_atom(env, "actordb_driver");
	atom_again = enif_make_atom(env, "again");
	atom_counters = enif_make_atom(env, "counters");
	atom_maxreqtime = enif_make_atom(env, "maxtime");
	atom_timecounter = enif_make_atom(env, "timecounter");
	atom_pluginfiles = enif_make_atom(env, "pluginfiles");

#ifdef _TESTDBG_
	if (enif_get_map_value(env, info, atom_logname, &value))
	{
		enif_get_string(env,value,nodename,512,ERL_NIF_LATIN1);
		g_log = fopen(nodename, "w");
	}
#endif
	if (enif_get_map_value(env, info, atom_pluginfiles, &value))
	{
		if (!enif_get_tuple(env, value, &i, &param))
		{
			DBG("Param not tuple");
			return -1;
		}
		priv->nPlugins = i;
		if (i)
		{
			priv->pluginFiles = calloc(i,sizeof(char*));
			for (i = 0; i < priv->nPlugins; i++)
			{
				priv->pluginFiles[i] = calloc(256,sizeof(char));
				enif_get_string(env,param[i],priv->pluginFiles[i],256*sizeof(char),ERL_NIF_LATIN1);
			}
		}
		else
		{
			priv->pluginFiles = NULL;
		}
	}
	if (enif_get_map_value(env, info, atom_dbsize, &value))
	{
		if (!enif_get_uint64(env,value,(ErlNifUInt64*)&priv->dbsize))
			return -1;
	}
	if (enif_get_map_value(env, info, atom_staticsqls, &value))
	{
		if (!enif_get_tuple(env, value, &i, &param))
		{
			DBG("Param not tuple");
			return -1;
		}
		if (i > MAX_STATIC_SQLS)
			return -1;
		priv->nstaticSqls = i;
		for (i = 0; i < priv->nstaticSqls; i++)
			enif_get_string(env,param[i],priv->staticSqls[i],256,ERL_NIF_LATIN1);
	}
	if (enif_get_map_value(env, info, atom_counters, &value))
	{
		if (!enif_get_int(env, value, &g_nCounters))
			return -1;
		g_counters = calloc(g_nCounters, sizeof(_Atomic(i64))*8);
		for (i = 0; i < g_nCounters; i++)
			atomic_init(&g_counters[i*8], 0);
	}
	if (enif_get_map_value(env, info, atom_maxreqtime, &value))
	{
		if (!enif_get_uint(env, value, &priv->maxReqTime))
			return -1;
	}
	if (enif_get_map_value(env, info, atom_timecounter, &value))
	{
		if (!enif_get_int(env, value, &g_timeCounter))
			return -1;
	}
	if (enif_get_map_value(env, info, atom_paths, &value))
	{
		if (!enif_get_tuple(env, value, &priv->nEnvs, &pathtuple))
		{
			DBG("Param not tuple");
			return -1;
		}

		priv->paths = calloc(priv->nEnvs, sizeof(char*));
		for (i = 0; i < priv->nEnvs; i++)
		{
			priv->paths[i] = calloc(MAX_PATHNAME, sizeof(char));
			if (!(enif_get_string(env,pathtuple[i],priv->paths[i],MAX_PATHNAME,ERL_NIF_LATIN1) < (MAX_PATHNAME-MAX_ACTOR_NAME)))
				return -1;
		}
	}
	if (enif_get_map_value(env, info, atom_rthreads, &value))
	{
		if (!enif_get_int(env,value,&priv->nReadThreads))
			return -1;
	}
	if (enif_get_map_value(env, info, atom_wthreads, &value))
	{
		if (!enif_get_int(env,value,&priv->nWriteThreads))
			return -1;
	}
	if (enif_get_map_value(env, info, atom_nbatch, &value))
	{
		if (!enif_get_int(env,value,&g_nbatch))
			return -1;
	}
	if (enif_get_map_value(env, info, atom_lmdbsync, &value))
	{
		if (!enif_get_int(env,value,&i))
			return -1;
		if (i == 0)
			g_transsync = 0;
		else
			g_transsync = 1;
	}

	if (priv->nReadThreads > 255)
		return -1;
	if (priv->nWriteThreads+1 > 255)
		return -1;

	db_connection_type = enif_open_resource_type(env, NULL, "db_connection_type",
				destruct_connection, ERL_NIF_RT_CREATE, NULL);
	if(!db_connection_type)
		return -1;

	iterate_type =  enif_open_resource_type(env, NULL, "iterate_type",
				destruct_iterate, ERL_NIF_RT_CREATE, NULL);
	if(!iterate_type)
		return -1;

	priv->sockets = calloc(priv->nEnvs*priv->nWriteThreads,sizeof(int)*MAX_CONNECTIONS);
	priv->socketTypes = calloc(priv->nEnvs*priv->nWriteThreads,sizeof(int)*MAX_CONNECTIONS);
	priv->actorIndexes = malloc(sizeof(_Atomic(u64))*priv->nEnvs);
	priv->wtasks = malloc(sizeof(queue*)*(priv->nEnvs*priv->nWriteThreads+1));
	priv->rtasks = malloc(sizeof(queue*)*(priv->nEnvs*priv->nReadThreads));
	priv->tids = malloc(sizeof(ErlNifTid)*(priv->nEnvs*priv->nWriteThreads+1));
	priv->rtids = malloc(sizeof(ErlNifTid)*(priv->nEnvs*priv->nReadThreads));
	priv->wthreads = malloc(sizeof(db_thread*)*(priv->nEnvs*priv->nWriteThreads));
	priv->rthreads = malloc(sizeof(db_thread*)*(priv->nEnvs*priv->nReadThreads));
	priv->wmdb = malloc(sizeof(mdbinf)*priv->nEnvs);

	memset(priv->wmdb, 0, sizeof(mdbinf)*priv->nEnvs);
	atomic_init(&priv->sockUpdate,0);
	// priv->writeBufs = malloc(sizeof(u8*)*priv->nEnvs);

	return start_threads(priv);
}

static void on_unload(ErlNifEnv* env, void* pd)
{
	int i,j,k;
	priv_data *priv = (priv_data*)pd;
	db_command *cmd = NULL;
	DBG("on_unload");

	qitem *item = command_create(-1,-1,priv);
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_stop;
	push_command(-1, -1, priv, item);

	for (i = 0; i < priv->nEnvs; i++)
	{
		// first close read threads, then close write thread, which will close lmdb env
		for (k = 0; k < priv->nReadThreads; k++)
		{
			item = command_create(-1,i*priv->nEnvs + k,priv);
			cmd = (db_command*)item->cmd;
			cmd->type = cmd_stop;
			push_command(-1, i*priv->nEnvs + k, priv, item);
		}
		for (k = 0; k < priv->nWriteThreads; k++)
		{
			item = command_create(i*priv->nEnvs + k,-1,priv);
			cmd = (db_command*)item->cmd;
			cmd->type = cmd_stop;
			push_command(i*priv->nEnvs + k,-1, priv, item);
		}
		for (k = 0; k < priv->nReadThreads; k++)
		{
			enif_thread_join((ErlNifTid)priv->rtids[i*priv->nReadThreads + k],NULL);
			queue_destroy(priv->rtasks[i*priv->nEnvs+k]);
			priv->rtasks[i*priv->nEnvs+k] = NULL;
		}
		for (k = 0; k < priv->nWriteThreads; k++)
		{
			enif_thread_join((ErlNifTid)priv->tids[i*priv->nWriteThreads+k],NULL);
			queue_destroy(priv->wtasks[i*priv->nEnvs+k]);
			priv->wtasks[i*priv->nEnvs+k] = NULL;
		}
		enif_mutex_destroy(priv->wthrMutexes[i]);
		free(priv->paths[i]);
		// free(priv->writeBufs[i]);
	}
	enif_thread_join((ErlNifTid)priv->tids[priv->nEnvs * priv->nWriteThreads],NULL);
	for (i = 0; i < MAX_PREP_SQLS; i++)
	{
		for (j = 0; j < MAX_PREP_SQLS; j++)
		{
			free(priv->prepSqls[i][j]);
			priv->prepSqls[i][j] = NULL;
		}
	}
#ifdef _TESTDBG_
	fclose(g_log);
#endif
	if (priv->pluginFiles)
	{
		for (i = 0; i < priv->nPlugins; i++)
			free(priv->pluginFiles[i]);
		free(priv->pluginFiles);
	}
	enif_mutex_destroy(priv->prepMutex);
	enif_mutex_destroy(priv->sockMutex);
	free(g_counters);
	g_counters = NULL;
	free(priv->paths);
	free(priv->wtasks);
	free(priv->rtasks);
	free(priv->tids);
	free(priv->rtids);
	free(priv->wthrMutexes);
	free(priv->syncNumbers);
	free(priv->actorIndexes);
	free(priv->wmdb);
	free(priv->wthreads);
	free(priv->rthreads);
	free(priv->sockets);
	free(priv->socketTypes);
	// free(priv->writeBufs);
	// free(priv->sqlite_pgcache);
	free(pd);
}

static ErlNifFunc nif_funcs[] = {
	{"open", 5, db_open},
	{"open", 6, db_open},
	{"close", 3, db_close},
	{"checkpoint",4,db_checkpoint},
	{"replicate_opts",3,replicate_opts},
	{"exec_read", 4, exec_read},
	{"exec_read", 5, exec_read},
	{"exec_script", 7, exec_script},
	{"exec_script", 8, exec_script},
	{"noop", 3, noop},
	{"parse_helper",2,parse_helper},
	// {"interrupt_query",1,interrupt_query},
	{"lz4_compress",1,lz4_compress},
	{"lz4_decompress",2,lz4_decompress},
	{"lz4_decompress",3,lz4_decompress},
	{"set_tunnel_connector",0,set_tunnel_connector},
	{"set_thread_fd",4,set_thread_fd},
	{"all_tunnel_call",3,all_tunnel_call},
	{"all_tunnel_call",4,all_tunnel_call},
	{"store_prepared_table",2,store_prepared_table},
	{"checkpoint_lock",4,checkpoint_lock},
	{"iterate_db",4,iterate_db},
	{"iterate_db",5,iterate_db},
	{"iterate_close",1,iterate_close},
	{"page_size",0,page_size},
	{"inject_page",5,inject_page},
	{"wal_rewind",4,wal_rewind},
	{"wal_rewind",5,wal_rewind},
	{"actor_info",4,get_actor_info},
	{"term_store",3,term_store},
	{"term_store",4,term_store},
	{"fsync_num",1,sync_num},
	{"fsync",3,db_sync},
	{"fsync",0,db_sync},
	{"replication_done",1,replication_done},
	{"stmt_info",4,stmt_info},
	{"counter_inc", 2, counter_inc},
};

ERL_NIF_INIT(actordb_driver_nif, nif_funcs, on_load, NULL, NULL, on_unload);
