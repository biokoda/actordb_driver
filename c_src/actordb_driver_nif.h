#ifndef ACTORDB_DRIVER_NIF_H
#define ACTORDB_DRIVER_NIF_H
#ifdef  _WIN32
#define snprintf _snprintf
#endif
#include "lmdb.h"
#ifndef _TESTAPP_
#include "erl_nif.h"
#endif
#define MAX_ATOM_LENGTH 255
#define MAX_PATHNAME 512
#define PAGE_BUFF_SIZE 4300
#define MAX_CONNECTIONS 8
#define PACKET_ITEMS 9
#define MAX_STATIC_SQLS 11
#define MAX_PREP_SQLS 100
#define MAX_ACTOR_NAME 92

#include "queue.h"
#include <stdatomic.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>
// #define TRACK_TIME 1


FILE *g_log = 0;
#if defined(_TESTDBG_)
# define DBG(X, ...)  fprintf(g_log,"thr=%lld: " X "\r\n",(i64)pthread_self(),##__VA_ARGS__) ;fflush(g_log);
#else
# define DBG(X, ...)
#endif


typedef struct db_connection db_connection;
typedef struct db_backup db_backup;
typedef struct db_thread db_thread;
typedef struct control_data control_data;
typedef struct conn_resource conn_resource;
typedef struct wal_file wal_file;

typedef struct WalIndexHdr WalIndexHdr;
typedef struct WalIterator WalIterator;
typedef struct WalCkptInfo WalCkptInfo;
typedef struct iterate_resource iterate_resource;
typedef struct priv_data priv_data;
typedef struct mdbinf mdbinf;

struct mdbinf
{
	MDB_dbi infodb;
	MDB_dbi logdb;
	MDB_dbi pagesdb;
	MDB_dbi actorsdb;
	MDB_env *env;
	MDB_txn *txn;
	MDB_cursor *cursorLog;
	MDB_cursor *cursorPages;
	MDB_cursor *cursorInfo;
	u32 usageCount;
};

struct priv_data
{
	int nEnvs;           // number of environments
	int nReadThreads;
	int nWriteThreads;
	queue **wtasks;      // array of queues for every write thread + control thread
	queue **rtasks;      // every environment has nReadThreads
	atomic_ullong *syncNumbers;
	mdbinf *wmdb;

	#ifndef _TESTAPP_
	ErlNifMutex *prepMutex;
	ErlNifMutex **wthrMutexes;
	ErlNifTid *tids;    // tids for every write thread + control
	ErlNifTid *rtids;    // tids for every read thread
	#endif

	// For actorsdb, when opening a new actor
	// do an atomic increment to get a unique index. Then send a write
	// to write thread for it.
	atomic_llong *actorIndexes;

	int prepSize;
	int prepVersions[MAX_PREP_SQLS][MAX_PREP_SQLS];
	char* prepSqls[MAX_PREP_SQLS][MAX_PREP_SQLS];
	void *sqlite_scratch;
};

struct Wal {
	// db_thread *thread;
	// db_thread *rthread;
	// for access to readSafeXXX values. They are set on write/scheduler thread and read
	// on read thread.
	#ifndef _TESTAPP_
	ErlNifMutex *mtx;
	#endif
	// #ifndef _WIN32
	// pthread_t rthreadId;
	// #else
	// DWORD rthreadId;
	// #endif
	u64 index;
	u64 firstCompleteTerm;
	u64 firstCompleteEvnum;
	u64 lastCompleteTerm;
	u64 lastCompleteEvnum;
	u64 inProgressTerm;
	u64 inProgressEvnum;
	// This is set from lastCompleteXXXX once write is safely replicated.
	// Or after a rewind. Used by read thread.
	// -> not used atm because we don't control the sqlite cache
	//   so we cant set which page versions are used.
	u64 readSafeTerm;
	u64 readSafeEvnum;
	Pgno readSafeMxPage;
	Pgno mxPage;
	u32 allPages; // mxPage + unused pages
};


struct control_data
{
	char addresses[MAX_CONNECTIONS][255];
	int ports[MAX_CONNECTIONS];
	int types[MAX_CONNECTIONS];
	#ifndef _TESTAPP_
	// connection prefixes
	ErlNifBinary prefixes[MAX_CONNECTIONS];
	char isopen[MAX_CONNECTIONS];
	#endif
};


struct db_thread
{
	#ifdef TRACK_TIME
	u8 timeBuf[1024*1024];
	int timeBufPos;
	u8 timeTrack;
	#endif
	MDB_val *resFrames;
	mdbinf mdb;
	u8 *wBuffer;
	int bufSize;

	// For read threads. Before executing sql on a connection, copy over term/evnum upper limit.
	// Reads/writes can be completely asynchronous, at least from our code
	// we make no assumptions about sqlite.
	// We can't allow readSafeTerm/readSafeEvnum to change in the middle of a read.
	// u64 readSafeTerm;
	// u64 readSafeEvnum;

	// Raft page replication
	// MAX_CONNECTIONS (8) servers to replicate write log to
	int sockets[MAX_CONNECTIONS];
	int socket_types[MAX_CONNECTIONS];

	#ifndef _TESTAPP_
	queue *tasks;
	control_data *control;
	ERL_NIF_TERM *columnSpace;
	#endif

	int columnSpaceSize;
	u32 pagesChanged;
	int nThread; // Index of this thread
	int nEnv;    // Environment index of this thread
	int maxvalsize;
	int nResFrames;
	int fd;
	u8 forceCommit;
	u8 isopen;
	u8 isreadonly;

	char staticSqls[MAX_STATIC_SQLS][256];
	int nstaticSqls;
};


struct db_connection
{
	// Write thread index
	u8 wthreadind;
	// Read thread index
	u8 rthreadind;
	// On rewind/inject or wal open.
	// Signals that sqlite should flush cache.
	u8 dirty;
	// Can we do checkpoint or not
	u8 checkpointLock;
	// 0   - do not replicate
	// > 0 - replicate to socket types that match number
	u8 doReplicate;
	u8 changed;

	struct Wal wal;
	sqlite3 *db;
	sqlite3_stmt **staticPrepared;
	sqlite3_stmt **prepared;
	int *prepVersions;
	u64 syncNum;

	#ifndef _TESTAPP_
	// Fixed part of packet prefix
	char* packetPrefix;
	int packetPrefixSize;
	// Variable part of packet prefix
	ErlNifBinary packetVarPrefix;
	#endif
	// char wal_configured;
};

struct iterate_resource
{
	u64 evnum;
	u64 evterm;
	u32 pgnoPos;

	int thread;
	db_connection *conn;

	u32 mxPage;

	char started;
	char entiredb;
	char termMismatch;
	char closed;
};

typedef enum
{
	cmd_unknown = 0,
	cmd_open  = 1,
	cmd_exec_script  = 2,
	cmd_stop  = 4,
	cmd_interrupt  = 8,
	cmd_tcp_connect  = 9,
	cmd_set_socket  = 10,
	cmd_tcp_reconnect  = 11,
	// cmd_bind_insert  = 12,
	cmd_alltunnel_call  = 13,
	// cmd_store_prepared  = 14,
	cmd_checkpoint_lock  = 15,
	cmd_iterate  = 16,
	cmd_inject_page  = 17,
	cmd_wal_rewind  = 18,
	cmd_checkpoint = 20,
	cmd_term_store = 21,
	cmd_actor_info = 22,
	cmd_sync = 23,
	cmd_stmt_info = 24,
	cmd_file_write = 25,
	cmd_actorsdb_add = 26
} command_type;

typedef struct
{
	// void *p;
	db_connection *conn;
#ifndef _TESTAPP_
	ERL_NIF_TERM ref;
	ErlNifPid pid;
	ERL_NIF_TERM arg;
	ERL_NIF_TERM arg1;
	ERL_NIF_TERM arg2;
	ERL_NIF_TERM arg3;
	ERL_NIF_TERM arg4;
#endif
	// int connindex;
	command_type type;
} db_command;


#ifdef TRACK_TIME
#include <mach/mach_time.h>

void track_time(u8 id, db_thread *thr);
void track_flag(db_thread *thr, u8 flag);

void track_flag(db_thread *thr, u8 flag)
{
	thr->timeTrack = flag;
}
void track_time(u8 id, db_thread *thr)
{
	if (thr->timeTrack && thr->timeBufPos+sizeof(u64) < sizeof(thr->timeBuf))
	{
		u64 t = mach_absolute_time();
		thr->timeBuf[thr->timeBufPos] = id;
		memcpy(thr->timeBuf+thr->timeBufPos+1, &t, sizeof(u64));
		thr->timeBufPos += sizeof(u64) + 1;
	}
}
#else
#define track_time(X,Y)
#define track_flag(X,Y)
#endif

#ifndef _TESTAPP_
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

static ERL_NIF_TERM make_cell(ErlNifEnv *env, sqlite3_stmt *statement, unsigned int i);
static ERL_NIF_TERM push_command(int thread, int readThreadNum,priv_data *pd, qitem *cmd);
static ERL_NIF_TERM make_binary(ErlNifEnv *env, const void *bytes, unsigned int size);
// int wal_hook(void *data,sqlite3* db,const char* nm,int npages);
static qitem *command_create(int threadnum,int readThreadNum,priv_data* pd);
static ERL_NIF_TERM do_tcp_connect1(db_command *cmd, db_thread* thread, int pos, ErlNifEnv *env);
static int bind_cell(ErlNifEnv *env, const ERL_NIF_TERM cell, sqlite3_stmt *stmt, unsigned int i);
void errLogCallback(void *pArg, int iErrCode, const char *zMsg);
void fail_send(int i,priv_data *priv);
#endif


int reopen_db(db_connection *conn, db_thread *thread);
void close_prepared(db_connection *conn);
// SQLITE_API int sqlite3_wal_data(sqlite3 *db,void *pArg);
int checkpoint_continue(db_thread *thread);
int read_wal_hdr(sqlite3_vfs *vfs, sqlite3_file *pWalFd, wal_file **outWalFile);
int read_thread_wal(db_thread*);



#endif
