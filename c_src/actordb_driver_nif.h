#ifndef ACTORDB_DRIVER_NIF_H


#ifdef  _WIN32
// #include "dirent.h"
#define snprintf _snprintf
#else
#include <dirent.h>
#endif
#include "lmdb.h"

#define MAX_ATOM_LENGTH 255
#define MAX_PATHNAME 512
#define PAGE_BUFF_SIZE 5300
#define MAX_CONNECTIONS 8
#define PACKET_ITEMS 9
#define MAX_STATIC_SQLS 11
#define MAX_PREP_SQLS 100
#define MAX_ACTOR_NAME 92


FILE *g_log = 0;
#if defined(_TESTDBG_)
# define DBG(X)  fprintf X ;fflush(g_log);
#else
# define DBG(X)
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
typedef struct queue_t queue;
typedef struct qitem_t qitem;
typedef struct priv_data priv_data;

typedef u16 ht_slot;


struct priv_data
{
	queue **tasks;      // array of queues for every thread + control thread
	int nthreads;       // number of work threads

	#ifndef _TESTAPP_
	ErlNifTid *tids;    // tids for every thread
	ErlNifResourceType *db_connection_type;
	ErlNifResourceType *db_backup_type;
	ErlNifResourceType *iterate_type;
	#endif
};

struct Wal {
	db_thread *thread;
	u64 index;
	u64 firstCompleteTerm;
	u64 firstCompleteEvnum;
	u64 lastCompleteTerm;
	u64 lastCompleteEvnum;
	u64 inProgressTerm;
	u64 inProgressEvnum;
	MDB_val curFrame;
	Pgno mxPage;
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

// struct wal_file
// {
//   wal_file *prev;
//   sqlite3_file *pWalFd;
//   u64 walIndex;
// 	u32 mxFrame;
// 	int szPage;
// 	u32 aSalt[2];
//   u32 lastCommit;    // frame number of last commit. Used in sqlite3WalUndo
//   u32 checkpointPos; // checkpoints are done one actor at a time from start to end of conns table.
//                      // once we reach the end of actor table, we are done.
//   u8 bigEndCksum;
//   char filename[MAX_PATHNAME];
// };

struct db_thread
{
	MDB_dbi infodb;
	MDB_dbi logdb;
	MDB_dbi pagesdb;
	MDB_dbi actorsdb;
	// MDB_dbi testdb;
	MDB_env *env;
	MDB_txn *wtxn;
    MDB_txn *rtxn;
	MDB_cursor *cursorLog;
	MDB_cursor *cursorPages;
	MDB_cursor *cursorInfo;
	// MDB_cursor *cursorTest;
	// sqlite3_vfs *vfs;
	// so currently executing connection data is accessible from wal callback
	db_connection *curConn;
	db_connection* conns;
	int nconns;

	// wal_file *walFile;

	// Raft page replication
	// MAX_CONNECTIONS (8) servers to replicate write log to
	int sockets[MAX_CONNECTIONS];
	int socket_types[MAX_CONNECTIONS];
	void (*wal_page_hook)(void *data,void *page,int pagesize,void* header, int headersize);

	#ifndef _TESTAPP_
	queue *tasks;
	control_data *control;
	#endif

	unsigned int dbcount;
	unsigned int inactivity;
	int isopen;
	// initialized to random, increased for every call.
	// it is used to distinguish writes to same actor from each other and detect
	// when they should be rollbacked (if they do not end with db size number).
	// Unlike regular sqlite wal files, in actordb wal files can be intertwined.
	u32 threadNum;
	u32 walSizeLimit; // in pages. So wal file in bytes is walSizeLimit*pagesize
	u32 pagesChanged;
	u8 forceCommit;
	int index;        // Index in table of threads.
	int nthreads;

	// Maps DBPath (relative path to db) to connections index.
	Hash walHash;
	// All DB paths are relative to this thread path.
	// This path is absolute and stems from app.config (main_db_folder, extra_db_folders).
	char path[MAX_PATHNAME];
	int pathlen;
	char staticSqls[MAX_STATIC_SQLS][256];
	int nstaticSqls;

	// Prepared statements. 2d array (for every type, list of sqls and versions)
	int prepSize;
	int prepVersions[MAX_PREP_SQLS][MAX_PREP_SQLS];
	char* prepSqls[MAX_PREP_SQLS][MAX_PREP_SQLS];
	priv_data *pd;
};


struct db_connection
{
	struct Wal wal;
	u64 writeNumber;
	u64 writeTermNumber;
	sqlite3 *db;
	char *dbpath;
	sqlite3_stmt **staticPrepared;
	sqlite3_stmt **prepared;
	int *prepVersions;
	// When actor is requesting pages from wal for replication use this iterator.
	// WalIterator *walIter;
	// // Pointer to wal structure that iterator belongs to.
	// // Iterators move from oldest wal to youngest
	// Wal* iterWal;
	#ifndef _TESTAPP_
	// Fixed part of packet prefix
	char* packetPrefix;
	int packetPrefixSize;
	// Variable part of packet prefix
	ErlNifBinary packetVarPrefix;
	#endif
	u32 lastWriteThreadNum;
	u32 writeNumToIgnore;
	// index in thread table of actors (thread->conns)
	int connindex;
	// 0   - do not replicate
	// > 0 - replicate to socket types that match number
	int doReplicate;
	int thread;
	// Is db open from erlang. It may just be open in driver.
	char nErlOpen;
	char checkpointLock;
	char wal_configured;
	// For every write:
	// over how many connections data has been sent
	char nSent;
	// Set bit for every failed attempt to write to socket of connection
	char failFlags;
	char needRestart;
};

struct conn_resource
{
	int thread;
	int connindex;
	char checkpointLock;
	char dodelete;
};

struct iterate_resource
{
	i64 iOffset;
	u64 evnumFrom;
	u64 evtermFrom;
	u64 walIndex;

	int thread;
	int connindex;

	char started;
	char closed;
};

/* backup object */
struct db_backup
{
	sqlite3_backup *b;
	sqlite3 *dst;
	sqlite3 *src;
	int pages_for_step;
	int thread;
};


typedef enum
{
	cmd_unknown = 0,
	cmd_open  = 1,
	cmd_exec_script  = 2,
	cmd_close  = 3,
	cmd_stop  = 4,
	cmd_backup_init  = 5,
	cmd_backup_step  = 6,
	cmd_backup_finish  = 7,
	cmd_interrupt  = 8,
	cmd_tcp_connect  = 9,
	cmd_set_socket  = 10,
	cmd_tcp_reconnect  = 11,
	cmd_bind_insert  = 12,
	cmd_alltunnel_call  = 13,
	cmd_store_prepared  = 14,
	cmd_checkpoint_lock  = 15,
	cmd_iterate_wal  = 16,
	cmd_inject_page  = 17,
	cmd_wal_rewind  = 18,
	cmd_replicate_opts = 19
} command_type;

typedef struct
{
	// void *p;
	db_connection *conn;
#ifndef _TESTAPP_
	ErlNifEnv *env;
	ERL_NIF_TERM ref;
	ErlNifPid pid;
	ERL_NIF_TERM arg;
	ERL_NIF_TERM arg1;
	ERL_NIF_TERM arg2;
	ERL_NIF_TERM arg3;
	ERL_NIF_TERM arg4;
#endif
	int connindex;
	command_type type;
} db_command;

struct qitem_t
{
	qitem* next;
	db_command cmd;
	char blockStart;
};



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

static ERL_NIF_TERM make_cell(ErlNifEnv *env, sqlite3_stmt *statement, unsigned int i);
static ERL_NIF_TERM push_command(int thread,priv_data *pd, qitem *cmd);
static ERL_NIF_TERM make_binary(ErlNifEnv *env, const void *bytes, unsigned int size);
// int wal_hook(void *data,sqlite3* db,const char* nm,int npages);
qitem *command_create(int threadnum,priv_data* pd);
static ERL_NIF_TERM do_tcp_connect1(db_command *cmd, db_thread* thread, int pos);
static int bind_cell(ErlNifEnv *env, const ERL_NIF_TERM cell, sqlite3_stmt *stmt, unsigned int i);
void errLogCallback(void *pArg, int iErrCode, const char *zMsg);
void fail_send(int i,priv_data *priv);
#endif


int reopen_db(db_connection *conn, db_thread *thread);
void close_prepared(db_connection *conn);
// int wal_iterate_from(db_connection *conn, iterate_resource *iter, int bufSize, u8* buffer, int *nFilled,char *activeWal);
SQLITE_API int sqlite3_wal_data(sqlite3 *db,void *pArg);
// int wal_rewind(db_connection *conn, u64 evnum);
// int wal_iterate(db_connection *conn, int bufSize, char* buffer, char *done, char *activeWal);
int checkpoint_continue(db_thread *thread);
// wal_file *new_wal_file(char* filename,sqlite3_vfs *vfs);
int read_wal_hdr(sqlite3_vfs *vfs, sqlite3_file *pWalFd, wal_file **outWalFile);
// int read_thread_wal(db_thread*);
u64 readUInt64(u8* buf);
int read_thread_wal(db_thread*);
void writeUInt64(unsigned char* buf, u64 num);
void write32bit(char *p, int v);
void write16bit(char *p, int v);


queue *queue_create(void);
void queue_destroy(queue *queue);
int queue_push(queue *queue, qitem* item);
qitem* queue_pop(queue *queue);
// void* queue_get_item_data(void* item);
// void queue_set_item_data(void* item, void *ndata);
void queue_recycle(queue *queue,qitem* item);
qitem* queue_get_item(queue *queue);
int queue_size(queue *queue);

#endif
