// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
// #define _TESTDBG_ 1
#ifdef __linux__
#define _GNU_SOURCE 1
#include <sys/mman.h>
#include <dlfcn.h>
#endif

#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>

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

// wal.c code has been taken out of sqlite3.c and placed in wal.c file.
// Every wal interface function is changed, but the wal-index code remains unchanged.
#include "wal.c"
#include "queue.c"
#include "nullvfs.c"

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

// static ERL_NIF_TERM
// make_row_tuple(ErlNifEnv *env, ERL_NIF_TERM value)
// {
//     return enif_make_tuple2(env, make_atom(env, "row"), value);
// }


// int
// wal_hook(void *data,sqlite3* db,const char* nm,int npages)
// {
//     db_connection *conn = (db_connection *) data;
//     conn->nPrevPages = conn->nPages;
//     conn->nPages = npages;
//     return SQLITE_OK;
// }

static void wal_page_hook(void *data,void *buff,int buffUsed,void* header, int headersize)
{
	db_thread *thread = (db_thread *) data;
	db_connection *conn = thread->curConn;
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
	conn->nSent = conn->failFlags = 0;

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
				conn->failFlags |= (1 << i);
				close(thread->sockets[i]);
				thread->sockets[i] = 0;
				fail_send(i,thread->pd);
			}
			else
			{
				// rt = recv(thread->sockets[i],confirm,6,0);
				// if (rt != 6 || confirm[4] != 'o' || confirm[5] != 'k')
				// {
				//     conn->failFlags |= (1 << i);
				//     close(thread->sockets[i]);
				//     thread->sockets[i] = 0;
				//     fail_send(i);
				// }
				conn->nSent++;
			}
		}
	}

	// var prefix only sent with first packet
	conn->packetVarPrefix.data = NULL;
	conn->packetVarPrefix.size = 0;
}

void fail_send(int i,priv_data *priv)
{
	// tell control thread to create new connections for position i
	DBG((g_log,"FAIL SEND!\n"));
	qitem *item = command_create(-1,-1,priv);
	item->cmd.type = cmd_tcp_connect;
	item->cmd.arg3 = enif_make_int(item->cmd.env,i);
	push_command(-1,-1, priv, item);
}


static ERL_NIF_TERM do_all_tunnel_call(db_command *cmd,db_thread *thread)
{
#ifndef  _WIN32
	struct iovec iov[PACKET_ITEMS];
#else
	WSABUF iov[PACKET_ITEMS];
#endif
	u8 packetLen[4];
	u8 lenBin[2];
	ErlNifBinary bin;
	int nsent = 0, i = 0, rt = 0;
	// char confirm[7] = {0,0,0,0,0,0,0};

	enif_inspect_iolist_as_binary(cmd->env,cmd->arg,&(bin));

	put4byte(packetLen,bin.size);
	put2byte(lenBin,bin.size);

#ifndef  _WIN32
	iov[0].iov_base = packetLen;
	iov[0].iov_len = 4;
	iov[1].iov_len = bin.size;
	iov[1].iov_base = bin.data;
#else
	iov[0].buf = packetLen;
	iov[0].len = 4;
	iov[1].len = bin.size;
	iov[1].buf = bin.data;
#endif

	for (i = 0; i < MAX_CONNECTIONS; i++)
	{
		if (thread->sockets[i] > 3 && thread->socket_types[i] == 1)
		{
#ifndef _WIN32
			rt = writev(thread->sockets[i],iov,2);
#else
			if (WSASend(thread->sockets[i],iov,2, &rt, 0, NULL, NULL) != 0)
				rt = 0;
#endif
			if (rt == -1)
			{
				close(thread->sockets[i]);
				thread->sockets[i] = 0;
				fail_send(i,thread->pd);
			}
			// rt = recv(thread->sockets[i],confirm,6,0);
			// if (rt != 6 || confirm[4] != 'o' || confirm[5] != 'k')
			// {
			//     close(thread->sockets[i]);
			//     thread->sockets[i] = 0;

			//     fail_send(i);
			// }
			nsent++;
		}
	}

	return enif_make_int(cmd->env,nsent);
}


static ERL_NIF_TERM do_store_prepared_table(db_command *cmd,db_thread *thread)
{
	// Delete old table of prepared statements and set new one.
	const ERL_NIF_TERM *versTuple;
	const ERL_NIF_TERM *sqlTuple;
	const ERL_NIF_TERM *versRow;
	const ERL_NIF_TERM *sqlRow;
	int tupleSize,rowSize,i,j;
	ErlNifBinary bin;

	memset(thread->prepVersions,0,sizeof(thread->prepVersions));
	for (i = 0; i < MAX_PREP_SQLS; i++)
	{
		for (j = 0; j < MAX_PREP_SQLS; j++)
		{
			free(thread->prepSqls[i][j]);
			thread->prepSqls[i][j] = NULL;
		}
	}

	// {{1,2,2,0,0,..},{0,0,0,0,...}}
	if (!enif_get_tuple(cmd->env, cmd->arg, &tupleSize, &versTuple))
		return atom_false;
	// {{"select....","insert into..."},{"select....","update ...."}}
	if (!enif_get_tuple(cmd->env, cmd->arg1, &tupleSize, &sqlTuple))
		return atom_false;

	if (tupleSize > MAX_PREP_SQLS)
		return atom_false;

	for (i = 0; i < tupleSize; i++)
	{
		if (!enif_get_tuple(cmd->env, versTuple[i], &rowSize, &versRow))
			break;
		if (!enif_get_tuple(cmd->env, sqlTuple[i], &rowSize, &sqlRow))
			break;

		thread->prepSize = rowSize;
		for (j = 0; j < rowSize; j++)
		{
			enif_get_int(cmd->env,versRow[j],&(thread->prepVersions[i][j]));
			if (enif_is_list(cmd->env,sqlRow[j]) || enif_is_binary(cmd->env,sqlRow[j]))
			{
				enif_inspect_iolist_as_binary(cmd->env,sqlRow[j],&bin);
				thread->prepSqls[i][j] = malloc(bin.size+1);
				thread->prepSqls[i][j][bin.size] = 0;
				memcpy(thread->prepSqls[i][j],bin.data,bin.size);
			}
		}
	}
	return atom_ok;
}

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

static ERL_NIF_TERM make_sqlite3_error_tuple(ErlNifEnv *env,const char* calledfrom, int error_code, sqlite3 *db)
{
	const char *error_code_msg = get_sqlite3_return_code_msg(error_code);
	const char *msg = get_sqlite3_error_msg(error_code, db);

	if (calledfrom == NULL)
		return enif_make_tuple2(env, atom_error,
			enif_make_tuple3(env, enif_make_string(env,"",ERL_NIF_LATIN1),
							  make_atom(env, error_code_msg),
							  enif_make_string(env, msg, ERL_NIF_LATIN1)));
	else
		return enif_make_tuple2(env, atom_error,
			enif_make_tuple3(env, enif_make_string(env,calledfrom,ERL_NIF_LATIN1),
							  make_atom(env, error_code_msg),
							  enif_make_string(env, msg, ERL_NIF_LATIN1)));
}

// static void
// command_destroy(db_command cmd)
// {
//     if(cmd.env != NULL)
//        enif_free_env(cmd.env);
// }

static qitem* command_create(int threadnum, int readThreadNum, priv_data *p)
{
	queue *thrCmds = NULL;
	qitem *item;
	ErlNifEnv *env;
	if (threadnum == -1)
		thrCmds = p->wtasks[p->nthreads];
	else if (readThreadNum == -1)
		thrCmds = p->wtasks[threadnum];
	else
		thrCmds = p->rtasks[threadnum * p->nReadThreads + readThreadNum];

	item = queue_get_item(thrCmds);
	if (item->cmd.env == NULL)
	{
		item->cmd.env = enif_alloc_env();
	}
	env = item->cmd.env;
	memset(&item->cmd,0,sizeof(db_command));
	item->cmd.env = env;

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
	DBG((g_log,"destruct connection\n"));

	if (conn->packetPrefix)
	{
		free(conn->packetPrefix);
		conn->packetPrefix = NULL;
		conn->packetPrefixSize = 0;
	}
	close_prepared(conn);
	rc = sqlite3_close(conn->db);
	if(rc != SQLITE_OK)
	{
		DBG((g_log,"ERROR! closing %d\n",rc));
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

	DBG((g_log,"destruct iterate.\n"));
	if (res->closed)
		return;

	// enif_release_resource(conn);
	item = command_create(res->thread,-1,pd);

	item->cmd.type = cmd_checkpoint_lock;
	item->cmd.arg = enif_make_int(item->cmd.env, 0);
	// No keep. This way release for connection will be called on thread when done with command.
	// It will decrement keep on connection held by iterate.
	item->cmd.conn = res->conn;

	push_command(res->thread, -1, pd, item);
}


static ERL_NIF_TERM do_open(db_command *cmd, db_thread *thread)
{
	static int readThrIndex = 0;
	char filename[MAX_PATHNAME];
	unsigned int size;
	int rc;
	ERL_NIF_TERM result;
	db_connection *conn;
	char mode[10];
	sqlite3 *db;

	memset(filename,0,MAX_PATHNAME);

	enif_get_atom(cmd->env,cmd->arg1,mode,10,ERL_NIF_LATIN1);
	if (strcmp(mode,"wal") != 0)
		return atom_false;

	size = enif_get_string(cmd->env, cmd->arg, filename, MAX_PATHNAME, ERL_NIF_LATIN1);
	// Actor path name must be written to wal. Filename slot is MAX_ACTOR_NAME bytes.
	if(size <= 0 || size >= MAX_ACTOR_NAME)
		return make_error_tuple(cmd->env, "invalid_filename");

	rc = sqlite3_open(filename,&(db));
	if(rc != SQLITE_OK)
	{
		result = make_sqlite3_error_tuple(cmd->env, "sqlite3_open", rc, cmd->conn->db);
		sqlite3_close(db);
		cmd->conn = NULL;
		return result;
	}
	conn = enif_alloc_resource(thread->pd->db_connection_type, sizeof(db_connection));
	if(!conn)
		return make_error_tuple(cmd->env, "no_memory");
	memset(conn,0,sizeof(db_connection));

	cmd->conn = conn;
	thread->curConn = cmd->conn;
	conn->db = db;
	conn->thread = thread->index;
	conn->wal.thread = thread;
	conn->rthread = readThrIndex % thread->pd->nReadThreads;
	conn->wal.mtx = enif_mutex_create("conmutex");

	if (filename[0] != ':')
	{
		conn->wal_configured = SQLITE_OK == sqlite3_wal_data(cmd->conn->db,(void*)thread);

		// PRAGMA locking_mode=EXCLUSIVE
		sqlite3_exec(cmd->conn->db,"PRAGMA synchronous=0;PRAGMA journal_mode=wal;",NULL,NULL,NULL);
		// else if (strcmp(mode,"off") == 0)
		// 	sqlite3_exec(cmd->conn->db,"PRAGMA journal_mode=off;",NULL,NULL,NULL);
		// else if (strcmp(mode,"delete") == 0)
		// 	sqlite3_exec(cmd->conn->db,"PRAGMA journal_mode=delete;",NULL,NULL,NULL);
		// else if (strcmp(mode,"truncate") == 0)
		// 	sqlite3_exec(cmd->conn->db,"PRAGMA journal_mode=truncate;",NULL,NULL,NULL);
		// else if (strcmp(mode,"persist") == 0)
		// 	sqlite3_exec(cmd->conn->db,"PRAGMA journal_mode=persist;",NULL,NULL,NULL);
	}

	DBG((g_log,"opened new thread=%d name=%s mode=%s.\n",thread->index,filename,mode));
	result = enif_make_resource(cmd->env, conn);

	readThrIndex++;

	return result;
}

static ERL_NIF_TERM do_interrupt(db_command *cmd, db_thread *thread)
{
	sqlite3_interrupt(cmd->conn->db);
	// enif_release_resource(cmd->conn);
	return atom_error;
}

static ERL_NIF_TERM do_tcp_reconnect(db_command *cmd, db_thread *thread)
{
	int i;

	if (!thread->control)
		return atom_ok;

	for (i = 0; i < MAX_CONNECTIONS; i++)
	{
		// address set and not open
		if (thread->control->addresses[i][0] && !thread->control->isopen[i])
		{
			do_tcp_connect1(cmd,thread, i);
		}
	}
	return atom_ok;
}

static ERL_NIF_TERM do_tcp_connect(db_command *cmd, db_thread *thread)
{
	int pos;
	ErlNifBinary bin;
	if (!thread->control)
	{
		thread->control = enif_alloc(sizeof(control_data));
		memset(thread->control,0,sizeof(control_data));
	}
	if (!enif_get_int(cmd->env,cmd->arg3,&pos))
		return enif_make_badarg(cmd->env);

	if (pos < 0 || pos > 7)
		return enif_make_badarg(cmd->env);

	// this can be called from erlang, or it can be called
	// from a thread that has lost connection.
	// If called from a thread, only pos is sent in arg3. Everything else
	//  has already been set on first call from erlang.
	if (cmd->arg)
	{
		if (!enif_get_string(cmd->env, cmd->arg,thread->control->addresses[pos],255,ERL_NIF_LATIN1))
			return enif_make_badarg(cmd->env);
		if (!enif_get_int(cmd->env,cmd->arg1,&(thread->control->ports[pos])))
			return enif_make_badarg(cmd->env);
		if (!enif_inspect_iolist_as_binary(cmd->env,cmd->arg2,&bin))
			return enif_make_badarg(cmd->env);

		enif_alloc_binary(bin.size,&(thread->control->prefixes[pos]));
		memcpy(thread->control->prefixes[pos].data,bin.data,bin.size);
		if (cmd->arg4)
		{
			if (!enif_get_int(cmd->env,cmd->arg4,&(thread->control->types[pos])))
				return enif_make_badarg(cmd->env);
		}
		else
			thread->control->types[pos] = 1;
	}
	else
	{
		bin = thread->control->prefixes[pos];
	}

	return do_tcp_connect1(cmd,thread,pos);
}

static ERL_NIF_TERM do_tcp_connect1(db_command *cmd, db_thread* thread, int pos)
{
	int i;
	// struct sockaddr_in addr;
	int fd;
	priv_data *pd = thread->pd;
	ERL_NIF_TERM result = atom_ok;
#ifndef _WIN32
	struct iovec iov[2];
#else
	WSABUF iov[2];
#endif
	char portstr[10];
	u8 packetLen[4];
	int *sockets;
	char confirm[7] = {0,0,0,0,0,0,0};
	int flag = 1, rt = 0, error = 0, opts;
	socklen_t errlen = sizeof error;
	struct timeval timeout;
	fd_set fdset;
	struct addrinfo *addrlist;
	struct addrinfo *adrp;


	put4byte(packetLen,thread->control->prefixes[pos].size);
#ifndef _WIN32
	iov[0].iov_base = packetLen;
	iov[0].iov_len = 4;
	iov[1].iov_base = thread->control->prefixes[pos].data;
	iov[1].iov_len = thread->control->prefixes[pos].size;
#else
	iov[0].buf = packetLen;
	iov[0].len = 4;
	iov[1].buf = thread->control->prefixes[pos].data;
	iov[1].len = thread->control->prefixes[pos].size;
#endif

	sockets = enif_alloc(sizeof(int)*pd->nthreads);
	memset(sockets,0,sizeof(int)*pd->nthreads);

	for (i = 0; i < pd->nthreads; i++)
	{
		fd = socket(AF_INET,SOCK_STREAM,0);

#ifndef _WIN32
		if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1)
#else
		opts = 1;
		if (ioctlsocket(fd, FIONBIO, &opts) != 0)
#endif
		{
			close(fd);
			result = make_error_tuple(cmd->env,"noblock");
			break;
		}

		DBG((g_log,"Connecting to port %s:%d\n",thread->control->addresses[pos],thread->control->ports[pos]));

		// memset(&addr,0,sizeof(addr));
		// addr.sin_family = AF_INET;
		// addr.sin_addr.s_addr = inet_addr(thread->control->addresses[pos]);
		// addr.sin_port = htons(thread->control->ports[pos]);
		snprintf(portstr,9,"%d",thread->control->ports[pos]);
		if (getaddrinfo(thread->control->addresses[pos], portstr, NULL, &addrlist) != 0)
		{
			close(fd);
			result = make_error_tuple(cmd->env,"getaddrinfo");
			break;
		}
		for(adrp = addrlist; adrp != NULL; adrp = adrp->ai_next)
		{
			if (adrp->ai_family == AF_INET && adrp->ai_socktype == SOCK_STREAM)
			{
				rt = connect(fd, adrp->ai_addr, adrp->ai_addrlen);
				break;
			}
		}
		if (adrp == NULL)
			result = make_error_tuple(cmd->env,"findaddrinfo");

		freeaddrinfo(addrlist);
#ifndef _WIN32
		if (errno != EINPROGRESS)
#else
		if (WSAGetLastError() != WSAEWOULDBLOCK)
#endif
		{
			close(fd);
			result = make_error_tuple(cmd->env,"connect");
			break;
		}

		FD_ZERO(&fdset);
		FD_SET(fd, &fdset);
		timeout.tv_sec = 1;
		timeout.tv_usec = 0;

		rt = select(fd + 1, NULL, &fdset, NULL, &timeout);
#ifndef _WIN32
		getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &errlen);
#else
		getsockopt(fd, SOL_SOCKET, SO_ERROR, (char*)&error, &errlen);
#endif

		if (rt != 1 || error != 0)
		{
			close(fd);
			result = make_error_tuple(cmd->env,"connect");
			break;
		}

#ifndef _WIN32
		opts = fcntl(fd,F_GETFL);
		if (fcntl(fd, F_SETFL, opts & (~O_NONBLOCK)) == -1 || fcntl(fd,F_GETFL) & O_NONBLOCK)
#else
		opts = 0;
		if (ioctlsocket(fd, FIONBIO, &opts) != 0)
#endif
		{
			close(fd);
			result = make_error_tuple(cmd->env,"blocking");
			break;
		}

		if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&flag, sizeof(int)) != 0)
		{
			close(fd);
			result = make_error_tuple(cmd->env,"keepalive");
			break;
		}
		if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&flag, sizeof(int)) != 0)
		{
			close(fd);
			result = make_error_tuple(cmd->env,"reuseaddr");
			break;
		}
		if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int)) != 0)
		{
			close(fd);
			result = make_error_tuple(cmd->env,"nodelay");
			break;
		}
#ifdef SO_NOSIGPIPE
		if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&flag, sizeof(int)) != 0)
		{
		  close(fd);
		  result = make_error_tuple(cmd->env,"nosigpipe");
		  break;
		}
#endif

		timeout.tv_sec = 2;
		timeout.tv_usec = 0;
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(timeout));
		timeout.tv_sec = 2;
		timeout.tv_usec = 0;
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout,sizeof(timeout));
#ifndef _WIN32
		rt = writev(fd,iov,2);
#else
		if (WSASend(fd,iov,2, &rt, 0, NULL, NULL) != 0)
			rt = 0;
#endif
		if (thread->control->prefixes[pos].size+4 != rt)
		{
			close(fd);
			result = make_error_tuple(cmd->env,"initialize");
			break;
		}

		rt = recv(fd,confirm,6,0);
		if (rt != 6 || confirm[4] != 'o' || confirm[5] != 'k')
		{
			close(fd);
			result = make_error_tuple(cmd->env,"initialize");
			break;
		}

		sockets[i] = fd;
	}

	if (result == atom_ok)
	{
		thread->control->isopen[pos] = 1;

		for (i = 0; i < pd->nthreads; i++)
		{
			qitem *item = command_create(i,-1,thread->pd);
			item->cmd.type = cmd_set_socket;
			item->cmd.arg = enif_make_int(item->cmd.env,sockets[i]);
			item->cmd.arg1 = enif_make_int(item->cmd.env,pos);
			item->cmd.arg2 = enif_make_int(item->cmd.env,thread->control->types[pos]);
			push_command(i, -1, thread->pd, item);
		}
	}
	else
	{
		thread->control->isopen[pos] = 0;

		for (i = 0; i < pd->nthreads; i++)
		{
			if (sockets[i])
				close(sockets[i]);
		}
	}
	enif_free(sockets);

	return result;
}


static ERL_NIF_TERM do_iterate(db_command *cmd, db_thread *thread)
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

	if (!enif_get_resource(cmd->env, cmd->arg, thread->pd->iterate_type, (void **) &iter))
	{
		dorel = 1;
		DBG((g_log,"Create iterate %d\n",(int)cmd->conn->checkpointLock));

		if (!enif_get_uint64(cmd->env,cmd->arg,(ErlNifUInt64*)&evtermFrom))
			return enif_make_badarg(cmd->env);
		if (cmd->arg1 == 0 || !enif_get_uint64(cmd->env,cmd->arg1,(ErlNifUInt64*)&evnumFrom))
		{
			return enif_make_badarg(cmd->env);
		}

		iter = enif_alloc_resource(thread->pd->iterate_type, sizeof(iterate_resource));
		if(!iter)
			return make_error_tuple(cmd->env, "no_memory");
		memset(iter,0,sizeof(iterate_resource));
		iter->thread = thread->index;
		iter->evnum = evnumFrom;
		iter->evterm = evtermFrom;
		iter->conn = cmd->conn;
		// Connection must not close before iterate is closed
		enif_keep_resource(iter->conn);
		// Creating a iterator requires checkpoint lock.
		// On iterator destruct lock will be released.
		cmd->conn->checkpointLock++;

		res = enif_make_resource(cmd->env,iter);
	}
	else
	{
		res = cmd->arg;
	}

	// 4 pages of buffer size
	// This might contain many more actual db pages because data is compressed
	nfilled = iterate(&cmd->conn->wal, iter, buf, PAGE_BUFF_SIZE, hdrbuf, &done);
	DBG((g_log,"nfilled %d\n",nfilled));

	if (nfilled > 0)
	{
		enif_alloc_binary(sizeof(u64)*2+sizeof(u32)*2, &header);
		enif_alloc_binary(nfilled, &bin);
		memcpy(bin.data, buf, nfilled);
		memcpy(header.data, hdrbuf, sizeof(hdrbuf));

		tBin = enif_make_binary(cmd->env,&bin);
		tHead = enif_make_binary(cmd->env,&header);
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
		DBG((g_log,"RET DONE\n"));
		if (mismatch)
			return enif_make_tuple2(cmd->env, atom_ok, enif_make_uint64(cmd->env,evterm));
		return atom_done;
	}
	else
	{
		tDone = enif_make_uint(cmd->env,done);
		return enif_make_tuple5(cmd->env,atom_ok, enif_make_tuple2(cmd->env,atom_iter,res), tBin, tHead, tDone);
	}
}


static ERL_NIF_TERM do_wal_rewind(db_command *cmd, db_thread *thr)
{
	// Rewind discards a certain number of writes. This may happen due to consensus conflicts.
	// Rewind is very similar to checkpoint. Checkpoint goes from beginning (from firstCompleteEvterm/Evnum),
	// forward to some limit. Rewind goes from the end (from lastCompleteEvterm/Evnum) backwards to some limit evnum (including the limit).
	// DB may get shrunk in the process.
	MDB_val logKey, logVal;
	u8 logKeyBuf[sizeof(u64)*3];
	int logop, pgop, rc;
	u64 evnum,evterm,aindex,limitEvnum;
	Wal *pWal = &cmd->conn->wal;

	enif_get_uint64(cmd->env,cmd->arg,(ErlNifUInt64*)&limitEvnum);

	DBG((g_log,"do_wal_rewind\r\n"));

	// if limitEvnum == 0, this means delete all pages for actor.
	if (limitEvnum < pWal->firstCompleteEvnum && limitEvnum > 0)
		return atom_false;

	if (pWal->inProgressTerm > 0 || pWal->inProgressEvnum > 0)
	{
		DBG((g_log,"undo before rewind, %llu, %llu\n",pWal->inProgressTerm, pWal->inProgressEvnum));
		doundo(&cmd->conn->wal, NULL, NULL, 1);
	}

	logKey.mv_data = logKeyBuf;
	logKey.mv_size = sizeof(logKeyBuf);

	memcpy(logKeyBuf,                 &pWal->index,          sizeof(u64));
	memcpy(logKeyBuf + sizeof(u64),   &pWal->lastCompleteTerm, sizeof(u64));
	memcpy(logKeyBuf + sizeof(u64)*2, &pWal->lastCompleteEvnum,sizeof(u64));

	if ((rc = mdb_cursor_get(thr->cursorLog,&logKey,&logVal,MDB_SET)) != MDB_SUCCESS && limitEvnum > 0)
	{
		DBG((g_log,"Key not found in log for rewind %llu %llu\n",pWal->lastCompleteTerm,pWal->lastCompleteEvnum));
		return atom_false;
	}

	while (rc == MDB_SUCCESS && pWal->lastCompleteEvnum >= limitEvnum)
	{
		// size_t ndupl;

		// mdb_cursor_count(thr->cursorLog,&ndupl);
		// For every page here
		// ** - Log DB: {<<ActorIndex:64, Evterm:64, Evnum:64>>, <<Pgno:32/unsigned>>}
		// Delete from
		// ** - Pages DB: {<<ActorIndex:64, Pgno:32/unsigned>>, <<Evterm:64,Evnum:64,Count,CompressedPage/binary>>}
		logop = MDB_LAST_DUP;
		while ((rc = mdb_cursor_get(thr->cursorLog,&logKey,&logVal,logop)) == MDB_SUCCESS)
		{
			u32 pgno;
			u8 pagesKeyBuf[sizeof(u64)+sizeof(u32)];
			MDB_val pgKey, pgVal;

			memcpy(&pgno, logVal.mv_data,sizeof(u32));
			DBG((g_log,"Moving to pgno=%u, evnum=%llu\r\n",pgno,pWal->lastCompleteEvnum));

			memcpy(pagesKeyBuf,               &pWal->index,sizeof(u64));
			memcpy(pagesKeyBuf + sizeof(u64), &pgno,       sizeof(u32));
			pgKey.mv_data = pagesKeyBuf;
			pgKey.mv_size = sizeof(pagesKeyBuf);

			pgop = MDB_LAST_DUP;
			logop = MDB_PREV_DUP;
			if (mdb_cursor_get(thr->cursorPages,&pgKey,&pgVal,MDB_SET) != MDB_SUCCESS)
			{
				continue;
			}
			while ((rc = mdb_cursor_get(thr->cursorPages,&pgKey,&pgVal,pgop)) == MDB_SUCCESS)
			{
				// memcpy(&evterm, pgVal.mv_data,            sizeof(u64));
				memcpy(&evnum,  pgVal.mv_data+sizeof(u64),sizeof(u64));
				DBG((g_log,"Deleting pgno=%u, evnum=%llu\r\n",pgno,evnum));
				if (evnum >= limitEvnum)
				{
					mdb_cursor_del(thr->cursorPages,0);
					pWal->allPages--;
				}
				else
					break;

				pgop = MDB_PREV_DUP;
			}
			DBG((g_log,"Done looping pages %d\n",rc));
			// if reached notfound, this means we deleted all versions of page.
			// If this is last page, we have shrunk DB.
			if (rc == MDB_NOTFOUND && pgno == pWal->mxPage)
				pWal->mxPage--;
		}
		if (mdb_cursor_del(thr->cursorLog,MDB_NODUPDATA) != MDB_SUCCESS)
		{
			DBG((g_log,"Rewind Unable to cleanup key from logdb\n"));
		}
		if (mdb_cursor_get(thr->cursorLog,&logKey,&logVal,MDB_PREV_NODUP) != MDB_SUCCESS)
		{
			DBG((g_log,"Rewind Unable to move to next log\n"));
			break;
		}
		memcpy(&aindex, logKey.mv_data,                 sizeof(u64));
		memcpy(&evterm, logKey.mv_data + sizeof(u64),   sizeof(u64));
		memcpy(&evnum,  logKey.mv_data + sizeof(u64)*2, sizeof(u64));

		if (aindex != pWal->index)
		{
			DBG((g_log,"Rewind Reached another actor=%llu, me=%llu\n",aindex,pWal->index));
			break;
		}
		pWal->lastCompleteTerm = evterm;
		pWal->lastCompleteEvnum = evnum;
		rc = MDB_SUCCESS;
	}
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
		while (mdb_cursor_get(thr->cursorPages,&pgKey,&pgVal,pgop) == MDB_SUCCESS)
		{
			u64 aindex;
			memcpy(&aindex,pgKey.mv_data,sizeof(u64));
			if (aindex != pWal->index)
				break;
			mdb_cursor_del(thr->cursorPages, MDB_NODUPDATA);
			pgop = MDB_NEXT_NODUP;
		}
		pWal->mxPage = 0;
		pWal->allPages = 0;
	}
	DBG((g_log,"evterm = %llu, evnum=%llu\r\n",pWal->lastCompleteTerm, pWal->lastCompleteEvnum));
	// no dirty pages, but will write info
	sqlite3WalFrames(pWal, SQLITE_DEFAULT_PAGE_SIZE, NULL, pWal->mxPage, 1, 0);
	pWal->changed = 1;

	enif_mutex_lock(pWal->mtx);
	pWal->readSafeTerm = pWal->lastCompleteTerm;
	pWal->readSafeEvnum = pWal->lastCompleteEvnum;
	pWal->readSafeMxPage = pWal->readSafeEvnum;
	enif_mutex_unlock(pWal->mtx);
	// return enif_make_tuple2(cmd->env, atom_ok, enif_make_int(cmd->env,rc));
	return atom_ok;
}

static ERL_NIF_TERM do_term_store(db_command *cmd, db_thread *thread)
{
	ErlNifBinary votedFor;
	u64 currentTerm;

	if (!enif_get_uint64(cmd->env,cmd->arg1,(ErlNifUInt64*)&currentTerm))
		return atom_false;
	if (!enif_inspect_binary(cmd->env,cmd->arg2,&votedFor))
		return atom_false;

	if (votedFor.size > 128)
		return atom_false;

	if (cmd->conn)
	{
		storeinfo(&cmd->conn->wal, currentTerm, (u8)votedFor.size, votedFor.data);
	}
	else
	{
		ErlNifBinary name;
		db_connection con;
		char pth[MAX_PATHNAME];

		if (!enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &name))
			return atom_false;
		if (name.size >= MAX_PATHNAME-5)
			return atom_false;

		memset(&con, 0, sizeof(db_connection));
		memset(pth, 0, MAX_PATHNAME);
		memcpy(pth, name.data, name.size);
		strcat(pth,"-wal");
		thread->curConn = &con;
		sqlite3WalOpen(NULL, NULL, pth, 0, 0, NULL, thread);
		storeinfo(&con.wal, currentTerm, (u8)votedFor.size, votedFor.data);
		thread->curConn = NULL;
	}
	return atom_ok;
}

static ERL_NIF_TERM do_actor_info(db_command *cmd, db_thread *thr)
{
	MDB_val key, data;
	ErlNifBinary bin;
	// MDB_txn *txn;
	int rc;

	if (!enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &bin))
		return make_error_tuple(cmd->env, "not_iolist");
	//
	// if (mdb_txn_begin(thr->env, NULL, MDB_RDONLY, &txn) != MDB_SUCCESS)
	// 	return SQLITE_ERROR;

	key.mv_size = bin.size;
	key.mv_data = bin.data;
	rc = mdb_get(thr->txn,thr->actorsdb,&key,&data);

	if (rc == MDB_NOTFOUND)
		return atom_false;
	else if (rc == MDB_SUCCESS)
	{
		key = data;
		rc = mdb_get(thr->txn,thr->infodb,&key,&data);

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

			// DBG((g_log,"Size=%zu, should=%lu\n",data.mv_size, sizeof(u64)*7+2+sizeof(u32)));
			if (data.mv_size < sizeof(u64)*7+2+sizeof(u32))
				return atom_error;

			memcpy(&firstCompleteTerm,  data.mv_data+1,               sizeof(u64));
			memcpy(&firstCompleteEvnum, data.mv_data+1+sizeof(u64),   sizeof(u64));
			memcpy(&lastCompleteTerm,   data.mv_data+1+sizeof(u64)*2, sizeof(u64));
			memcpy(&lastCompleteEvnum,  data.mv_data+1+sizeof(u64)*3, sizeof(u64));
			memcpy(&inProgressTerm,     data.mv_data+1+sizeof(u64)*4, sizeof(u64));
			memcpy(&inProgressEvnum,    data.mv_data+1+sizeof(u64)*5, sizeof(u64));
			memcpy(&mxPage,             data.mv_data+1+sizeof(u64)*6, sizeof(u32));
			memcpy(&allPages,           data.mv_data+1+sizeof(u64)*6+sizeof(u32), sizeof(u32));
			memcpy(&currentTerm, data.mv_data+1+sizeof(u64)*6+sizeof(u32)*2, sizeof(u64));
			vfSize = ((u8*)data.mv_data)[1+sizeof(u64)*7+sizeof(u32)*2];
			votedFor = (u8*)data.mv_data+2+sizeof(u64)*7+sizeof(u32)*2;

			enif_alloc_binary(vfSize, &vfbin);
			memcpy(vfbin.data, votedFor, vfSize);
			vft = enif_make_binary(cmd->env,&vfbin);
			enif_release_binary(&vfbin);

			fct = enif_make_uint64(cmd->env, firstCompleteTerm);
			fce = enif_make_uint64(cmd->env, firstCompleteEvnum);
			lct = enif_make_uint64(cmd->env, lastCompleteTerm);
			lce = enif_make_uint64(cmd->env, lastCompleteEvnum);
			ipt = enif_make_uint64(cmd->env, inProgressTerm);
			ipe = enif_make_uint64(cmd->env, inProgressEvnum);
			ct  = enif_make_uint64(cmd->env, currentTerm);

			fc = enif_make_tuple2(cmd->env, fct, fce);
			lc = enif_make_tuple2(cmd->env, lct, lce);
			in = enif_make_tuple2(cmd->env, ipt, ipe);
			allt = enif_make_uint(cmd->env, allPages);
			mxt = enif_make_uint(cmd->env, mxPage);

			res = enif_make_tuple7(cmd->env,fc,lc,in,mxt,allt,ct,vft);
			return res;
		}
		else
			return atom_false;
	}
	else
		return atom_error;
}

static ERL_NIF_TERM do_sync(db_command *cmd, db_thread *thread)
{
	priv_data *pd = thread->pd;

	enif_mutex_lock(pd->thrMutexes[thread->index]);
	if (!cmd->conn || cmd->conn->syncNum >= pd->syncNumbers[thread->index])
	{
		pd->syncNumbers[thread->index]++;
		mdb_txn_commit(thread->txn);
		thread->txn = NULL;
		mdb_env_sync(thread->env,1);
	}
	enif_mutex_unlock(pd->thrMutexes[thread->index]);

	return atom_ok;
}

static ERL_NIF_TERM do_inject_page(db_command *cmd, db_thread *thread)
{
	ErlNifBinary bin;
	u32 commit;
	u64 evterm,evnum;
	int rc;
	u8 pbuf[SQLITE_DEFAULT_PAGE_SIZE];
	PgHdr page;
	int doreplicate = cmd->conn->doReplicate;;
	ErlNifBinary header;
	Wal *pWal = &cmd->conn->wal;

	if (!enif_is_binary(cmd->env,cmd->arg))
		return make_error_tuple(cmd->env,"not_bin");

	if (!enif_is_binary(cmd->env,cmd->arg1))
		return make_error_tuple(cmd->env,"hdr_not_bin");

	memset(&page,0,sizeof(page));
	enif_inspect_binary(cmd->env,cmd->arg,&bin);
	page.pData = pbuf;
	enif_inspect_binary(cmd->env,cmd->arg1,&header);

	if (header.size != sizeof(u64)*2+sizeof(u32)*2)
		return make_error_tuple(cmd->env,"bad_hdr_size");

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
			DBG((g_log,"Unable to decompress inject page!! %ld\r\n",bin.size));
			return make_error_tuple(cmd->env,"cant_decompress");
		}
	}
	cmd->conn->doReplicate = 0;
	rc = sqlite3WalFrames(pWal, sizeof(pbuf), &page, commit, commit, 0);
	cmd->conn->doReplicate = doreplicate;
	if (rc != SQLITE_OK)
	{
		DBG((g_log,"Unable to write inject page\r\n"));
		return make_error_tuple(cmd->env,"cant_inject");
	}
	enif_mutex_lock(pWal->mtx);
	pWal->readSafeTerm = cmd->conn->wal.lastCompleteTerm;
	pWal->readSafeEvnum = cmd->conn->wal.lastCompleteEvnum;
	pWal->readSafeMxPage = cmd->conn->wal.mxPage;
	enif_mutex_unlock(pWal->mtx);
	pWal->changed = 1;
	return atom_ok;
}

static ERL_NIF_TERM do_checkpoint(db_command *cmd, db_thread *thread)
{
	ErlNifUInt64 evnum;
	db_connection *con = cmd->conn;

	enif_get_uint64(cmd->env,cmd->arg,(ErlNifUInt64*)&(evnum));

	if (con->wal.firstCompleteEvnum >= evnum || con->checkpointLock)
		return atom_ok;

	checkpoint(&con->wal, evnum);

	return atom_ok;
}


static ERL_NIF_TERM do_exec_script(db_command *cmd, db_thread *thread)
{
	u32 pagesPre = thread->pagesChanged;
	int rc = 0,i;
	u64 newTerm,newEvnum;
	ERL_NIF_TERM *stackArray = NULL;
	sqlite3_stmt *statement = NULL;
	char *errat = NULL;
	ERL_NIF_TERM results;
	const ERL_NIF_TERM *inputTuple = NULL;
	const ERL_NIF_TERM *tupleRecs = NULL;
	ERL_NIF_TERM *tupleResult = NULL;
	int tupleSize = 0, tuplePos = 0, tupleRecsSize = 0;

	if (!cmd->conn->wal_configured)
		cmd->conn->wal_configured = SQLITE_OK == sqlite3_wal_data(cmd->conn->db,(void*)thread);

	if (cmd->arg1)
	{
		enif_get_uint64(cmd->env,cmd->arg1,(ErlNifUInt64*)&(newTerm));
		enif_get_uint64(cmd->env,cmd->arg2,(ErlNifUInt64*)&(newEvnum));
		enif_inspect_binary(cmd->env,cmd->arg3,&(cmd->conn->packetVarPrefix));
	}

	if ((cmd->conn->wal.inProgressTerm > 0 || cmd->conn->wal.inProgressEvnum > 0) && cmd->arg1)
	{
		DBG((g_log,"undo before exec, %llu, %llu\n",cmd->conn->wal.inProgressTerm,cmd->conn->wal.inProgressEvnum));
		doundo(&cmd->conn->wal, NULL, NULL, 1);
		// mdb_txn_abort(thread->wtxn);
		// open_wtxn(thread);
	}
	if (cmd->arg1)
	{
		cmd->conn->wal.inProgressTerm = newTerm;
		cmd->conn->wal.inProgressEvnum = newEvnum;
	}
	else
	{
		// Copy over safe read limits.
		enif_mutex_lock(cmd->conn->wal.mtx);
		thread->readSafeTerm = cmd->conn->wal.readSafeTerm;
		thread->readSafeEvnum = cmd->conn->wal.readSafeEvnum;
		enif_mutex_unlock(cmd->conn->wal.mtx);
	}

	if (enif_get_tuple(cmd->env, cmd->arg, &tupleSize, &inputTuple))
	{
		if (cmd->arg4 != 0 && !enif_get_tuple(cmd->env, cmd->arg4, &tupleRecsSize, &tupleRecs))
			return atom_false;
		if (cmd->arg4 != 0 && tupleRecsSize != tupleSize)
			return atom_false;

		if (tupleSize > 200)
			tupleResult = malloc(sizeof(ERL_NIF_TERM));
		else
			tupleResult = alloca(sizeof(ERL_NIF_TERM));
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
		int skip = 0;
		int statementlen = 0;
		ERL_NIF_TERM rows;
		char dofinalize = 1;
		const ERL_NIF_TERM *insertRow;
		int rowLen = 0;

		if (inputTuple)
		{
			if (tupleRecs)
				listTop = tupleRecs[tuplePos];
			else
				listTop = 0;
			if (!enif_inspect_iolist_as_binary(cmd->env, inputTuple[tuplePos], &bin))
				return make_error_tuple(cmd->env, "not_iolist");
		}
		else
		{
			listTop = cmd->arg4;
			if (!enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &bin))
				return make_error_tuple(cmd->env, "not_iolist");
		}

	#ifdef _TESTDBG_
		if (bin.size > 1024*10)
		{
			DBG((g_log,"Executing %.*s...\n", 1024*10,bin.data));
		}
		else
		{
			DBG((g_log,"Executing %.*s\n",(int)bin.size,bin.data));
		}
	#endif
		end = (char*)bin.data + bin.size;
		readpoint = (char*)bin.data;
		results = enif_make_list(cmd->env,0);

		while (readpoint < end || headTop != 0)
		{
			if (readpoint[0] == '$')
				skip = 1;
			else
				skip = 0;
			statementlen = end-readpoint;

			// if _insert, then this is a prepared statement with multiple rows in arg4
			if (headTop == 0 && statementlen >= 8 && cmd->arg4 && readpoint[skip] == '_' &&
				(readpoint[skip+1] == 'i' || readpoint[skip+1] == 'I') &&
				(readpoint[skip+2] == 'n' || readpoint[skip+2] == 'N'))
			{
				skip++;
				rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(readpoint+skip), statementlen, &(statement), &readpoint);
				if(rc != SQLITE_OK)
				{
					errat = "_prepare";
					sqlite3_finalize(statement);
					break;
				}
				rc = SQLITE_DONE;

				if (!enif_get_list_cell(cmd->env, listTop, &headTop, &listTop))
				{
					rc = SQLITE_INTERRUPT;
					sqlite3_finalize(statement);
					break;
				}

				// Move over a list of records.
				// First element is ignored as it is presumed to be record name.
				while (rc == SQLITE_DONE && enif_get_list_cell(cmd->env, headTop, &headBot, &headTop))
				{
					if (!enif_get_tuple(cmd->env, headBot, &rowLen, &insertRow) && rowLen > 1 && rowLen < 100)
					{
						rc = SQLITE_INTERRUPT;
						break;
					}

					for (i = 1; i < rowLen; i++)
					{
						if (bind_cell(cmd->env, insertRow[i], statement, i) == -1)
						{
							errat = "cant_bind";
							sqlite3_finalize(statement);
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
						rc = sqlite3_prepare_v2(cmd->conn->db, (char *)thread->staticSqls[i], -1, &(cmd->conn->staticPrepared[i]), NULL);
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

					if (thread->prepSize <= i)
					{
						errat = "prepare";
						break;
					}

					if (cmd->conn->prepared == NULL)
					{
						cmd->conn->prepared = malloc(MAX_PREP_SQLS*sizeof(sqlite3_stmt*));
						memset(cmd->conn->prepared,0,MAX_PREP_SQLS*sizeof(sqlite3_stmt*));
						cmd->conn->prepVersions = malloc(MAX_PREP_SQLS*sizeof(int));
						memset(cmd->conn->prepVersions,0,MAX_PREP_SQLS*sizeof(int));
					}

					if (cmd->conn->prepared[rowLen] == NULL || cmd->conn->prepVersions[rowLen] != thread->prepVersions[i][rowLen])
					{
						if (thread->prepSqls[i][rowLen] == NULL)
						{
							errat = "prepare";
							break;
						}
						if (cmd->conn->prepared[rowLen] != NULL)
							sqlite3_finalize(cmd->conn->prepared[rowLen]);

						rc = sqlite3_prepare_v2(cmd->conn->db, thread->prepSqls[i][rowLen], -1, &(cmd->conn->prepared[rowLen]), NULL);
						if(rc != SQLITE_OK)
						{
							DBG((g_log,"Prepared statement failed\n"));
							errat = "prepare";
							break;
						}
						cmd->conn->prepVersions[rowLen] = thread->prepVersions[i][rowLen];
					}
					readpoint += 7;
					statement = cmd->conn->prepared[rowLen];
				}
				else if (headTop == 0)
				{
					dofinalize = 1;
					// #ifdef _TESTDBG_
					//     if (statementlen > 1024)
					//     {
					//         DBG((g_log,"Executing %.*s\n",1024,readpoint));
					//     }
					//     else
					//     {
					//         DBG((g_log,"Executing %.*s\n",(int)statementlen,readpoint));
					//     }
					// #endif
					rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(readpoint+skip), statementlen, &statement, &readpoint);
					if(rc != SQLITE_OK)
					{
						DBG((g_log,"Prepare statement failed\n"));
						errat = "prepare";
						sqlite3_finalize(statement);
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
						// [[[Column1,Column2,..],..],...] -> for every statement, for every row, list of columns.
						if (!enif_get_list_cell(cmd->env, listTop, &headTop, &listTop))
						{
							rc = SQLITE_INTERRUPT;
							break;
						}
					}

					// Every row is a list. It can be a list of tuples (which implies records thus we start at offset 1),
					//   or a list of columns.
					if (enif_get_list_cell(cmd->env, headTop, &headBot, &headTop))
					{
						// If tuple bind from tuple
						if (enif_is_tuple(cmd->env, headBot))
						{
							if (!enif_get_tuple(cmd->env, headBot, &rowLen, &insertRow) && rowLen > 1 && rowLen < 100)
							{
								rc = SQLITE_INTERRUPT;
								break;
							}

							for (i = 1; i < rowLen; i++)
							{
								if (bind_cell(cmd->env, insertRow[i], statement, i) == -1)
								{
									errat = "cant_bind";
									rc = SQLITE_INTERRUPT;
									break;
								}
							}
						}
						// If list, bind from list
						else if (enif_is_list(cmd->env,headBot))
						{
							ERL_NIF_TERM rowHead;
							// Index is from 1 because sqlite bind param start with 1 not 0
							for (i = 1; enif_get_list_cell(cmd->env, headBot, &rowHead, &headBot); i++)
							{
								if (bind_cell(cmd->env, rowHead, statement, i) == -1)
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
							thread->columnSpace = realloc(thread->columnSpace, column_count*sizeof(ERL_NIF_TERM));
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
						array[i] = make_binary(cmd->env, cname,strlen(cname));
					}

					column_names = enif_make_tuple_from_array(cmd->env, array, column_count);
				}

				rows = enif_make_list(cmd->env,0);
				rowcount = 0;
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
						array[i] = make_cell(cmd->env, statement, i);

					rows = enif_make_list_cell(cmd->env, enif_make_tuple_from_array(cmd->env, array, column_count), rows);
					rowcount++;
				}
			}
			DBG((g_log,"exec rc=%d, rowcount=%d, column_count=%d\n", rc, rowcount, column_count));
			if (rc > 0 && rc < 100)
			{
				errat = "step";
				dofinalize ? sqlite3_finalize(statement) : sqlite3_reset(statement);
				break;
			}
			if (skip == 0 && (rowcount > 0 || column_count > 0))
			{
				ERL_NIF_TERM cols = enif_make_tuple2(cmd->env,atom_columns,column_names);
				ERL_NIF_TERM rowst = enif_make_tuple2(cmd->env,atom_rows,rows);
				ERL_NIF_TERM res1 = enif_make_list2(cmd->env,cols,rowst);
				results = enif_make_list_cell(cmd->env, res1,results);
			}
			else if (skip == 0)
			{
				ERL_NIF_TERM nchanges = enif_make_int(cmd->env,sqlite3_changes(cmd->conn->db));
				ERL_NIF_TERM lirowid = enif_make_int64(cmd->env,sqlite3_last_insert_rowid(cmd->conn->db));
				ERL_NIF_TERM changes = enif_make_tuple3(cmd->env,atom_changes,lirowid,nchanges);
				results = enif_make_list_cell(cmd->env, changes, results);
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
		if (tupleResult)
		{
			tupleResult[tuplePos] = results;
		}
		tuplePos++;
	} while (tuplePos < tupleSize);

	if (tupleResult)
	{
		results = enif_make_tuple_from_array(cmd->env, tupleResult, tupleSize);
		if (tupleSize > 200)
			free(tupleResult);
	}

	if (rc > 0 && rc < 100 && pagesPre != thread->pagesChanged)
	{
		sqlite3_prepare_v2(cmd->conn->db, "ROLLBACK;", strlen("ROLLBACK;"), &statement, NULL);
		sqlite3_step(statement);
		sqlite3_finalize(statement);
	}

	// enif_release_resource(cmd->conn);
	// Errors are from 1 to 99.
	if (rc > 0 && rc < 100 && rc != SQLITE_INTERRUPT)
		return make_sqlite3_error_tuple(cmd->env, errat, rc, cmd->conn->db);
	else if (rc == SQLITE_INTERRUPT)
	{
		return make_error_tuple(cmd->env, "query_aborted");
	}
	else
	{
		if (pagesPre != thread->pagesChanged)
		{
			priv_data *pd = thread->pd;
			cmd->conn->syncNum = pd->syncNumbers[thread->index];
		}
		return make_ok_tuple(cmd->env,results);
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
		{
		   return sqlite3_bind_null(stmt, i);
		}

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
		return enif_make_tuple2(env, make_atom(env, "blob"),
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

static ERL_NIF_TERM do_checkpoint_lock(db_command *cmd,db_thread *thread)
{
	int lock;
	enif_get_int(cmd->env,cmd->arg,&lock);

	DBG((g_log,"Checkpoint lock now %d, lockin=%d.\n",cmd->conn->checkpointLock,lock));

	if (lock > 0)
		cmd->conn->checkpointLock++;
	else if (cmd->conn->checkpointLock > 0)
		cmd->conn->checkpointLock--;
	return atom_ok;
}

static ERL_NIF_TERM evaluate_command(db_command *cmd,db_thread *thread)
{
	thread->curConn = cmd->conn;

	switch(cmd->type)
	{
	case cmd_open:
	{
		ERL_NIF_TERM connres = do_open(cmd,thread);
		if (cmd->conn != NULL && cmd->arg2 == 0)
			return enif_make_tuple2(cmd->env,atom_ok,connres);
		else if (cmd->conn == NULL)
			return connres;
		else if (cmd->arg2 != 0)
		{
			cmd->arg = cmd->arg2;
			cmd->arg1 = 0;
			cmd->arg2 = 0;
			return enif_make_tuple3(cmd->env,atom_ok,connres,do_exec_script(cmd,thread));
		}
	}
	case cmd_exec_script:
		return do_exec_script(cmd,thread);
	case cmd_store_prepared:
		return do_store_prepared_table(cmd,thread);
	case cmd_checkpoint:
		return do_checkpoint(cmd,thread);
	case cmd_sync:
		return do_sync(cmd,thread);
	case cmd_inject_page:
		return do_inject_page(cmd,thread);
	case cmd_actor_info:
		return do_actor_info(cmd,thread);
	case cmd_wal_rewind:
		return do_wal_rewind(cmd,thread);
	case cmd_interrupt:
		return do_interrupt(cmd,thread);
	case cmd_iterate:
		return do_iterate(cmd,thread);
	case cmd_term_store:
		return do_term_store(cmd,thread);
	case cmd_unknown:
		return atom_ok;
	case cmd_tcp_connect:
		return do_tcp_connect(cmd,thread);
	case cmd_tcp_reconnect:
		return do_tcp_reconnect(cmd,thread);
	case cmd_alltunnel_call:
		return do_all_tunnel_call(cmd,thread);
	case cmd_checkpoint_lock:
		return do_checkpoint_lock(cmd,thread);
	case cmd_set_socket:
	{
		int fd = 0;
		int pos = -1;
		int type = 1;
		if (!enif_get_int(cmd->env,cmd->arg,&fd))
			return atom_error;
		if (!enif_get_int(cmd->env,cmd->arg1,&pos))
			return atom_error;
		if (!enif_get_int(cmd->env,cmd->arg2,&type))
			return atom_error;

		if (fd > 3 && pos >= 0 && pos < 8)
		{
			if (thread->sockets[pos] > 3)
			{
				char zero[4];
				memset(zero,0,4);

				// check if connection open, if it is do not use new socket
#ifdef MSG_NOSIGNAL
				if (send(thread->sockets[pos], zero, sizeof(zero), MSG_NOSIGNAL) == -1)
#else
				if (write(thread->sockets[pos],zero,4) == -1)
#endif
				{
					close(thread->sockets[pos]);
					thread->sockets[pos] = fd;
				}
				else
				{
					close(fd);
				}
			}
			else
			{
				thread->sockets[pos] = fd;
				thread->socket_types[pos] = type;
			}
			return atom_ok;
		}
		return atom_error;
	}
	default:
		return make_error_tuple(cmd->env, "invalid_command");
	}
}

static ERL_NIF_TERM push_command(int threadnum, int readThreadNum, priv_data *pd, qitem *item)
{
	queue *thrCmds = NULL;
	if (threadnum == -1)
		thrCmds = pd->wtasks[pd->nthreads];
	else if (readThreadNum == -1)
		thrCmds = pd->wtasks[threadnum];
	else
		thrCmds = pd->rtasks[threadnum * pd->nReadThreads + readThreadNum];

	if(!queue_push(thrCmds, item))
	{
		return make_error_tuple(item->cmd.env, "command_push_failed");
	}

	return atom_ok;
}

static ERL_NIF_TERM make_answer(db_command *cmd, ERL_NIF_TERM answer)
{
	return enif_make_tuple2(cmd->env, cmd->ref, answer);
}

static int logdb_cmp(const MDB_val *a, const MDB_val *b)
{
	// <<ActorIndex:64, Evterm:64, Evnum:64>>
	i64 aActor,aEvterm,aEvnum,bActor,bEvterm,bEvnum;
	int diff;

	// aActor = *(i64*)a->mv_data;
	memcpy(&aActor,a->mv_data,sizeof(i64));
	// bActor = *(i64*)b->mv_data;
	memcpy(&bActor,b->mv_data,sizeof(i64));
	diff = aActor - bActor;
	if (diff == 0)
	{
		// aEvterm = *(i64*)(a->mv_data+sizeof(i64));
		memcpy(&aEvterm, a->mv_data+sizeof(i64), sizeof(i64));
		// bEvterm = *(i64*)(b->mv_data+sizeof(i64));
		memcpy(&bEvterm, b->mv_data+sizeof(i64), sizeof(i64));
		diff = aEvterm - bEvterm;
		if (diff == 0)
		{
			// aEvnum  = *(i64*)(a->mv_data+sizeof(i64)*2);
			memcpy(&aEvnum, a->mv_data+sizeof(i64)*2, sizeof(i64));
			// bEvnum  = *(i64*)(a->mv_data+sizeof(i64)*2);
			memcpy(&bEvnum, b->mv_data+sizeof(i64)*2, sizeof(i64));
			return aEvnum - bEvnum;
		}
		return diff;
	}
	return diff;
}

static int pagesdb_cmp(const MDB_val *a, const MDB_val *b)
{
	// <<ActorIndex:64, Pgno:32/unsigned>>
	i64 aActor;
	i64 bActor;
	u32 aPgno;
	u32 bPgno;
	int diff;

	// aActor = *(i64*)a->mv_data;
	memcpy(&aActor,a->mv_data,sizeof(i64));
	// bActor = *(i64*)b->mv_data;
	memcpy(&bActor,b->mv_data,sizeof(i64));
	diff = aActor - bActor;
	if (diff == 0)
	{
		// aPgno = *(u32*)(a->mv_data+sizeof(i64));
		memcpy(&aPgno,a->mv_data + sizeof(i64),sizeof(u32));
		// bPgno = *(u32*)(b->mv_data+sizeof(i64));
		memcpy(&bPgno,b->mv_data + sizeof(i64),sizeof(u32));
		return aPgno - bPgno;
	}
	return diff;
}

// static int logdb_val_cmp(const MDB_val *a, const MDB_val *b)
// {
// 	u32 aPgno;
// 	u32 bPgno;
// 	memcpy(&aPgno,a->mv_data,sizeof(u32));
// 	memcpy(&bPgno,b->mv_data,sizeof(u32));
// 	return aPgno - bPgno;
// }

static int pagesdb_val_cmp(const MDB_val *a, const MDB_val *b)
{
	// <<Evterm:64,Evnum:64,Counter:8,CompressedPage/binary>>}
	i64 aEvterm,aEvnum;
	i64 bEvterm,bEvnum;
	u8 aCounter, bCounter;
	int diff;

	// aEvterm = *(i64*)a->mv_data;
	memcpy(&aEvterm, a->mv_data, sizeof(i64));
	// bEvterm = *(i64*)b->mv_data;
	memcpy(&bEvterm, b->mv_data, sizeof(i64));
	diff = aEvterm - bEvterm;
	if (diff == 0)
	{
		// aEvnum = *(i64*)(a->mv_data+sizeof(i64));
		memcpy(&aEvnum, a->mv_data+sizeof(i64), sizeof(i64));
		// bEvnum = *(i64*)(b->mv_data+sizeof(i64));
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

static MDB_txn* open_wtxn(db_thread *data)
{
	if (mdb_txn_begin(data->env, NULL, 0, &data->txn) != MDB_SUCCESS)
		return NULL;
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

static MDB_txn* open_rtxn(db_thread *data)
{
	if (mdb_txn_begin(data->env, NULL, MDB_RDONLY, &data->txn) != MDB_SUCCESS)
		return NULL;
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

static void thread_ex(db_thread *data, qitem *item)
{
	DBG((g_log,"thread=%d command=%d.\n",data->index,item->cmd.type));

	if (item->cmd.ref == 0)
	{
		evaluate_command(&item->cmd,data);
		enif_clear_env(item->cmd.env);
	}
	else
	{
		ERL_NIF_TERM answer = make_answer(&item->cmd, evaluate_command(&item->cmd,data));
		DBG((g_log,"thread=%d command done 1. pagesChanged=%d\n",data->index,data->pagesChanged));
		// if (data->pagesChanged != pagesChanged)
		if (data->forceCommit)
		{
			data->forceCommit = 0;
			mdb_txn_commit(data->txn);
			data->txn = NULL;
		}
		enif_send(NULL, &item->cmd.pid, item->cmd.env, answer);
		enif_clear_env(item->cmd.env);
	}

	if (item->cmd.conn != NULL)
	{
		enif_release_resource(item->cmd.conn);
	}

	DBG((g_log,"thread=%d command done 2.\n",data->index));
}

static void *thread_func(void *arg)
{
	int i,j,chkCounter = 0, syncListSize = 0;
	db_thread* data = (db_thread*)arg;
	qitem *syncList = NULL;

	data->isopen = 1;

	if (data->env)
	{
		data->maxvalsize = mdb_env_get_maxkeysize(data->env);
		data->resFrames = alloca((SQLITE_DEFAULT_PAGE_SIZE/data->maxvalsize + 1)*sizeof(MDB_val));
	}

	while(1)
	{
		qitem *item = queue_pop(data->tasks);

		if (item->cmd.type == cmd_stop)
		{
			queue_recycle(data->tasks,item);
			if (data->txn)
			{
				mdb_txn_commit(data->txn);
				mdb_env_sync(data->env,1);
			}
			break;
		}
		if (item->cmd.type == cmd_sync)
		{
			if (syncList == NULL)
				chkCounter = 0;

			// Sync only when nothing else to do.
			// So if sync item, check if there is anything else we could do first and put sync task in a list.
			if (queue_size(data->tasks) != 0)
			{
				item->next = syncList;
				syncList = item;
				syncListSize++;
				continue;
			}
		}
		chkCounter++;

		if (data->env && data->txn == NULL)
		{
			if (open_wtxn(data) == NULL)
				break;
		}

		// printf("Queue size %d\r\n",queue_size(data->tasks));
		thread_ex(data, item);

		// Execute list of syncs if:
		// - we just did a sync
		// - more than 100 requests went by since we started list
		// - sync list size over 10 items
		if (syncList != NULL &&
			(queue_size(data->tasks) == 0 || chkCounter > 100 || syncListSize > 10 || item->cmd.type == cmd_sync))
		{
			if (data->txn == NULL)
				open_wtxn(data);

			while (syncList != NULL)
			{
				thread_ex(data, syncList);
				if (data->txn != NULL)
					open_wtxn(data);

				queue_recycle(data->tasks,item);
				syncList = syncList->next;
			}
			chkCounter = 0;
			syncListSize = 0;
		}
		queue_recycle(data->tasks,item);
	}
	queue_destroy(data->tasks);
	data->isopen = 0;

	DBG((g_log,"thread=%d stopping.\n",data->index));

	if (data->control)
	{
		enif_free(data->control);
		data->control = NULL;
	}

	if (data->columnSpace)
		free(data->columnSpace);

	for (i = 0; i < MAX_PREP_SQLS; i++)
	{
		for (j = 0; j < MAX_PREP_SQLS; j++)
		{
			free(data->prepSqls[i][j]);
			data->prepSqls[i][j] = NULL;
		}
	}
	return NULL;
}

static void *read_thread_func(void *arg)
{
	db_thread* data = (db_thread*)arg;
	int i,j,rc;
	data->isopen = 1;

	data->maxvalsize = mdb_env_get_maxkeysize(data->env);
	data->resFrames = alloca((SQLITE_DEFAULT_PAGE_SIZE/data->maxvalsize + 1)*sizeof(MDB_val));

	while (1)
	{
		qitem *item = queue_pop(data->tasks);
		DBG((g_log,"rthread=%d command=%d.\n",data->index,item->cmd.type));

		if (item->cmd.type == cmd_stop)
		{
			queue_recycle(data->tasks,item);
			mdb_txn_abort(data->txn);
			break;
		}
		else
		{
			if (item->cmd.conn && item->cmd.conn->wal.rthread == 0)
			{
				item->cmd.conn->wal.rthreadId = pthread_self();
				item->cmd.conn->wal.rthread = data;
			}
			if (!data->txn)
			{
				DBG((g_log,"Open read transaction\n"));
				if (open_rtxn(data) == NULL)
				{
					ERL_NIF_TERM errterm;
					DBG((g_log,"Can not open read transaction\n"));

					errterm = make_error_tuple(item->cmd.env, "lmdb_unreadable_1");
					enif_send(NULL, &item->cmd.pid, item->cmd.env, make_answer(&item->cmd, errterm));
					enif_clear_env(item->cmd.env);
					continue;
				}
			}
			else
			{
				if ((rc = mdb_txn_renew(data->txn)) != MDB_SUCCESS)
					break;
				if ((rc = mdb_cursor_renew(data->txn, data->cursorLog)) != MDB_SUCCESS)
				{
					DBG((g_log,"Unable to renew cursor, reopening read txn\n"));
					mdb_cursor_close(data->cursorLog);
					mdb_cursor_close(data->cursorPages);
					mdb_cursor_close(data->cursorInfo);
					mdb_txn_abort(data->txn);
					if (open_rtxn(data) == NULL)
					{
						ERL_NIF_TERM errterm;
						DBG((g_log,"Unable to open read transaction\n"));
						data = NULL;

						errterm = make_error_tuple(item->cmd.env, "lmdb_unreadable_2");
						enif_send(NULL, &item->cmd.pid, item->cmd.env, make_answer(&item->cmd, errterm));
						enif_clear_env(item->cmd.env);
						continue;
					}
				}
				else
				{
					mdb_cursor_renew(data->txn, data->cursorPages);
					mdb_cursor_renew(data->txn, data->cursorInfo);
				}
			}

			if (item->cmd.ref == 0)
			{
				evaluate_command(&item->cmd,data);
				enif_clear_env(item->cmd.env);
			}
			else
			{
				enif_send(NULL, &item->cmd.pid, item->cmd.env, make_answer(&item->cmd, evaluate_command(&item->cmd,data)));
				enif_clear_env(item->cmd.env);
			}
			if (item->cmd.conn != NULL)
			{
				enif_release_resource(item->cmd.conn);
			}
			mdb_txn_reset(data->txn);

			DBG((g_log,"rthread=%d command done 2.\n",data->index));
			queue_recycle(data->tasks,item);
		}
	}
	DBG((g_log,"rthread=%d stopping.\n",data->index));

	if (data->columnSpace)
		free(data->columnSpace);

	for (i = 0; i < MAX_PREP_SQLS; i++)
	{
		for (j = 0; j < MAX_PREP_SQLS; j++)
		{
			free(data->prepSqls[i][j]);
			data->prepSqls[i][j] = NULL;
		}
	}

	queue_destroy(data->tasks);
	data->isopen = 0;

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

	enif_consume_timeslice(env,500);

	return atom_ok;
}

static ERL_NIF_TERM term_store(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	qitem *item;
	u32 thread;
	db_connection *res = NULL;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"term_store\n"));

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
		thread %= pd->nthreads;
		item = command_create(thread,-1,pd);
	}
	else
	{
		if (!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
			return enif_make_badarg(env);
		thread = res->thread;
		item = command_create(thread,-1,pd);
		item->cmd.conn = res;
		enif_keep_resource(res);
	}
	item->cmd.type = cmd_term_store;
	if (argc == 4)
		item->cmd.arg = enif_make_copy(item->cmd.env,argv[0]); // actor path
	item->cmd.arg1 = enif_make_copy(item->cmd.env,argv[1]); // evterm
	item->cmd.arg2 = enif_make_copy(item->cmd.env,argv[2]); // votedfor

	enif_consume_timeslice(env,500);
	return push_command(thread, -1, pd, item);
}

static ERL_NIF_TERM get_actor_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	qitem *item;
	ErlNifPid pid;
	u32 thread;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"get_actor_info\n"));

	if (argc != 4)
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[0]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[1], &pid))
		return make_error_tuple(env, "invalid_pid");
	if(!enif_get_uint(env, argv[3], &thread))
		return make_error_tuple(env, "invalid_pid");

	thread %= pd->nthreads;
	item = command_create(thread,-1,pd);
	item->cmd.type = cmd_actor_info;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[0]);
	item->cmd.pid = pid;
	item->cmd.arg = enif_make_copy(item->cmd.env,argv[2]);

	enif_consume_timeslice(env,500);
	return push_command(thread, -1, pd, item);
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

	DBG((g_log,"db_open\n"));

	if(!(argc == 5 || argc == 6))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[0]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[1], &pid))
		return make_error_tuple(env, "invalid_pid");
	if(!enif_get_uint(env, argv[3], &thread))
		return make_error_tuple(env, "invalid_pid");

	thread %= pd->nthreads;
	item = command_create(thread,-1,pd);

	item->cmd.type = cmd_open;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[0]);
	item->cmd.pid = pid;
	item->cmd.arg = enif_make_copy(item->cmd.env, argv[2]);
	item->cmd.arg1 = enif_make_copy(item->cmd.env, argv[4]);
	if (argc == 6)
	{
	  item->cmd.arg2 = enif_make_copy(item->cmd.env, argv[5]);
	}

	enif_consume_timeslice(env,500);
	// return enif_make_tuple2(env,push_command(conn->thread, item),db_conn);
	return push_command(thread, -1, pd, item);
}

static ERL_NIF_TERM sync_num(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_connection *res;

	DBG((g_log,"sync_num\n"));

	if (argc != 1)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return make_error_tuple(env, "invalid_connection");

	return enif_make_uint64(env,res->syncNum);
}

static ERL_NIF_TERM replication_done(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	if (argc != 1)
		return atom_false;

	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
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
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"replicate_opts\n"));

	if (!(argc == 3))
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return make_error_tuple(env, "invalid_connection");
	if (!enif_inspect_iolist_as_binary(env, argv[1], &bin))
		return make_error_tuple(env, "not_iolist");

	DBG((g_log,"do_replicate_opts %zu\n", bin.size));
	if (res->packetPrefixSize < bin.size)
	{
		free(res->packetPrefix);
		res->packetPrefixSize = 0;
		res->packetPrefix = NULL;
	}

	if (bin.size > 0)
	{
		if (!enif_get_int(env,argv[2],&(res->doReplicate)))
			return make_error_tuple(env, "repltype_not_int");
		if (!res->packetPrefix)
			res->packetPrefix = malloc(bin.size);
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


// static ERL_NIF_TERM
// replicate_status(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
// {
//     db_connection* conn;

//     if (argc != 1)
//         return enif_make_badarg(env);

//     if(!enif_get_resource(env, argv[0], db_connection_type, (void **) &conn))
//     {
//         return enif_make_badarg(env);
//     }

//     enif_consume_timeslice(env,500);
//     return enif_make_tuple2(env,enif_make_int(env,conn->nSent),enif_make_int(env,conn->failFlags));
// }

// Called with: ref,pid, ip, port, connect string, connection number
static ERL_NIF_TERM tcp_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log, "tcp_connect\n"));

	if (!(argc == 6 || argc == 7))
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[0]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[1], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_is_list(env,argv[2]))
		return enif_make_badarg(env);
	if (!enif_is_number(env,argv[3]))
		return enif_make_badarg(env);
	if (!(enif_is_binary(env,argv[4]) || enif_is_list(env,argv[2])))
		return enif_make_badarg(env);
	if (!enif_is_number(env,argv[5]))
		return enif_make_badarg(env);
	if (argc == 7 && !enif_is_number(env,argv[6]))
		return enif_make_badarg(env);

	item = command_create(-1,-1,pd);

	item->cmd.type = cmd_tcp_connect;
	item->cmd.arg = enif_make_copy(item->cmd.env,argv[2]);
	item->cmd.arg1 = enif_make_copy(item->cmd.env,argv[3]);
	item->cmd.arg2 = enif_make_copy(item->cmd.env,argv[4]);
	item->cmd.arg3 = enif_make_copy(item->cmd.env,argv[5]);
	if (argc == 7)
	  item->cmd.arg4 = enif_make_copy(item->cmd.env,argv[6]);
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[0]);
	item->cmd.pid = pid;

	enif_consume_timeslice(env,500);
	return push_command(-1,-1,pd,item);
}

static ERL_NIF_TERM tcp_reconnect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	priv_data *pd = (priv_data*)enif_priv_data(env);
	qitem *item = command_create(-1,-1,pd);
	item->cmd.type = cmd_tcp_reconnect;

	enif_consume_timeslice(env,500);

	return push_command(-1,-1,pd,item);
}

static ERL_NIF_TERM interrupt_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log, "interrupt\n"));

	if(argc != 1)
		return enif_make_badarg(env);

	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
	{
		return enif_make_badarg(env);
	}
	item = command_create(-1,-1,pd);
	item->cmd.type = cmd_interrupt;
	item->cmd.conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,500);

	return push_command(-1,-1,pd, item);
}

static ERL_NIF_TERM lz4_compress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary binIn;
	ErlNifBinary binOut;
	int size;
	ERL_NIF_TERM termbin;

	// DBG((g_log, "lz4_compress\n"));

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

	// DBG((g_log, "lz4_decompress\n"));

	if (argc != 2 && argc != 3)
		return enif_make_badarg(env);

	if (!enif_inspect_iolist_as_binary(env, argv[0], &binIn))
		return enif_make_badarg(env);

	if (!enif_get_int(env,argv[1],&sizeOriginal))
		return enif_make_badarg(env);

	if (argc == 3)
	{
		if (!enif_get_int(env,argv[2],&sizeReadNum))
			return enif_make_badarg(env);
	}
	else
		sizeReadNum = binIn.size;

	enif_alloc_binary(sizeOriginal,&binOut);
	rt = LZ4_decompress_safe((char*)binIn.data,(char*)binOut.data,sizeReadNum,sizeOriginal);
	enif_consume_timeslice(env,500);
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
	int i;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	int nthreads = pd->nthreads;

	DBG((g_log, "all_tunnel_call\n"));

	if (argc != 3)
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[0]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[1], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!(enif_is_binary(env,argv[2]) || enif_is_list(env,argv[2])))
		return make_error_tuple(env, "invalid bin");

	for (i = 0; i < nthreads; i++)
	{
		item = command_create(i,-1,pd);
		item->cmd.type = cmd_alltunnel_call;
		item->cmd.ref = enif_make_copy(item->cmd.env, argv[0]);
		item->cmd.pid = pid;
		item->cmd.arg = enif_make_copy(item->cmd.env, argv[2]);

		enif_consume_timeslice(env,500);

		push_command(i, -1, pd, item);
	}
	return atom_ok;
}


static ERL_NIF_TERM store_prepared_table(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	int i;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	int nthreads = pd->nthreads;

	DBG((g_log,"store_prepared_table\n"));

	if (argc != 2)
		return enif_make_badarg(env);

	if (!(enif_is_tuple(env,argv[0]) && enif_is_tuple(env,argv[1])))
		return enif_make_badarg(env);

	for (i = 0; i < nthreads; i++)
	{
		item = command_create(i,-1,pd);

		/* command */
		item->cmd.type = cmd_store_prepared;
		item->cmd.arg = enif_make_copy(item->cmd.env, argv[0]);
		item->cmd.arg1 = enif_make_copy(item->cmd.env, argv[1]);

		enif_consume_timeslice(env,500);
		push_command(i, -1, pd, item);
	}
	return atom_ok;
}

static ERL_NIF_TERM db_checkpoint(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"Checkpoint\r\n"));

	if (argc != 4)
		return enif_make_badarg(env);

	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_is_number(env,argv[3]))
		return make_error_tuple(env, "evnum NaN");

	item = command_create(res->thread,-1,pd);

	item->cmd.type = cmd_checkpoint;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
	item->cmd.pid = pid;
	item->cmd.arg = enif_make_copy(item->cmd.env, argv[3]);  // evnum
	item->cmd.conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,500);
	return push_command(res->thread, -1, pd, item);
}

static ERL_NIF_TERM exec_read(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"exec_read %d\n",argc));

	if(argc != 4 && argc != 5)
		return enif_make_badarg(env);

	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!(enif_is_binary(env,argv[3]) || enif_is_list(env,argv[3]) || enif_is_tuple(env,argv[3])))
		return make_error_tuple(env,"sql");

	item = command_create(res->thread,-1,pd);

	item->cmd.type = cmd_exec_script;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
	item->cmd.pid = pid;
	item->cmd.arg = enif_make_copy(item->cmd.env, argv[3]);  // sql string
	if (argc == 5)
	  item->cmd.arg4 = enif_make_copy(item->cmd.env, argv[4]);  // records for bulk insert
	item->cmd.conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,500);
	return push_command(res->thread, res->rthread, pd, item);
}


static ERL_NIF_TERM exec_script(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"exec_script %d\n",argc));

	if(argc != 7 && argc != 8)
		return enif_make_badarg(env);

	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!(enif_is_binary(env,argv[3]) || enif_is_list(env,argv[3]) || enif_is_tuple(env,argv[3])))
		return make_error_tuple(env,"sql");
	if (!enif_is_number(env,argv[4]))
		return make_error_tuple(env, "term");
	if (!enif_is_number(env,argv[5]))
		return make_error_tuple(env, "index");
	if (!enif_is_binary(env,argv[6]))
		return make_error_tuple(env, "appendparam");


	item = command_create(res->thread,-1,pd);

	item->cmd.type = cmd_exec_script;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
	item->cmd.pid = pid;
	item->cmd.arg = enif_make_copy(item->cmd.env, argv[3]);  // sql string
	item->cmd.arg1 = enif_make_copy(item->cmd.env, argv[4]); // term
	item->cmd.arg2 = enif_make_copy(item->cmd.env, argv[5]); // index
	item->cmd.arg3 = enif_make_copy(item->cmd.env, argv[6]); // appendentries param binary
	if (argc == 8)
	  item->cmd.arg4 = enif_make_copy(item->cmd.env, argv[7]);  // records for bulk insert
	item->cmd.conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,500);
	return push_command(res->thread, -1, pd, item);
}

static ERL_NIF_TERM db_sync(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	u8 doit = 1;

	DBG((g_log,"db_sync\n"));

	if(argc != 3 && argc != 0)
		return enif_make_badarg(env);

	if (argc == 3)
	{
		if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
			return enif_make_badarg(env);
		if(!enif_is_ref(env, argv[1]))
			return make_error_tuple(env, "invalid_ref");
		if(!enif_get_local_pid(env, argv[2], &pid))
			return make_error_tuple(env, "invalid_pid");

		enif_mutex_lock(pd->thrMutexes[res->thread]);
		if (res->syncNum < pd->syncNumbers[res->thread])
		{
			doit = 0;
		}
		enif_mutex_unlock(pd->thrMutexes[res->thread]);

		if (doit)
		{
			item = command_create(res->thread,-1,pd);
			item->cmd.type = cmd_sync;
			item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
			item->cmd.pid = pid;
			item->cmd.conn = res;
			enif_keep_resource(res);

			enif_consume_timeslice(env,500);
			return push_command(res->thread, -1, pd, item);
		}
		else
		{
			ERL_NIF_TERM answer = enif_make_tuple2(env, argv[1], atom_ok);
			enif_send(NULL, &pid, env, answer);
			return atom_ok;
		}
	}
	else
	{
		int i;
		for (i = 0; i < pd->nthreads; i++)
		{
			item = command_create(i,-1,pd);
			item->cmd.type = cmd_sync;
			push_command(i, -1, pd, item);
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

	DBG((g_log,"checkpoint_lock\n"));

	if(argc != 4)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_is_number(env,argv[3]))
		return make_error_tuple(env, "term");

	item = command_create(res->thread,-1,pd);
	item->cmd.type = cmd_checkpoint_lock;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
	item->cmd.pid = pid;
	item->cmd.arg = enif_make_copy(item->cmd.env, argv[3]);  // 1 - lock, 0 - unlock
	item->cmd.conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,500);
	return push_command(res->thread, -1, pd, item);
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

	DBG((g_log,"iterate_db %d\n",argc));

	if(argc != 4 && argc != 5)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");

	item = command_create(res->thread,-1,pd);
	item->cmd.type = cmd_iterate;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
	item->cmd.pid = pid;
	item->cmd.conn = res;
	enif_keep_resource(res);
	item->cmd.arg = enif_make_copy(item->cmd.env,argv[3]); // evterm or iterator resource
	if (argc == 5)
	  item->cmd.arg1 = enif_make_copy(item->cmd.env,argv[4]); // evnum

	enif_consume_timeslice(env,500);
	return push_command(res->thread, res->rthread, pd, item);
}

static ERL_NIF_TERM iterate_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	iterate_resource *iter;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"iterate_close %d\n",argc));

	if(argc != 1)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], pd->iterate_type, (void **) &iter))
		return enif_make_badarg(env);

	iter->closed = 1;
	item = command_create(iter->thread,-1,pd);
	item->cmd.type = cmd_checkpoint_lock;
	item->cmd.arg = enif_make_int(item->cmd.env, 0);
	item->cmd.conn = iter->conn;
	// No keep. This way release for connection will be called on thread when done with command.
	// It will decrement keep on connection held by iterate.
	// enif_keep_resource(res);

	enif_consume_timeslice(env,500);
	return push_command(iter->thread, -1, pd, item);
}

static ERL_NIF_TERM inject_page(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"inject_page\n"))

	if(argc != 5)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
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

	item = command_create(res->thread,-1,pd);
	item->cmd.type = cmd_inject_page;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
	item->cmd.pid = pid;
	item->cmd.arg = enif_make_copy(item->cmd.env,argv[3]);  // bin
	item->cmd.arg1 = enif_make_copy(item->cmd.env,argv[4]); // header bin
	item->cmd.conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,500);
	return push_command(res->thread, -1, pd, item);
}

static ERL_NIF_TERM wal_rewind(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	DBG((g_log,"wal_rewind\n"));

	if(argc != 4)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return enif_make_badarg(env);

	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_is_number(env,argv[3]))
		return make_error_tuple(env,"evnum");

	item = command_create(res->thread,-1,pd);
	item->cmd.type = cmd_wal_rewind;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
	item->cmd.pid = pid;
	item->cmd.arg = enif_make_copy(item->cmd.env,argv[3]);
	item->cmd.conn = res;
	enif_keep_resource(res);

	enif_consume_timeslice(env,500);
	return push_command(res->thread, -1, pd, item);
}

static ERL_NIF_TERM noop(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	db_connection *res;
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	if(argc != 3)
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], pd->db_connection_type, (void **) &res))
		return enif_make_badarg(env);
	if(!enif_is_ref(env, argv[1]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[2], &pid))
		return make_error_tuple(env, "invalid_pid");

	item = command_create(res->thread,-1,pd);
	item->cmd.type = cmd_unknown;
	item->cmd.ref = enif_make_copy(item->cmd.env, argv[1]);
	item->cmd.pid = pid;

	return push_command(res->thread, -1, pd, item);
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
	const ERL_NIF_TERM *param1;
	char nodename[128];
	ERL_NIF_TERM head, tail;
	priv_data *priv;
	db_thread *controlThread = NULL;
	char staticSqls[MAX_STATIC_SQLS][256];
	int nstaticSqls;
// Apple/Win get smaller max dbsize
// Havent really tested win32 yet, but apple mmap is fucked. Can't create mmap larger than RAM.
#if defined(__APPLE__) || defined(_WIN32)
	u64 dbsize = 4096*1024*1024LL;
#else
	// 1TB def size on linux
	u64 dbsize = 4096*1024*1024*128*2LL;
#endif

#ifdef _WIN32
	WSADATA wsd;
	if (WSAStartup(MAKEWORD(1, 1), &wsd) != 0)
		return -1;
#endif
	priv = malloc(sizeof(priv_data));
	memset(priv,0,sizeof(priv_data));
	*priv_out = priv;
	priv->nReadThreads = 1;
	memset(nodename,0,128);
	enif_get_list_cell(env,info,&head,&info);
	enif_get_list_cell(env,info,&info,&tail);

#ifdef _TESTDBG_
	enif_get_string(env,head,nodename,128,ERL_NIF_LATIN1);
	g_log = fopen(nodename, "w");
#endif

	sqlite3_initialize();
	sqlite3_config(SQLITE_CONFIG_LOG, errLogCallback, NULL);
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

	// Paths will determine thread numbers. Every path has a thread.
	// {{Path1,Path2,Path3,...},{StaticSql1,StaticSql2,StaticSql3,...}}
	if (!enif_get_tuple(env,info,&i,&param))
	{
		DBG((g_log,"Param not tuple\n"));
		return -1;
	}

	if (i != 2 && i != 3 && i != 4)
		return -1;

	// if (i > 2)
	// {
	// 	if (!enif_get_int(env,param[2],&sync))
	// 		return -1;
	// }
	if (i > 2)
	{
		if (!enif_get_uint64(env,param[2],(ErlNifUInt64*)&dbsize))
			return -1;
	}
	if (i > 3)
	{
		if (!enif_get_int(env,param[3],&priv->nReadThreads))
			return -1;
	}
	if (priv->nReadThreads > 255)
		return -1;
	if (priv->nthreads+1 > 255)
		return -1;
	// if (sync)
	// 	sync = 0;
	// else
	// sync = MDB_NOSYNC;

	if (!enif_get_tuple(env,param[0],&priv->nthreads,&param1))
		return -1;

	if (!enif_get_tuple(env,param[1],&i,&param))
		return -1;

	if (i > MAX_STATIC_SQLS)
		return -1;

	nstaticSqls = i;
	for (i = 0; i < nstaticSqls; i++)
		enif_get_string(env,param[i],staticSqls[i],256,ERL_NIF_LATIN1);

	priv->db_connection_type = enif_open_resource_type(env, "actordb_driver_nif", "db_connection_type",
				destruct_connection, ERL_NIF_RT_CREATE, NULL);
	if(!priv->db_connection_type)
		return -1;

	priv->iterate_type =  enif_open_resource_type(env, "actordb_driver_nif", "iterate_type",
				destruct_iterate, ERL_NIF_RT_CREATE, NULL);
	if(!priv->iterate_type)
		return -1;

	priv->wtasks = malloc(sizeof(queue*)*(priv->nthreads+1));
	priv->rtasks = malloc(sizeof(queue*)*(priv->nthreads*priv->nReadThreads));
	priv->tids = malloc(sizeof(ErlNifTid)*(priv->nthreads+1));
	priv->rtids = malloc(sizeof(ErlNifTid)*(priv->nthreads*priv->nReadThreads));

	controlThread = malloc(sizeof(db_thread));
	memset(controlThread,0,sizeof(db_thread));
	controlThread->index = -1;
	controlThread->tasks = queue_create();
	controlThread->pd = priv;
	priv->wtasks[priv->nthreads] = controlThread->tasks;
	priv->syncNumbers = malloc(sizeof(u64)*priv->nthreads);
	priv->thrMutexes = malloc(sizeof(ErlNifMutex*)*priv->nthreads);
	memset(priv->syncNumbers, 0, sizeof(sizeof(u64)*priv->nthreads));

	if(enif_thread_create("db_connection", &(priv->tids[priv->nthreads]), thread_func, controlThread, NULL) != 0)
	{
		printf("Unable to create esqlite3 thread\r\n");
		return -1;
	}

	DBG((g_log,"Driver starting w=%d, r=%d threads. Dbsize %llu\n",priv->nthreads,priv->nReadThreads,dbsize));

	for (i = 0; i < priv->nthreads; i++)
	{
		MDB_env *menv = NULL;
		MDB_dbi infodb;
		MDB_dbi logdb;
		MDB_dbi pagesdb;
		MDB_dbi actorsdb;
		int j,k;
		// start with -1 for write thread
		for (k = -1; k < priv->nReadThreads; k++)
		{
			char lmpath[MAX_PATHNAME];
			db_thread *curThread = malloc(sizeof(db_thread));

			memset(curThread,0,sizeof(db_thread));

			if (!(enif_get_string(env,param1[i],curThread->path,MAX_PATHNAME,ERL_NIF_LATIN1) < (MAX_PATHNAME-MAX_ACTOR_NAME)))
				return -1;

			sprintf(lmpath,"%s/lmdb",curThread->path);

			if (k == -1)
			{
				// MDB INIT
				if (mdb_env_create(&menv) != MDB_SUCCESS)
					return -1;
				if (mdb_env_set_maxdbs(menv,5) != MDB_SUCCESS)
					return -1;
				if (mdb_env_set_mapsize(menv,dbsize) != MDB_SUCCESS)
					return -1;
				// Syncs are handled from erlang.
				if (mdb_env_open(menv, lmpath, MDB_NOSUBDIR|MDB_NOSYNC, 0664) != MDB_SUCCESS)
					return -1;

				// Create databases if they do not exist yet
				if (mdb_txn_begin(menv, NULL, 0, &curThread->txn) != MDB_SUCCESS)
					return -1;
				if (mdb_dbi_open(curThread->txn, "info", MDB_INTEGERKEY | MDB_CREATE, &infodb) != MDB_SUCCESS)
					return -1;
				if (mdb_dbi_open(curThread->txn, "actors", MDB_CREATE, &actorsdb) != MDB_SUCCESS)
					return -1;
				if (mdb_dbi_open(curThread->txn, "log", MDB_CREATE | MDB_DUPSORT | MDB_DUPFIXED | MDB_INTEGERDUP, &logdb) != MDB_SUCCESS)
					return -1;
				if (mdb_dbi_open(curThread->txn, "pages", MDB_CREATE | MDB_DUPSORT, &pagesdb) != MDB_SUCCESS)
					return -1;
				if (mdb_txn_commit(curThread->txn) != MDB_SUCCESS)
					return -1;
				curThread->txn = NULL;
			}

			if (k == -1)
				priv->thrMutexes[i] = enif_mutex_create("thrmutex");
			curThread->env = menv;
			curThread->pathlen = strlen(curThread->path);
			curThread->index = i;
			curThread->tasks = queue_create();
			curThread->pd = priv;
			curThread->infodb = infodb;
			curThread->actorsdb = actorsdb;
			curThread->logdb = logdb;
			curThread->pagesdb = pagesdb;
			if (k == -1)
				priv->wtasks[i] = curThread->tasks;
			else
				priv->rtasks[i*priv->nReadThreads+k] = curThread->tasks;

			curThread->nstaticSqls = nstaticSqls;
			for (j = 0; j < nstaticSqls; j++)
				memcpy(curThread->staticSqls[j], staticSqls[j], 256);

			if (k == -1)
			{
				if (enif_thread_create("wthr", &(priv->tids[i]), thread_func, curThread, NULL) != 0)
				{
					printf("Unable to create thread\r\n");
					return -1;
				}
			}
			else
			{
				if (enif_thread_create("rthr", &(priv->rtids[i*priv->nReadThreads+k]), read_thread_func, curThread, NULL) != 0)
				{
					printf("Unable to create thread\r\n");
					return -1;
				}
			}
		}
	}

	return 0;
}

static void on_unload(ErlNifEnv* env, void* pd)
{
	int i,k;
	priv_data *priv = (priv_data*)pd;
	int nthreads = priv->nthreads;

	for (i = -1; i < nthreads; i++)
	{
		for (k = -1; k < priv->nReadThreads; k++)
		{
			qitem *item = command_create(i,k,priv);
			item->cmd.type = cmd_stop;
			push_command(i, k, priv, item);

			if (i >= 0 && k == -1)
			{
				// write thread
				enif_mutex_destroy(priv->thrMutexes[i]);
				enif_thread_join((ErlNifTid)priv->tids[i],NULL);
			}
			else if (i >= 0)
			{
				DBG((g_log,"stop read\n"));
				// read thread
				enif_thread_join((ErlNifTid)priv->rtids[i*priv->nReadThreads+k],NULL);
			}
			else
			{
				// control thread
				enif_thread_join((ErlNifTid)priv->tids[nthreads],NULL);
				break;
			}
		}
	}
	free(priv->wtasks);
	free(priv->rtasks);
	free(priv->tids);
	free(priv->rtids);
	free(priv->thrMutexes);
	free(priv->syncNumbers);
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
	{"interrupt_query",1,interrupt_query},
	{"lz4_compress",1,lz4_compress},
	{"lz4_decompress",2,lz4_decompress},
	{"lz4_decompress",3,lz4_decompress},
	{"tcp_connect",6,tcp_connect},
	{"tcp_connect",7,tcp_connect},
	{"tcp_reconnect",0,tcp_reconnect},
	{"all_tunnel_call",3,all_tunnel_call},
	{"store_prepared_table",2,store_prepared_table},
	{"checkpoint_lock",4,checkpoint_lock},
	{"iterate_db",4,iterate_db},
	{"iterate_db",5,iterate_db},
	{"iterate_close",1,iterate_close},
	{"page_size",0,page_size},
	{"inject_page",5,inject_page},
	{"wal_rewind",4,wal_rewind},
	{"actor_info",4,get_actor_info},
	{"term_store",3,term_store},
	{"term_store",4,term_store},
	{"fsync_num",1,sync_num},
	{"fsync",3,db_sync},
	{"fsync",0,db_sync},
	{"replication_done",1,replication_done},
};

ERL_NIF_INIT(actordb_driver_nif, nif_funcs, on_load, NULL, NULL, on_unload);
