% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_driver_nif).

-export([init/1,
open/5,
open/6,
exec_script/7,
exec_script/8,
exec_read/4,
exec_read/5,
store_prepared_table/2,
close/3,
noop/3,
parse_helper/2,
interrupt_query/1,
lz4_compress/1,
lz4_decompress/2,
lz4_decompress/3,
tcp_connect/6,
tcp_connect/7,
tcp_reconnect/0,
replicate_opts/3,
replication_done/1,
page_size/0,
iterate_db/4,
iterate_db/5,
iterate_close/1,
all_tunnel_call/3,
checkpoint_lock/4,
inject_page/5,
wal_rewind/4,
checkpoint/4,
term_store/3,
term_store/4,
actor_info/4,
fsync_num/1,
fsync/3,
fsync/0,
stmt_info/4
]).

stmt_info(_,_,_,_) ->
	exit(nif_library_not_loaded).

actor_info(_,_,_,_) ->
	exit(nif_library_not_loaded).

exec_read(_,_,_,_) ->
	exit(nif_library_not_loaded).

exec_read(_,_,_,_,_) ->
	exit(nif_library_not_loaded).

replication_done(_) ->
	exit(nif_library_not_loaded).

fsync() ->
	exit(nif_library_not_loaded).

fsync(_,_,_) ->
	exit(nif_library_not_loaded).

term_store(_,_,_) ->
	exit(nif_library_not_loaded).

term_store(_,_,_,_) ->
	exit(nif_library_not_loaded).

fsync_num(_) ->
	exit(nif_library_not_loaded).

checkpoint(_,_,_,_) ->
	exit(nif_library_not_loaded).

wal_rewind(_,_,_,_) ->
	exit(nif_library_not_loaded).

page_size() ->
	exit(nif_library_not_loaded).

iterate_db(_,_,_,_) ->
	exit(nif_library_not_loaded).
iterate_db(_,_,_,_,_) ->
	exit(nif_library_not_loaded).

iterate_close(_) ->
	exit(nif_library_not_loaded).

checkpoint_lock(_,_,_,_) ->
	exit(nif_library_not_loaded).

inject_page(_,_,_,_,_) ->
	exit(nif_library_not_loaded).

store_prepared_table(_,_) ->
	exit(nif_library_not_loaded).

all_tunnel_call(_,_,_) ->
	exit(nif_library_not_loaded).

noop(_,_,_) ->
	exit(nif_library_not_loaded).

replicate_opts(_,_,_) ->
	exit(nif_library_not_loaded).

tcp_connect(_,_,_,_,_,_) ->
	exit(nif_library_not_loaded).

tcp_connect(_,_,_,_,_,_,_) ->
	exit(nif_library_not_loaded).

tcp_reconnect() ->
	exit(nif_library_not_loaded).

interrupt_query(_) ->
	exit(nif_library_not_loaded).

parse_helper(_,_) ->
	exit(nif_library_not_loaded).

lz4_compress(_) ->
	exit(nif_library_not_loaded).

lz4_decompress(_,_) ->
	exit(nif_library_not_loaded).

lz4_decompress(_,_,_) ->
	exit(nif_library_not_loaded).

% backup_init(_,_,_,_) ->
%     exit(nif_library_not_loaded).

% backup_finish(_,_,_) ->
%     exit(nif_library_not_loaded).

% backup_step(_,_,_,_) ->
%     exit(nif_library_not_loaded).

% backup_pages(_) ->
%     exit(nif_library_not_loaded).

init(Threads) when tuple_size(Threads) == 2 orelse tuple_size(Threads) == 3 orelse tuple_size(Threads) == 4 ->
	NifName = "actordb_driver_nif",
	NifFileName = case code:priv_dir(actordb_driver) of
		{error, bad_name} -> filename:join("priv", NifName);
		Dir -> filename:join(Dir, NifName)
	end,
	case erlang:load_nif(NifFileName, ["drv_"++hd(string:tokens(atom_to_list(node()),"@"))++".txt",Threads]) of
		ok ->
			ok;
		{error,{upgrade,_}} ->
			ok;
		{error,{reload,_}} ->
			ok
	end.

% wal_pages(_) ->
%     exit(nif_library_not_loaded).

open(_Ref, _Dest, _Filename,_ThreadNumber,_Mode) ->
	exit(nif_library_not_loaded).
open(_Ref, _Dest, _Filename,_ThreadNumber,_Sql,_Mode) ->
	exit(nif_library_not_loaded).


exec_script(_Db, _Ref, _Dest, _Sql,_Term,_Index,_AParam) ->
	exit(nif_library_not_loaded).

exec_script(_Db, _Ref, _Dest, _Sql,_Term,_Index,_AParam,_RecordInsert) ->
	exit(nif_library_not_loaded).


close(_Db, _Ref, _Dest) ->
	exit(nif_library_not_loaded).
