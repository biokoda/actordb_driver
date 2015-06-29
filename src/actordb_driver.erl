% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_driver).

-export([init/1,noop/1,
		 open/1,open/2,open/3,open/4,
		 exec_script/2,exec_script/3,exec_script/6,exec_script/4,exec_script/7,
		 store_prepared_table/2,
		 close/1,inject_page/3,
		 parse_helper/1,parse_helper/2, iterate_db/2,iterate_db/3,page_size/0, %wal_pages/1,
		 % backup_init/2,backup_step/2,backup_finish/1,backup_pages/1,
		 lz4_compress/1,lz4_decompress/2,lz4_decompress/3, %replicate_status/1,
		 iterate_close/1, fsync_num/1,fsync/1,fsync/0,
		 replicate_opts/2,replicate_opts/3,tcp_connect/4,all_tunnel_call/1,checkpoint_lock/2,
		 checkpoint/2, term_store/3,term_store/4, actor_info/2, wal_rewind/2,
		 tcp_connect_async/4,tcp_connect_async/5,%make_wal_header/1, wal_checksum/4,
		 tcp_reconnect/0]).

% Every path is a write thread.
% {{Path1,Path2,...},{StaticSql1,StaticSql2,...},MaxDbSize, ReadThreadsPerWriteThread}
init(Threads) ->
	actordb_driver_nif:init(Threads).

open(Filename) ->
	open(Filename,0,wal).
open(Filename,ThreadNumber) ->
	open(Filename,ThreadNumber,wal).
open(Filename,ThreadNumber,Mode) when Mode == wal; Mode == off; Mode == delete; Mode == persist; Mode == truncate ->
	Ref = make_ref(),
	ok = actordb_driver_nif:open(Ref, self(), Filename,ThreadNumber,Mode),
	case receive_answer(Ref) of
		{ok,Connection} ->
			{ok, {actordb_driver, make_ref(),Connection}};
		{error, _Msg}=Error ->
			Error
	end;
open(Filename,ThreadNumber,Sql) when is_binary(Sql); is_list(Sql) ->
	open(Filename,ThreadNumber,Sql,wal).
open(Filename,ThreadNumber,Sql,Mode) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:open(Ref, self(), Filename,ThreadNumber,Mode,Sql),
	case receive_answer(Ref) of
		{ok,Connection,Res} ->
			{ok, {actordb_driver, make_ref(),Connection},Res};
		{ok,Connection} ->
			{ok, {actordb_driver, make_ref(),Connection}};
		{error, _Msg}=Error ->
			Error
	end.

actor_info(Name,Thread) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:actor_info(Ref,self(),Name,Thread),
	receive_answer(Ref).

term_store({actordb_driver, _Ref, Connection},CurrentTerm,VotedFor) ->
	ok = actordb_driver_nif:term_store(Connection, CurrentTerm, VotedFor).
term_store(Name, CurrentTerm, VotedFor, Thread) ->
	ok = actordb_driver_nif:term_store(Name, CurrentTerm, VotedFor, Thread).

close({actordb_driver, _Ref, _Connection}) ->
	% Noop. Rely on GC. This is to avoid double closing.
	ok.

store_prepared_table(Indexes,Sqls) when is_tuple(Indexes), is_tuple(Sqls), tuple_size(Indexes) == tuple_size(Sqls) ->
	actordb_driver_nif:store_prepared_table(Indexes,Sqls).

checkpoint({actordb_driver, _Ref, Connection}, Evnum) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:checkpoint(Connection,Ref,self(),Evnum),
	receive_answer(Ref).

% make_wal_header(PageSize) ->
%     actordb_driver_nif:wal_header(PageSize).

% wal_checksum(Bin,C1,C2,Size) ->
%     actordb_driver_nif:wal_checksum(Bin,C1,C2,Size).

parse_helper(Bin) ->
	parse_helper(Bin,0).
parse_helper(Bin,Offset) ->
	actordb_driver_nif:parse_helper(Bin,Offset).

replicate_opts(Con,PacketPrefix) ->
	replicate_opts(Con,PacketPrefix,1).
replicate_opts({actordb_driver, _Ref, Connection},PacketPrefix,Type) ->
	ok = actordb_driver_nif:replicate_opts(Connection,PacketPrefix,Type).

% replicate_status({actordb_driver, _Ref, Connection}) ->
%     actordb_driver_nif:replicate_status(Connection).

tcp_connect(Ip,Port,ConnectStr,ConnNumber) ->
	Ref = make_ref(),
	actordb_driver_nif:tcp_connect(Ref,self(),Ip,Port,ConnectStr,ConnNumber),
	receive_answer(Ref).
tcp_connect_async(Ip,Port,ConnectStr,ConnNumber) ->
	Ref = make_ref(),
	actordb_driver_nif:tcp_connect(Ref,self(),Ip,Port,ConnectStr,ConnNumber),
	Ref.
tcp_connect_async(Ip,Port,ConnectStr,ConnNumber,Type) ->
	Ref = make_ref(),
	actordb_driver_nif:tcp_connect(Ref,self(),Ip,Port,ConnectStr,ConnNumber,Type),
	Ref.
tcp_reconnect() ->
	actordb_driver_nif:tcp_reconnect().

all_tunnel_call(Bin) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:all_tunnel_call(Ref,self(),Bin),
	receive_answer(Ref).

lz4_compress(B) ->
	actordb_driver_nif:lz4_compress(B).
lz4_decompress(B,SizeOrig) ->
	actordb_driver_nif:lz4_decompress(B,SizeOrig).
lz4_decompress(B,SizeOrig,SizeIn) ->
	actordb_driver_nif:lz4_decompress(B,SizeOrig,SizeIn).

% wal_pages({actordb_driver, _Ref, Connection}) ->
%     actordb_driver_nif:wal_pages(Connection).

wal_rewind({actordb_driver, _Ref, Connection},Evnum) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:wal_rewind(Connection, Ref, self(),Evnum),
	receive_answer(Ref).

fsync_num({actordb_driver, _Ref, Connection}) ->
	actordb_driver_nif:fsync_num(Connection).
fsync() ->
	ok = actordb_driver_nif:fsync().
fsync({actordb_driver, _Ref, Connection}) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:fsync(Connection, Ref, self()),
	receive_answer(Ref).

iterate_close({iter,Iter}) ->
	ok = actordb_driver_nif:iterate_close(Iter).

iterate_db({actordb_driver, _Ref, Connection},{iter,Iter}) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:iterate_db(Connection, Ref, self(),Iter),
	receive_answer(Ref).
iterate_db({actordb_driver, _Ref, Connection},Evterm,Evnum) when is_integer(Evnum) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:iterate_db(Connection, Ref, self(), Evterm,Evnum),
	receive_answer(Ref).

inject_page({actordb_driver, _Ref, Connection},Bin,Head) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:inject_page(Connection, Ref, self(),Bin,Head),
	receive_answer(Ref).
% inject_page({actordb_driver, _Ref, Connection},Bin,Head) ->
% 	Ref = make_ref(),
% 	ok = actordb_driver_nif:inject_page(Connection, Ref, self(),Bin,Head),
% 	receive_answer(Ref).

page_size() ->
	actordb_driver_nif:page_size().

noop({actordb_driver, _Ref, Connection}) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:noop(Connection, Ref, self()),
	receive_answer(Ref).

exec_script(Sql, Db) ->
	exec_script(Sql,Db,infinity,0,0,<<>>).
exec_script(Sql,Recs, Db) when is_list(Recs) ->
	exec_script(Sql,Recs,Db,infinity,0,0,<<>>);
exec_script(Sql, Db, Timeout) when is_integer(Timeout) ->
	exec_script(Sql,Db,Timeout,0,0,<<>>).
exec_script(Sql, Recs, Db, Timeout) when is_integer(Timeout), is_list(Recs) ->
	exec_script(Sql,Recs,Db,Timeout,0,0,<<>>).
exec_script(Sql,  {actordb_driver, _Ref, Connection},Timeout,Term,Index,AppendParam) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:exec_script(Connection, Ref, self(), Sql,Term,Index,AppendParam),
	receive_answer(Ref,Connection,Timeout).
exec_script(Sql, Recs, {actordb_driver, _Ref, Connection},Timeout,Term,Index,AppendParam) when is_list(Recs) ->
	Ref = make_ref(),
	ok = actordb_driver_nif:exec_script(Connection, Ref, self(), Sql,Term,Index,AppendParam,Recs),
	receive_answer(Ref,Connection,Timeout).

checkpoint_lock({actordb_driver, _Ref, Connection},Lock) ->
	case Lock of
		true ->
			L = 1;
		false ->
			L = 0;
		0 = L ->
			ok;
		1 = L ->
			ok
	end,
	Ref = make_ref(),
	ok = actordb_driver_nif:checkpoint_lock(Connection,Ref,self(),L).

% backup_init({actordb_driver, _, Dest},{actordb_driver, _, Src}) ->
%     Ref = make_ref(),
%     ok = actordb_driver_nif:backup_init(Dest,Src,Ref,self()),
%     case receive_answer(Ref) of
%         {ok,B} ->
%             {ok,{backup,make_ref(),B}};
%         error ->
%             error
%     end.

% backup_step({backup, _Ref, B},N) ->
%     Ref = make_ref(),
%     ok = actordb_driver_nif:backup_step(B,Ref,self(),N),
%     receive_answer(Ref).

% backup_finish({backup, _Ref, B}) ->
%     Ref = make_ref(),
%     ok = actordb_driver_nif:backup_finish(B,Ref,self()),
%     receive_answer(Ref).

% backup_pages({backup, _Ref, B}) ->
%     actordb_driver_nif:backup_pages(B).


receive_answer(Ref) ->
	receive
		{Ref, Resp} -> Resp
	end.
receive_answer(Ref,Connection,Timeout) ->
	receive
		{Ref,Resp} ->
			Resp
	after Timeout ->
		ok = actordb_driver_nif:interrupt_query(Connection),
		receive
			{Ref,Resp} ->
				Resp
		end
   end.
