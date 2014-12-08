% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(actordb_driver).

-export([init/1,noop/1,
         open/1,open/2,open/3,
         exec_script/2,exec_script/3,exec_script/6,exec_script/4,exec_script/7,
         store_prepared_table/2,
         close/1,inject_page/2,
         parse_helper/1,parse_helper/2, traverse_wal/1,page_size/0, %wal_pages/1,
         % backup_init/2,backup_step/2,backup_finish/1,backup_pages/1,
         lz4_compress/1,lz4_decompress/2,lz4_decompress/3, %replicate_status/1,
         replicate_opts/2,replicate_opts/3,tcp_connect/4,all_tunnel_call/1,checkpoint_lock/2,
         tcp_connect_async/4,tcp_connect_async/5,make_wal_header/1,tcp_reconnect/0,wal_checksum/4,bind_insert/3]).

% {{ThreadPath1,ThreadPath2,...},{StaticSql1,StaticSql2,...}}
init(Threads) ->
    actordb_driver_nif:init(Threads).

open(Filename) ->
    open(Filename,0).

open(Filename,ThreadNumber) ->
    Ref = make_ref(),
    ok = actordb_driver_nif:open(Ref, self(), Filename,ThreadNumber),
    case receive_answer(Ref) of
        {ok,Connection} ->
            {ok, {actordb_driver, make_ref(),Connection}};
        {error, _Msg}=Error ->
            Error
    end.
open(Filename,ThreadNumber,Sql) ->
    Ref = make_ref(),
    ok = actordb_driver_nif:open(Ref, self(), Filename,ThreadNumber,Sql),
    case receive_answer(Ref) of
        {ok,Connection,Res} ->
            {ok, {actordb_driver, make_ref(),Connection},Res};
        {ok,Connection} ->
            {ok, {actordb_driver, make_ref(),Connection}};
        {error, _Msg}=Error ->
            Error
    end.

close({actordb_driver, _Ref, _Connection}) ->
    % Noop. Rely on GC. This is to avoid double closing.
    ok.

store_prepared_table(Indexes,Sqls) when is_tuple(Indexes), is_tuple(Sqls), tuple_size(Indexes) == tuple_size(Sqls) ->
    actordb_driver_nif:store_prepared_table(Indexes,Sqls).

make_wal_header(PageSize) ->
    actordb_driver_nif:wal_header(PageSize).

wal_checksum(Bin,C1,C2,Size) ->
    actordb_driver_nif:wal_checksum(Bin,C1,C2,Size).

parse_helper(Bin) ->
    parse_helper(Bin,0).
parse_helper(Bin,Offset) ->
    actordb_driver_nif:parse_helper(Bin,Offset).

replicate_opts({actordb_driver, _Ref, Connection},PacketPrefix) ->
    actordb_driver_nif:replicate_opts(Connection,PacketPrefix,1).
replicate_opts({actordb_driver, _Ref, Connection},PacketPrefix,Type) ->
    actordb_driver_nif:replicate_opts(Connection,PacketPrefix,Type).

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

traverse_wal({actordb_driver, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = actordb_driver_nif:traverse_wal(Connection, Ref, self()),
    receive_answer(Ref).

inject_page({actordb_driver, _Ref, Connection},Bin) ->
    Ref = make_ref(),
    ok = actordb_driver_nif:traverse_wal(Connection, Ref, self(),Bin),
    receive_answer(Ref).

page_size() ->
    actordb_driver_nif:page_size().

noop({actordb_driver, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = actordb_driver_nif:noop(Connection, Ref, self()),
    receive_answer(Ref).

bind_insert(Sql, [[_|_]|_] = Params, {actordb_driver, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = actordb_driver_nif:bind_insert(Connection,Ref,self(),Sql,Params),
    receive_answer(Ref).

exec_script(Sql, Db) ->
    exec_script(Sql,Db,infinity,0,0,<<>>).
exec_script(Sql,[_|_] = Recs, Db) ->
    exec_script(Sql,Recs,Db,infinity,0,0,<<>>);
exec_script(Sql, Db, Timeout) when is_integer(Timeout) ->
    exec_script(Sql,Db,Timeout,0,0,<<>>).
exec_script(Sql, [_|_] = Recs, Db, Timeout) when is_integer(Timeout) ->
    exec_script(Sql,Recs,Db,Timeout,0,0,<<>>).
exec_script(Sql,  {actordb_driver, _Ref, Connection},Timeout,Term,Index,AppendParam) ->
    Ref = make_ref(),
    ok = actordb_driver_nif:exec_script(Connection, Ref, self(), Sql,Term,Index,AppendParam),
    receive_answer(Ref,Connection,Timeout).
exec_script(Sql, [_|_] = Recs, {actordb_driver, _Ref, Connection},Timeout,Term,Index,AppendParam) ->
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

