-module(test).
-include_lib("eunit/include/eunit.hrl").
-define(READTHREADS,2).
-define(DBSIZE,4096*1024*128).
-define(INIT,actordb_driver:init({{"."},{},?DBSIZE,?READTHREADS})).
-define(READ,actordb_driver:exec_read).

run_test_() ->
	[file:delete(Fn) || Fn <- filelib:wildcard("wal.*")],
	[file:delete(Fn) || Fn <- [filelib:wildcard("*.db"),"lmdb","lmdb-lock"]],
	[fun lz4/0,
	fun modes/0,
	fun dbcopy/0,
	fun checkpoint/0,
	fun bigtrans/0,
	fun bigtrans_check/0,
	{timeout,25,fun async/0}
	].


lz4() ->
	?debugFmt("lz4",[]),
	?INIT,
	Bin1 = binary:copy(<<"SELECT * FROM WTF;">>,2),
	{Compressed1,CompressedSize1} = actordb_driver:lz4_compress(Bin1),
	% ?debugFmt("Compressed ~p size ~p ",[byte_size(Compressed),CompressedSize]),
	Bin1 = actordb_driver:lz4_decompress(Compressed1,byte_size(Bin1),CompressedSize1),
	ok.

modes() ->
	?debugFmt("modes",[]),
	?INIT,
	Sql = <<"select name, sql from sqlite_master where type='table';",
					"$PRAGMA cache_size=10;">>,
	{ok,Db,_} = actordb_driver:open(":memory:",1,Sql),
	{ok,_} = actordb_driver:exec_script(<<"$CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT);",
		"$CREATE TABLE tab1 (id INTEGER PRIMARY KEY, txt TEXT);",
		"$ALTER TABLE tab ADD i INTEGER;$CREATE TABLE tabx (id INTEGER PRIMARY KEY, txt TEXT);">>,Db),
	{ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (1, 'asdadad',1);",Db),
	{ok,[_]} = ?READ("SELECT * from tab;",Db),
	Sql1 = "INSERT INTO tab VALUES (2, 'asdadad',1);",
	Sql2 = "INSERT INTO tab VALUES (3, 'tritri',1);",
	Sql3 = "SELECT * FROM tab;",
	{ok,{_,_,_}} = R = actordb_driver:exec_script({Sql1,Sql2,Sql3},Db),
	% ?debugFmt("Tuple exec ~p", [R]).

	{ok,Blob} = actordb_driver:open("myfile",0,blob),
	{ok,{[],[]}} = actordb_driver:exec_script({1,2},{<<"page12">>,<<"page">>},Blob),
	{ok,{[<<"page12">>],[<<"page">>],[]}} = actordb_driver:exec_script({1,2,3},Blob),
	ok.

async() ->
	?debugFmt("Running many async reads/writes for 20s",[]),
	ets:new(ops,[set,public,named_table,{write_concurrency,true}]),
	ets:insert(ops,{w,0}),
	ets:insert(ops,{r,0}),
	Pids = [element(1,spawn_monitor(fun() -> w(P) end)) || P <- lists:seq(1,100)],
	receive
		{'DOWN',_Monitor,_,_PID,Reason} ->
			exit(Reason)
	after 20000 ->
		ok
	end,
	[exit(P,stop) || P <- Pids],
	timer:sleep(3000),
	?debugFmt("Reads: ~p, Writes: ~p",[ets:lookup(ops,r),ets:lookup(ops,w)]).

w(N) ->
	{ok,Db} = actordb_driver:open("ac"++integer_to_list(N),N),
	Sql = "CREATE TABLE tab (id integer primary key, val text);",
	{ok,_} = actordb_driver:exec_script(Sql,Db,infinity,1,1,<<>>),
	w(Db,1).
w(Db,C) ->
	case C rem 5 of
		0 ->
			Sql = ["INSERT INTO tab VALUES (",integer_to_list(C),",'bbb');"],
			{ok,_} = actordb_driver:exec_script(Sql,Db,infinity,1,C,<<>>),
			ets:update_counter(ops,w,{2,1});
		_ ->
			{ok,_} = ?READ("select * from tab",Db),
			ets:update_counter(ops,r,{2,1})
	end,
	w(Db,C+1).


dbcopy() ->
	?INIT,
	{ok,Db} = actordb_driver:open("original"),
	{ok,_} = actordb_driver:exec_script("CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT, val INTEGER);",Db,infinity,1,1,<<>>),
	ok = actordb_driver:term_store(Db,10,<<"abcdef">>),
	{{1,1},{1,1},{0,0},2,2,10,<<"abcdef">>} = actordb_driver:actor_info("original",0),
	ok = actordb_driver:term_store("original",10,<<"abcdef1">>,0),
	EN = 100,
	[ {ok,_} = actordb_driver:exec_script(["INSERT INTO tab VALUES (",integer_to_list(N+100),",'aaa',2)"],Db,infinity,1,N,<<>>) || N <- lists:seq(2,EN)],
	0 = actordb_driver:fsync_num(Db),
	ok = actordb_driver:fsync(Db),
	0 = actordb_driver:fsync_num(Db),
	{ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (2,'bbb',3)",Db,infinity,1,EN+1,<<>>),
	{ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (3,'ccc',4)",Db,infinity,1,EN+2,<<>>),
	ok = actordb_driver:replication_done(Db),
	{ok,Select} = ?READ("select * from tab;",Db),

	{ok,_} = actordb_driver:exec_script("SAVEPOINT 'adb'; UPDATE tab SET txt='ccc123' where id=3;",Db,infinity,1,EN+3,<<>>),
	{ok,_} = actordb_driver:exec_script("ROLLBACK;",Db),
	{ok,Select} = ?READ("select * from tab;",Db),
	% ?debugFmt("Select ~p",[Select]),
	{ok,Copy} = actordb_driver:open("copy"),
	{ok,Iter,Bin,Head,Done} =  actordb_driver:iterate_db(Db,0,0),
	ok = actordb_driver:inject_page(Copy,Bin,Head),
	% This will export into an sqlite file named sq.
	{ok,F} = file:open("sq",[write,binary,raw]),
	?debugFmt("Exporting actor into an sqlite file ~p",[Done]),
	% readpages(Head,Bin,F),
	file:write(F,actordb_driver:lz4_decompress(Bin,4096)),
	case Done > 0 of
	  true ->
		  ok;
	  _ ->
		  copy(Db,Iter,F,Copy)
	end,
	% ?debugFmt("pages=~pB, evterm=~p, evnum=~p",[byte_size(Bin), Evterm, Evnum1]),
	file:close(F),
	?debugFmt("Reading from exported sqlite file: ~p",[os:cmd("sqlite3 sq \"select * from tab\"")]),
	{ok,Select} = ?READ("select * from tab;",Copy),
	?debugFmt("Reading from copy!: ~p",[Select]),
	file:delete("sq"),

	% {ok,Copy2} = actordb_driver:open("copy2"),
	% {ok,_Iter2,Bin2,Head2,Done2} = actordb_driver:iterate_db(Db,1,1), % get pgno1 and pgno2 (create table)
	% <<A:64,B:64,PGNO:32,Commit:32>> = Head2,
	% ?debugFmt("Second inject ~p ~p ~p ~p",[A,B,PGNO,Commit]),
	% readpages(Bin2,undefined),
	% Inject pgno1
	% case Done2 > 0 of
	% 	true ->
	% 		ok;
	% 	_ ->
	% 		copy(Db,_Iter2,undefined,Copy2)
	% end,
	% {ok,_Iter3,Bin3,Head3,_Done3} = actordb_driver:iterate_db(Db,1,1), % get pgno2 with first insert
	% ok = actordb_driver:inject_page(Copy2,Bin3,Head3),
	% FirstInject = {ok,[[{columns,{<<"id">>,<<"txt">>,<<"val">>}},{rows,[{102,<<"aaa">>,2}]}]]},
	% FirstInject = actordb_driver:exec_script("select * from tab;",Copy2),
	% ?debugFmt("Reading from second copy success! - only first insert:~n ~p",[FirstInject]),
	{{1,1},{1,102},{0,0},2,103,10,<<"abcdef1">>} = Info = actordb_driver:actor_info("original",0),
	?debugFmt("Get actor info ~p",[Info]),
	?debugFmt("Rewind original to last insert!",[]),
	{ok,1} = actordb_driver:iterate_db(Db,2,10),
	% ok = actordb_driver:checkpoint(Db,60).
	ok = actordb_driver:wal_rewind(Db,100),
	{ok,[[{columns,{<<"id">>,<<"txt">>,<<"val">>}},
      {rows,[{199,<<"aaa">>,2},{198,<<"aaa">>,2}|_] = Rows}]]} = ?READ("select * from tab;",Db),
	[{102,<<"aaa">>,2}|_] = lists:reverse(Rows).
	% ?debugFmt("After rewind to evnum=2: ~p",[FirstInject]).

checkpoint() ->
	garbage_collect(),
	?debugFmt("Checkpoint!",[]),
	{ok,Db} = actordb_driver:open("original"),
	{ok,S} = actordb_driver:exec_script("select * from tab;",Db),
	ok = actordb_driver:checkpoint(Db,60),
	{ok,S} = ?READ("select * from tab;",Db),
	[[{columns,{<<"id">>,<<"txt">>,<<"val">>}},
      {rows,[{199,<<"aaa">>,2},{198,<<"aaa">>,2}|_]}]] = S,
	% ?debugFmt("AfterCheckpoint ~p",[S]),
	ok = actordb_driver:wal_rewind(Db,0),
	?debugFmt("After rewind to 0=~p",[actordb_driver:actor_info("original",0)]),
	ok.

copy(Orig,Iter,F,Copy) ->
	case actordb_driver:iterate_db(Orig,Iter) of
		{ok,Iter1,Bin,Head,Done} ->
			<<Evterm:64,Evnum:64,_/binary>> = Head,
			?debugFmt("pages=~pB, evterm=~p, evnum=~p",[byte_size(Bin), Evterm, Evnum]),
			ok = actordb_driver:inject_page(Copy,Bin,Head),
			file:write(F,actordb_driver:lz4_decompress(Bin,4096)),
			case Done > 0 of
				true ->
					ok;
				_ ->
					copy(Orig,Iter1,F,Copy)
			end
	end.


bigtrans() ->
	?INIT,
	application:ensure_all_started(crypto),
	?debugFmt("Generating large sql",[]),
	Sql = [<<"SAVEPOINT 'adb';",
	"CREATE TABLE IF NOT EXISTS __transactions (id INTEGER PRIMARY KEY, tid INTEGER, updater INTEGER, node TEXT,",
	  "schemavers INTEGER, sql TEXT);",
	"CREATE TABLE IF NOT EXISTS __adb (id INTEGER PRIMARY KEY, val TEXT);",
   "CREATE TABLE t_task ( id INTEGER NOT NULL, project_id INTEGER NOT NULL, group_id INTEGER NOT NULL, owner_id TEXT NOT NULL,",
	 " assignee_id TEXT, title TEXT NOT NULL, category TEXT NOT NULL, status TEXT NOT NULL, priority INTEGER NOT NULL, ",
	 " created INTEGER NOT NULL, assigned INTEGER NOT NULL, deadline INTEGER NOT NULL, PRIMARY KEY(id)) WITHOUT ",
	 " ROWID;",
	"CREATE TABLE t_comments ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, posted INTEGER NOT NULL, content TEXT NOT NULL,",
	" poster_id INTEGER NOT NULL, parent_id INTEGER);",
	"CREATE TABLE t_multimedia ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, mime TEXT NOT NULL,",
	  " content BLOB, owner_id INTEGER NOT NULL);",
	"CREATE TABLE t_task_files ( file_id INTEGER PRIMARY KEY REFERENCES multimedia(id));",
	"CREATE TABLE t_comment_files ( file_id INTEGER PRIMARY KEY REFERENCES multimedia(id));",
	"CREATE TABLE t_history ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, prev_assignee_id INTEGER NOT NULL,",
	  " prev_project_id INTEGER NOT NULL, prev_group_id INTEGER NOR NULL, moved INTEGER NOT NULL, info TEXT NOT NULL);",
	"CREATE TABLE t_followers ( id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, joined INTEGER NOT NULL);",
	"INSERT  INTO t_task (id,project_id,group_id,title,category,priority,owner_id,assignee_id,created,assigned",
	 ",deadline,status) VALUES (1015,1,1014,'Task','Sexy','low','bbb','bbb',1427955816,1427955816,100,'waiting');",
	"INSERT OR REPLACE INTO __adb (id,val) VALUES (1,'1');INSERT OR REPLACE INTO __adb (id,val) VALUES (9,'1');",
	"INSERT OR REPLACE INTO __adb (id,val) VALUES (3,'7');INSERT OR REPLACE INTO __adb (id,val) VALUES (4,'task');",
	"INSERT OR REPLACE INTO __adb (id,val) VALUES (1,'0');INSERT OR REPLACE INTO __adb (id,val) VALUES (9,'0');",
	"INSERT OR REPLACE INTO __adb (id,val) VALUES (7,'614475188');">>,
	"INSERT INTO __adb (id,val) VALUES (10,'",base64:encode(crypto:rand_bytes(1024*1024*10)),"');", % worst case scenario, incompressible data
	"INSERT INTO __adb (id,val) VALUES (?1, ?2);",
	"INSERT INTO __adb (id,val) VALUES (?1, ?2);",
	"DELETE from __adb where id=10;",
	"RELEASE SAVEPOINT 'adb';"],
	?debugFmt("Running large sql",[]),
	{ok,Db} = actordb_driver:open("big.db"),
	Param = [[[111,"fromparam1"],[222,"fromparam2"],[333,"fromparam3"]],
			 [[444,"secondstat"],[555,"secondstatement"]]],
	Res = [{changes,555,1},{changes,555,1},{changes,555,1},{changes,444,1},{changes,333,1},{changes,222,1},
	 {changes,111,1},{changes,10,1},{changes,7,1},{changes,9,1},{changes,1,1},{changes,4,1},{changes,3,1},
	 {changes,9,1},{changes,1,1},{changes,0,1},{changes,0,0},{changes,0,0},{changes,0,0},{changes,0,0},
	 {changes,0,0},{changes,0,0},{changes,0,0},{changes,0,0},{changes,0,0},{changes,0,0}],
	{ok,Res} = actordb_driver:exec_script(Sql,Param,Db),

	SR = {ok,[[{columns,{<<"id">>,<<"val">>}},{rows,[{555,<<"secondstatement">>},
	{444,<<"secondstat">>},{333,<<"fromparam3">>},{222,<<"fromparam2">>},{111,<<"fromparam1">>},
	{9,<<"0">>},{7,<<"614475188">>},{4,<<"task">>},{3,<<"7">>},{1,<<"0">>}]}]]},
	SR = ?READ("SELECT * FROM __adb;",Db),
	?debugFmt("select=~p",[SR]),

	SR2 = [[{columns,{<<"id">>,<<"val">>}},{rows,[{555,<<"secondstatement">>}]}],
		   [{columns,{<<"id">>,<<"val">>}},{rows,[{444,<<"secondstat">>}]}],
		   [{columns,{<<"id">>,<<"val">>}},{rows,[{9,<<"0">>}]}],
		   [{columns,{<<"id">>,<<"val">>}},{rows,[{3,<<"7">>}]}]],
	{ok,SR2} = ?READ(["SELECT * FROM __adb where id=?1;","SELECT * FROM __adb where id=?1;"],[[[3],[9]],[[444],[555]]],Db),
	?debugFmt("Double param select=~p",[SR2]).

bigtrans_check() ->
	?debugFmt("Reload and checking if all still there!",[]),
	file:copy("drv_nonode.txt","prev_drv_nonode.txt"),
	garbage_collect(),
	code:delete(actordb_driver_nif),
	code:purge(actordb_driver_nif),
	false = code:is_loaded(actordb_driver_nif),
	?INIT,

	Sql = "select * from __adb;",
	{ok,Db2} = actordb_driver:open("big.db"),
	R = ?READ(Sql,Db2),
	?debugFmt("~p",[R]),
	ok.
