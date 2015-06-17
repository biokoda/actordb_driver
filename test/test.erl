-module(test).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->
	[file:delete(Fn) || Fn <- filelib:wildcard("wal.*")],
	[file:delete(Fn) || Fn <- [filelib:wildcard("*.db"),"lmdb","lmdb-lock"]],
	[fun lz4/0,
	 fun modes/0,
	 fun dbcopy/0,
	 fun bigtrans/0,
	 fun bigtrans_check/0
		 ].


lz4() ->
	actordb_driver:init({{"."},{},100}),
	?debugFmt("lz4",[]),
	Bin1 = binary:copy(<<"SELECT * FROM WTF;">>,2),
	{Compressed1,CompressedSize1} = actordb_driver:lz4_compress(Bin1),
	% ?debugFmt("Compressed ~p size ~p ",[byte_size(Compressed),CompressedSize]),
	Bin1 = actordb_driver:lz4_decompress(Compressed1,byte_size(Bin1),CompressedSize1),
	ok.

modes() ->
	?debugFmt("modes",[]),
	Sql = <<"select name, sql from sqlite_master where type='table';",
					"$PRAGMA cache_size=10;">>,
	{ok,Db,_} = actordb_driver:open(":memory:",1,Sql),
	{ok,_} = actordb_driver:exec_script(<<"$CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT);",
		"$CREATE TABLE tab1 (id INTEGER PRIMARY KEY, txt TEXT);",
		"$ALTER TABLE tab ADD i INTEGER;$CREATE TABLE tabx (id INTEGER PRIMARY KEY, txt TEXT);">>,Db),
	{ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (1, 'asdadad',1);",Db),
	{ok,[_]} = actordb_driver:exec_script("SELECT * from tab;",Db).

dbcopy() ->
	actordb_driver:init({{"."},{},100}),
	{ok,Db} = actordb_driver:open("original"),
	{ok,_} = actordb_driver:exec_script("CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT, val INTEGER);",Db,infinity,1,1,<<>>),
	ok = actordb_driver:term_store(Db,10,<<"abcdef">>),
	EN = 100,
	[ {ok,_} = actordb_driver:exec_script(["INSERT INTO tab VALUES (",integer_to_list(N+100),",'aaa',2)"],Db,infinity,1,N,<<>>) || N <- lists:seq(2,EN)],
	{ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (2,'bbb',3)",Db,infinity,1,EN+1,<<>>),
	{ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (3,'ccc',4)",Db,infinity,1,EN+2,<<>>),
	{ok,Select} = actordb_driver:exec_script("select * from tab;",Db),
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
	{ok,Select} = actordb_driver:exec_script("select * from tab;",Copy),
	?debugFmt("Reading from copy!: ~p",[Select]),
	file:delete("sq"),

	{ok,Copy2} = actordb_driver:open("copy2"),
	{ok,_Iter2,Bin2,Head2,_Done2} = actordb_driver:iterate_db(Db,1,1), % get pgno1 and pgno2 (create table)
	% readpages(Bin2,undefined),
	actordb_driver:inject_page(Copy2,Bin2,Head2),
	{ok,_Iter3,Bin3,Head3,_Done3} = actordb_driver:iterate_db(Db,1,2), % get pgno2 with first insert
	actordb_driver:inject_page(Copy2,Bin3,Head3),
	FirstInject = {ok,[[{columns,{<<"id">>,<<"txt">>,<<"val">>}},{rows,[{102,<<"aaa">>,2}]}]]},
	FirstInject = actordb_driver:exec_script("select * from tab;",Copy2),
	?debugFmt("Reading from second copy success! - only first insert:~n ~p",[FirstInject]),
	?debugFmt("Get actor info ~p",[actordb_driver:actor_info("original",0)]),
	?debugFmt("Rewind original to last insert!",[]),
	ok = actordb_driver:wal_rewind(Db,3),
	FirstInject = actordb_driver:exec_script("select * from tab;",Db),
	?debugFmt("After rewind to evnum=2: ~p",[FirstInject]).


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
	actordb_driver:init({{"."},{},100}),
	application:ensure_all_started(crypto),
	?debugFmt("Generating large sql",[]),
	Sql = iolist_to_binary([<<"SAVEPOINT 'adb';",
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
	"DELETE from __adb where id=10;",
	"RELEASE SAVEPOINT 'adb';"]),
	?debugFmt("Running large sql",[]),
	{ok,Db,{ok,Res}} = actordb_driver:open("big.db",0,Sql,wal),
	?debugFmt("Result: ~p, select=~p",[Res,actordb_driver:exec_script("SELECT * FROM __adb;",Db)]).

bigtrans_check() ->
	?debugFmt("Reload and checking if all still there!",[]),
	file:copy("drv_nonode.txt","prev_drv_nonode.txt"),
	garbage_collect(),
	code:delete(actordb_driver_nif),
	code:purge(actordb_driver_nif),
	false = code:is_loaded(actordb_driver_nif),
	actordb_driver:init({{"."},{},100}),

	Sql = "select * from __adb;",
	{ok,Db2} = actordb_driver:open("big.db"),
	R = actordb_driver:exec_script(Sql,Db2),
	?debugFmt("~p",[R]),
	ok.
