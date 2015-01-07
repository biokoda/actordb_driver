-module(test).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    [file:delete(Fn) || Fn <- filelib:wildcard("wal.*")],
    [file:delete(Fn) || Fn <- filelib:wildcard("*.db")],
    [fun lz4/0,
     fun modes/0,
     fun repl/0,
     fun check/0].

check() ->
    ?debugFmt("Reload and checking result of repl",[]),
    garbage_collect(),
    code:delete(actordb_driver_nif),
    code:purge(actordb_driver_nif),
    false = code:is_loaded(actordb_driver_nif),
    actordb_driver:init({{"."},{},100}),
    Sql = "select name, sql from sqlite_master where type='table';",
    {ok,_Db,{ok,[[{columns,_},{rows,[]}]]}} = actordb_driver:open("t1.db",1,Sql),
    {ok,Db2} = actordb_driver:open("t2.db"),
    {ok,[[{columns,{_,_}},{rows,[{3,<<"thirdthird">>},{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db2),
    {ok,Db3} = actordb_driver:open("t3.db"),
    {ok,[[{columns,{_,_}},{rows,[{3,<<"thirdthird">>},{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db3),
    ok.

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
    {ok,[_]} = actordb_driver:exec_script("SELECT * from tab;",Db),
    
    {ok,Db1} = actordb_driver:open("deletemode.db",1,delete),
    {ok,_} = actordb_driver:exec_script(<<"$CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT);",
        "$CREATE TABLE tab1 (id INTEGER PRIMARY KEY, txt TEXT);",
        "$ALTER TABLE tab ADD i INTEGER;$CREATE TABLE tabx (id INTEGER PRIMARY KEY, txt TEXT);">>,Db1),
    {ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (1, 'asdadad',1);",Db1),
    {ok,[_]} = actordb_driver:exec_script("SELECT * from tab;",Db1).

repl() ->
    ?debugFmt("repl",[]),
    {ok,Db} = actordb_driver:open("t1.db"),
    % exec_script(Sql,  {actordb_driver, _Ref, Connection},Timeout,Term,Index,AppendParam) ->
    {ok,_} = actordb_driver:exec_script("CREATE TABLE tab (id INTEGER PRIMARY KEY, val TEXT);",Db,10000,1,1,<<>>),
    {ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (1, 'asdadad');",Db,10000,1,2,<<>>),
    {ok,_} = actordb_driver:exec_script(["INSERT INTO tab VALUES (2, '",binary:copy(<<"a">>,1024*6),"');"],Db,10000,1,3,<<>>),
    {ok,[[{columns,{_,_}},{rows,[{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db),
    ok = actordb_driver:checkpoint_lock(Db,1),
    ok = actordb_driver:checkpoint_lock(Db,1),
    ok = actordb_driver:checkpoint_lock(Db,0),

    % Create copy of base db file, read wal pages, inject wal pages for second db file and read data
    L = get_pages(Db),
    file:copy("t1.db","t2.db"),
    {ok,Db2} = actordb_driver:open("t2.db"),
    [ok = actordb_driver:inject_page(Db2,Bin) || Bin <- L],
    {ok,[[{columns,{_,_}},{rows,[{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db2),
    
    % Now insert into second db, copy new pages back into first db
    L1 = get_pages(Db2),
    {ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (3, 'thirdthird');",Db2,10000,1,4,<<>>),
    L2 = get_pages(Db2),
    [actordb_driver:inject_page(Db,Bin) || Bin <- L2 -- L1],
    
    % Check both
    {ok,[[{columns,{_,_}},{rows,[{3,<<"thirdthird">>},{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db),
    {ok,[[{columns,{_,_}},{rows,[{3,<<"thirdthird">>},{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db2),

    % make last write go away on first db
    actordb_driver:wal_rewind(Db,4),
    {ok,[[{columns,{_,_}},{rows,[{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db),
    {ok,[[{columns,{_,_}},{rows,[{3,<<"thirdthird">>},{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db2),

    file:copy("t2.db","t3.db"),
    % 0 means return all pages in wal for connection
    Pages2 = get_pages(Db2,0),
    {ok,Db3} = actordb_driver:open("t3.db"),
    [actordb_driver:inject_page(Db3,Bin) || Bin <- Pages2],
    {ok,[[{columns,{_,_}},{rows,[{3,<<"thirdthird">>},{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db3),
    delete(Db).
    % repl1(Db);
% repl1(Db) ->

delete(undefined) ->
    garbage_collect(),
    timer:sleep(100),
    ?assertMatch({error,enoent},file:read_file_info("t1.db"));
    % {ok,Db} = actordb_driver:open("t1.db"),
delete(Db) ->
    actordb_driver:delete_actor(Db),
    delete(undefined).


get_pages(Db) ->
	get_pages(Db,1).
get_pages(Db,Iter) ->
	case actordb_driver:iterate_wal(Db,Iter) of
		{ok,Iter2,Bin,1} ->
            % <<_:36/binary,Name:20/binary,_/binary>> = Bin,
            % ?debugFmt("Inject page ~s",[Name]),
    		[Bin|get_pages(Db,Iter2)];
    	done ->
    		[]
    end.

    

    
