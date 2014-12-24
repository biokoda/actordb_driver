-module(test).
-include_lib("eunit/include/eunit.hrl").
-define(INIT,
    [begin file:delete(Fn) end || Fn <- filelib:wildcard("wal.*")],
    [file:delete(Fn) || Fn <- filelib:wildcard("*.db")],
    actordb_driver:init({{"."},{}})).
% ?debugFmt("Deleting ~s",[Fn]),

lz4_test() ->
    ?INIT,
    Bin1 = binary:copy(<<"SELECT * FROM WTF;">>,2),
    {Compressed1,CompressedSize1} = actordb_driver:lz4_compress(Bin1),
    % ?debugFmt("Compressed ~p size ~p ",[byte_size(Compressed),CompressedSize]),
    Bin1 = actordb_driver:lz4_decompress(Compressed1,byte_size(Bin1),CompressedSize1),
    ok.

mem_test() ->
    ?INIT,
    Sql = <<"select name, sql from sqlite_master where type='table';",
                    "$PRAGMA cache_size=10;">>,
    {ok,Db,_} = actordb_driver:open(":memory:",1,Sql),
    {ok,_} = actordb_driver:exec_script(<<"$CREATE TABLE tab (id INTEGER PRIMARY KEY, txt TEXT);",
        "$CREATE TABLE tab1 (id INTEGER PRIMARY KEY, txt TEXT);",
        "$ALTER TABLE tab ADD i INTEGER;$CREATE TABLE tabx (id INTEGER PRIMARY KEY, txt TEXT);">>,Db),
    {ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (1, 'asdadad',1);",Db),
    {ok,[_]} = actordb_driver:exec_script("SELECT * from tab;",Db).

replication_test() ->
    ?INIT,
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
    [actordb_driver:inject_page(Db2,Bin) || Bin <- L],
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
    ok.
get_pages(Db) ->
	get_pages(Db,1).
get_pages(Db,Iter) ->
	case actordb_driver:iterate_wal(Db,Iter) of
		{ok,Iter2,Bin,1} ->
    		[Bin|get_pages(Db,Iter2)];
    	done ->
    		[]
    end.

    

    
