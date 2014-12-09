-module(actordb_test).
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

replication_test() ->
    ?INIT,
    {ok,Db} = actordb_driver:open("t1.db"),
    {ok,_} = actordb_driver:exec_script("CREATE TABLE tab (id INTEGER PRIMARY KEY, val TEXT);",Db),
    {ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (1, 'asdadad');",Db),
    {ok,_} = actordb_driver:exec_script(["INSERT INTO tab VALUES (2, '",binary:copy(<<"a">>,1024*6),"');"],Db),
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
    {ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (3, 'thirdthird');",Db2),
    L2 = get_pages(Db2),
    [actordb_driver:inject_page(Db,Bin) || Bin <- L2 -- L1],
    
    % Check both
    {ok,[[{columns,{_,_}},{rows,[{3,<<"thirdthird">>},{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db),
    {ok,[[{columns,{_,_}},{rows,[{3,<<"thirdthird">>},{2,_},{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db2),
    ok.
get_pages(Db) ->
	case actordb_driver:iterate_wal(Db) of
		{ok,Bin,0,1} ->
    		[Bin|get_pages(Db)];
    	done ->
    		[]
    end.

    

    
