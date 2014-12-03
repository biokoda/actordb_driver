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

open_single_database_test() ->
    ?INIT,
    {ok,Db} = actordb_driver:open("t1.db"),
    {ok,_} = actordb_driver:exec_script("CREATE TABLE tab (id INTEGER PRIMARY KEY, val TEXT);",Db),
    {ok,_} = actordb_driver:exec_script("INSERT INTO tab VALUES (1, 'asdadad');",Db),
    {ok,[[{columns,{_,_}},{rows,[{1,<<"asdadad">>}]}]]} = actordb_driver:exec_script("SELECT * from tab;",Db),
    ok = actordb_driver:checkpoint_lock(Db,1),
    ok = actordb_driver:checkpoint_lock(Db,1),
    ok = actordb_driver:checkpoint_lock(Db,0),
    ok.

    

    

    
    

    
    
    
