all:
	../../rebar compile

clean:
	../../rebar clean

eunit:
	../../rebar eunit

tool:
	gcc c_src/tool.c  -g -DSQLITE_DEBUG -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0  -o adbtool

lldb:
	-rm *.db wal.*
	gcc c_src/test.c  -g -DSQLITE_DEBUG -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0  -o t && lldb t

valgrind:
	-rm *.db wal.*
	gcc c_src/test.c  -g -DSQLITE_DEBUG -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0  -o t && valgrind --tool=memcheck --track-origins=yes --leak-check=full ./t
