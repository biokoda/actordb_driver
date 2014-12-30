all:
	../../rebar compile

clean:
	../../rebar clean

tool:
	gcc c_src/tool.c  -g -DSQLITE_DEBUG -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0  -o adbtool

valgrind:
	-rm *.db
	-rm wal.*
	gcc c_src/test.c  -g -DSQLITE_DEBUG -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0  -o t && valgrind --tool=memcheck --track-origins=yes --leak-check=full ./t
