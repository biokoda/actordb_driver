ifeq ($(uname_S),Darwin)
	TOOLCMD = gcc c_src/tool.c c_src/mdb.c c_src/midl.c c_src/lz4.c  -D_TESTAPP_=1 -DMDB_MAXKEYSIZE=0 -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0  -o adbtool
else
	TOOLCMD = gcc c_src/tool.c c_src/mdb.c c_src/midl.c c_src/lz4.c  -D_TESTAPP_=1 -DMDB_MAXKEYSIZE=0 -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0 -lpthread -ldl -o adbtool
endif

ifeq ($(uname_S),Darwin)
	NOERLCMD = gcc c_src/noerl.c c_src/mdb.c c_src/midl.c c_src/lz4.c  -D_TESTAPP_=1 -DMDB_MAXKEYSIZE=0 -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0 -g -o noerl
else
	NOERLCMD = gcc c_src/noerl.c c_src/mdb.c c_src/midl.c c_src/lz4.c  -D_TESTAPP_=1 -DMDB_MAXKEYSIZE=0 -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0 -g -lpthread -ldl -o noerl
endif

all:
	../../rebar compile
	$(TOOLCMD)

clean:
	../../rebar clean

eunit:
	../../rebar eunit

tool:
	$(TOOLCMD)

ne:
	$(NOERLCMD)

sim:
	gcc c_src/mdbsim.c c_src/mdb.c c_src/midl.c c_src/lz4.c  -g -DMDB_MAXKEYSIZE=0 -DSQLITE_DEBUG -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0  -o mdbsim

lldb:
	-rm *.db wal.*
	gcc c_src/test.c  -g -DSQLITE_DEBUG -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0  -o t && lldb t

valgrind:
	-rm *.db wal.*
	gcc c_src/test.c  -g -DSQLITE_DEBUG -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=0  -o t && valgrind --tool=memcheck --track-origins=yes --leak-check=full ./t
