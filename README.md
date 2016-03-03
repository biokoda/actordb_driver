ActorDB interface to SQLite and LMDB.

c_src/actordb_driver_nif.c - erlang interface

c_src/wal.c - interface between SQLite and LMDB. Identical API to original SQLite WAL implementation. 

c_src/mdb.c - LMDB

c_src/sqlite3.c - SQLite amalgamation code without wal.c

c_src/queue.c - old work queue. 

c_src/lfqueue.c - lock free queue used atm.

c_src/wbuf.c - another lock free experiment that is not used. A lock free write buffer.

c_src/tool.c - actordb_tool application.

c_src/nullvfs.c - an SQLite VFS that does nothing because we don't need sqlite files. Everything is in LMDB through the WAL API.

c_src/lz4.c - LZ4 compression. Every SQLite page stored in LMDB is compressed using LZ4.

c_src/noerl.c - Erlang-less app that uses our SQLite+LMDB engine. Used for easy profiling and debugging.

