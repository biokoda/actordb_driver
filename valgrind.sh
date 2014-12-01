gcc c_src/test.c -g -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_THREADSAFE=0  -o t && valgrind --tool=memcheck ./t
