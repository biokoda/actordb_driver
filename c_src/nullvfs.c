/*
** 2010 April 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************

** Null VFS. Because we want all data within MDB, we implement a custom VFS that does nothing.
** The only thing that is left is the base sqlite file at pgno 1.
** Pgno gets written with every write anyway so the base file is useless.
** It is cleaner to create this useless VFS than to mess with pager.c
** Sqlite will still read the base file when opened. So we return a default wal enabled empty
** page.
*/

#if !defined(SQLITE_TEST) || SQLITE_OS_UNIX

#include "sqlite3.h"

#include <assert.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>

/*
** The maximum pathname length supported by this VFS.
*/
#define MAXPATHNAME 512

/*
** When using this VFS, the sqlite3_file* handles that SQLite uses are
** actually pointers to instances of type NullFile.
*/
typedef struct NullFile NullFile;
struct NullFile {
  sqlite3_file base;              /* Base class. Must be first. */
  // i64 size;
  // u8 buf[SQLITE_DEFAULT_PAGE_SIZE];
  int lock;
};
sqlite3_vfs *sqlite3_nullvfs(void);

static int nullDirectWrite(
  NullFile *p,                    /* File handle */
  const void *zBuf,               /* Buffer containing data to write */
  int iAmt,                       /* Size of data to write in bytes */
  sqlite_int64 iOfst              /* File offset to write to */
){
  // printf("drwite offset=%lld, size=%d\n",iOfst,iAmt);

  // if ((iOfst + iAmt) > p->size)
  //   p->size += ((iOfst+iAmt)-p->size);
  // if ((iOfst + iAmt) <= sizeof(p->buf))
  //   memcpy(p->buf+iOfst,zBuf,iAmt);

  return SQLITE_OK;
}

static int nullFlushBuffer(NullFile *p){
  // printf("flush\n");
  return SQLITE_OK;
}

static int nullClose(sqlite3_file *pFile){
  // printf("close\n");
  return SQLITE_OK;
}

/*
** Read data from a file.
*/
static int nullRead(
  sqlite3_file *pFile,
  void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  // NullFile *p = (NullFile*)pFile;

  // printf("read bytes=%d offset=%lld\n",iAmt,iOfst);

  if ((iOfst + iAmt) > SQLITE_DEFAULT_PAGE_SIZE)
    return SQLITE_IOERR_SHORT_READ;

  if (iOfst == 0 && iAmt >= 100)
  {
      u8 *page = zBuf;
      memset(page,0,iAmt);
      strcpy((char*)page,"SQLite format 3");
      page[16] = ((SQLITE_DEFAULT_PAGE_SIZE >> 8) & 255);
      page[17] = SQLITE_DEFAULT_PAGE_SIZE & 255;
      page[18] = 2;
      page[19] = 2;
      page[21] = 64;
      page[22] = 32;
      page[23] = 32;
      page[27] = 1;
      page[31] = 1;
      page[95] = 1;
      page[97] = 45;
      page[98] = 230;
      page[99] = 10;

      if (iAmt > 100)
      {
          page[100] = 13;
          page[105] = 16;
      }
  }
  else
  {
      u8 page[SQLITE_DEFAULT_PAGE_SIZE];
      // Code to print values of first page (its mostly empty).
      // This way we can simply hardcode the only part of file that gets called to vfs for.
      // lists:foldl(fun(X,C) -> case X > 0 of true -> io:format("~p: ~p, ~n",[C,X]); false -> ok end,C+1 end,0,binary_to_list(B)).
      memset(page,0,sizeof(page));
      strcpy((char*)page,"SQLite format 3");
      page[16] = ((SQLITE_DEFAULT_PAGE_SIZE >> 8) & 255);
      page[17] = SQLITE_DEFAULT_PAGE_SIZE & 255;
      page[18] = 2;
      page[19] = 2;
      page[21] = 64;
      page[22] = 32;
      page[23] = 32;
      page[27] = 1;
      page[31] = 1;
      page[95] = 1;
      page[97] = 45;
      page[98] = 230;
      page[99] = 10;
      page[100] = 13;
      page[105] = 16;

      memcpy(zBuf,page+iOfst,iAmt);
  }

  return SQLITE_OK;
}

/*
** Write data to a crash-file.
*/
static int nullWrite(
  sqlite3_file *pFile,
  const void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
){
  NullFile *p = (NullFile*)pFile;
  return nullDirectWrite(p,zBuf,iAmt,iOfst);
}

/*
** Truncate a file. This is a no-op for this VFS (see header comments at
** the top of the file).
*/
static int nullTruncate(sqlite3_file *pFile, sqlite_int64 size){
  // printf("truncate\n");
  return SQLITE_OK;
}

/*
** Sync the contents of the file to the persistent media.
*/
static int nullSync(sqlite3_file *pFile, int flags){
  // printf("nullsync\n");
  return SQLITE_OK;
}

/*
** Write the size of the file in bytes to *pSize.
*/
static int nullFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  NullFile *p = (NullFile*)pFile;
  // *pSize = p->size;
  *pSize = SQLITE_DEFAULT_PAGE_SIZE;

  return SQLITE_OK;
}

static int nullLock(sqlite3_file *pFile, int eLock){
  // printf("lock\n");
  NullFile *p = (NullFile*)pFile;
  if (p->lock < eLock)
    p->lock = eLock;
  return SQLITE_OK;
}
static int nullUnlock(sqlite3_file *pFile, int eLock){
  // printf("unlock\n");
  NullFile *p = (NullFile*)pFile;
  if (p->lock > eLock)
    p->lock = eLock;
  return SQLITE_OK;
}
static int nullCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  *pResOut = 0;
  // printf("lock\n");
  return SQLITE_OK;
}

static int nullFileControl(sqlite3_file *pFile, int op, void *pArg){
    // printf("FILECONTROL %d\n",op);
  // if (op == SQLITE_FCNTL_SIZE_HINT)
  // {
  //     printf("SIZEHINT %lld\n",*(i64*)pArg);
  // }
  NullFile *p = (NullFile*)pFile;
  switch( op ){
  case SQLITE_FCNTL_WAL_BLOCK: {
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_LOCKSTATE: {
    *(int*)pArg = p->lock;
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_LAST_ERRNO: {
    *(int*)pArg = 0;
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_CHUNK_SIZE: {
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_SIZE_HINT: {
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_PERSIST_WAL: {
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_POWERSAFE_OVERWRITE: {
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_VFSNAME: {
    *(char**)pArg = "123";
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_TEMPFILENAME: {
      *(char**)pArg = "123";
    return SQLITE_OK;
  }
  case SQLITE_FCNTL_HAS_MOVED: {
    *(int*)pArg = 0;
    return SQLITE_OK;
  }
#if SQLITE_MAX_MMAP_SIZE>0
  case SQLITE_FCNTL_MMAP_SIZE: {
    return SQLITE_OK;
  }
#endif
#if SQLITE_ENABLE_LOCKING_STYLE && defined(__APPLE__)
  case SQLITE_FCNTL_SET_LOCKPROXYFILE:
  case SQLITE_FCNTL_GET_LOCKPROXYFILE: {
    return SQLITE_OK;
  }
#endif /* SQLITE_ENABLE_LOCKING_STYLE && defined(__APPLE__) */
}
return SQLITE_NOTFOUND;
}

static int nullSectorSize(sqlite3_file *pFile){
  // printf("nullSectorSize\n");
  return SQLITE_DEFAULT_SECTOR_SIZE;
}
static int nullDeviceCharacteristics(sqlite3_file *pFile){
  // printf("devchar\n");
  return 0;
}

static int nullShmMap(
  sqlite3_file *fd,               /* Handle open on database file */
  int iRegion,                    /* Region to retrieve */
  int szRegion,                   /* Size of regions */
  int bExtend,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Mapped memory */
){ return SQLITE_OK; }

static int nullShmLock(
  sqlite3_file *fd,          /* Database file holding the shared memory */
  int ofst,                  /* First lock to acquire or release */
  int n,                     /* Number of locks to acquire or release */
  int flags                  /* What to do with the lock */
){ return SQLITE_OK; }

static void nullShmBarrier(
  sqlite3_file *fd                /* Database file holding the shared memory */
){}

static int nullShmUnmap(
  sqlite3_file *fd,               /* The underlying database file */
  int deleteFlag                  /* Delete shared-memory if true */
){return SQLITE_OK;}

static void nullUnmapfile(unixFile *pFd){}

static int nullFetch(sqlite3_file *fd, i64 iOff, int nAmt, void **pp){ return SQLITE_OK; }

static int nullUnfetch(sqlite3_file *fd, i64 iOff, void *p){ return SQLITE_OK; }


static int nullOpen(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zName,              /* File to open, or 0 for a temp file */
  sqlite3_file *pFile,            /* Pointer to NullFile struct to populate */
  int flags,                      /* Input SQLITE_OPEN_XXX flags */
  int *pOutFlags                  /* Output SQLITE_OPEN_XXX flags (or NULL) */
){
    int openflags = 0;
  static const sqlite3_io_methods nullio = {
    3,                            /* iVersion */
    nullClose,                    /* xClose */
    nullRead,                     /* xRead */
    nullWrite,                    /* xWrite */
    nullTruncate,                 /* xTruncate */
    nullSync,                     /* xSync */
    nullFileSize,                 /* xFileSize */
    nullLock,                     /* xLock */
    nullUnlock,                   /* xUnlock */
    nullCheckReservedLock,        /* xCheckReservedLock */
    nullFileControl,              /* xFileControl */
    nullSectorSize,               /* xSectorSize */
    nullDeviceCharacteristics,     /* xDeviceCharacteristics */
    nullShmMap,                  /* xShmMap */
    nullShmLock,                /* xShmLock */
    nullShmBarrier,             /* xShmBarrier */
    nullShmUnmap,               /* xShmUnmap */
    nullFetch,                  /* xFetch */
    nullUnfetch                 /* xUnfetch */
  };
  int isExclusive  = (flags & SQLITE_OPEN_EXCLUSIVE);
  int isDelete     = (flags & SQLITE_OPEN_DELETEONCLOSE);
  int isCreate     = (flags & SQLITE_OPEN_CREATE);
  int isReadonly   = (flags & SQLITE_OPEN_READONLY);
  int isReadWrite  = (flags & SQLITE_OPEN_READWRITE);
  int isAutoProxy  = (flags & SQLITE_OPEN_AUTOPROXY);

  NullFile *p = (NullFile*)pFile; /* Populate this structure */

  // printf("open %d\n", isExclusive);
  if( zName==0 ){
    return SQLITE_IOERR;
  }

  memset(p, 0, sizeof(NullFile));

  if( isReadonly )  openflags |= O_RDONLY;
  if( isReadWrite ) openflags |= O_RDWR;
  if( isCreate )    openflags |= O_CREAT;
  if( isExclusive ) openflags |= (O_EXCL|O_NOFOLLOW);
  openflags |= (O_LARGEFILE|O_BINARY);

  if( pOutFlags ){
    *pOutFlags = (openflags | O_LARGEFILE | O_BINARY);
  }
  p->base.pMethods = &nullio;
  // printf("OPENOK\n");
  return SQLITE_OK;
}

/*
** Delete the file identified by argument zPath. If the dirSync parameter
** is non-zero, then ensure the file-system modification to delete the
** file has been synced to disk before returning.
*/
static int nullDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  // printf("delete\n");
  return SQLITE_OK;
}

#ifndef F_OK
# define F_OK 0
#endif
#ifndef R_OK
# define R_OK 4
#endif
#ifndef W_OK
# define W_OK 2
#endif

/*
** Query the file-system to see if the named file exists, is readable or
** is both readable and writable.
*/
static int nullAccess(
  sqlite3_vfs *pVfs,
  const char *zPath,
  int flags,
  int *pResOut
){
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** Argument zPath points to a nul-terminated string containing a file path.
** If zPath is an absolute path, then it is copied as is into the output
** buffer. Otherwise, if it is a relative path, then the equivalent full
** path is written to the output buffer.
**
** This function assumes that paths are UNIX style. Specifically, that:
**
**   1. Path components are separated by a '/'. and
**   2. Full paths begin with a '/' character.
*/
static int nullFullPathname(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zPath,              /* Input path (possibly a relative path) */
  int nPathOut,                   /* Size of output buffer in bytes */
  char *zPathOut                  /* Pointer to output buffer */
){
  char zDir[MAXPATHNAME+1];
  zDir[0] = '\0';
  zDir[MAXPATHNAME] = '\0';
  // printf("path\n");

  sqlite3_snprintf(nPathOut, zPathOut, "%s/%s", zDir, zPath);
  zPathOut[nPathOut-1] = '\0';

  return SQLITE_OK;
}


/*
** This function returns a pointer to the VFS implemented in this file.
** To make the VFS available to SQLite:
**
**   sqlite3_vfs_register(sqlite3_nullvfs(), 0);
*/
sqlite3_vfs *sqlite3_nullvfs(void){
  static sqlite3_vfs nullvfs = {
    3,                            /* iVersion */
    sizeof(NullFile),             /* szOsFile */
    MAXPATHNAME,                  /* mxPathname */
    0,                            /* pNext */
    "null",                       /* zName */
    0,                            /* pAppData */
    nullOpen,                     /* xOpen */
    nullDelete,                   /* xDelete */
    nullAccess,                   /* xAccess */
    nullFullPathname,             /* xFullPathname */
    unixDlOpen,                   /* xDlOpen */
    unixDlError,                  /* xDlError */
    unixDlSym,                    /* xDlSym */
    unixDlClose,                  /* xDlClose */
    unixRandomness,               /* xRandomness */
    unixSleep,                    /* xSleep */
    unixCurrentTime,              /* xCurrentTime */
    unixGetLastError,     /* xGetLastError */               \
    unixCurrentTimeInt64, /* xCurrentTimeInt64 */           \
    unixSetSystemCall,    /* xSetSystemCall */              \
    unixGetSystemCall,    /* xGetSystemCall */              \
    unixNextSystemCall,   /* xNextSystemCall */             \
  };
  return &nullvfs;
}

#endif /* !defined(SQLITE_TEST) || SQLITE_OS_UNIX */
