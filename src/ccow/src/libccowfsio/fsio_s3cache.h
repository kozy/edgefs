#ifndef fsio_s3_cache_entry_h
#define fsio_s3_cache_entry_h

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hashtable.h"
#include "ccowfsio.h"

#define FSIO_S3_CACHE_TTL 120

typedef struct fsio_s3_cache_entry_t {
	char *name;
    inode_t parent;
    inode_t child;
    uint64_t created;
    uint512_t vmchid;
} fsio_s3_cache_entry_t;

typedef struct fsio_s3_cache_t {
    hashtable_t *fsio_s3_cache_entry_ht;
    pthread_mutex_t fsio_s3_cache_entry_ht_lock;
    uint64_t last_clean;
} fsio_s3_cache_t;



// fsio_s3_cache_entry_t methods
int fsio_s3_cache_entry_init(fsio_s3_cache_entry_t *fsio_s3_cache_entry, inode_t parent, inode_t child,
    char *name, uint512_t *vmchid);
void fsio_s3_cache_entry_destroy(fsio_s3_cache_entry_t *fsio_s3_cache_entry);

int fsio_s3_cache_entry_age(fsio_s3_cache_entry_t *fsio_s3_cache_entry);
int fsio_s3_cache_entry_expired(fsio_s3_cache_entry_t *fsio_s3_cache_entry);

// fsio_s3_cache_entry_t hash table methods
char *build_s3_key(inode_t parent, char *name, char *buf);


int fsio_s3_cache_create(fsio_s3_cache_t *fsio_s3_cache);
int fsio_s3_cache_destroy(fsio_s3_cache_t *fsio_s3_cache);
int fsio_s3_cache_clean(fsio_s3_cache_t *fsio_s3_cache);


int fsio_s3_cache_put(fsio_s3_cache_t *fsio_s3_cache, fsio_s3_cache_entry_t *fsio_s3_cache_entry);
int fsio_s3_cache_get(fsio_s3_cache_t *fsio_s3_cache, char *key, inode_t *res_ino);
void fsio_s3_cache_delete(fsio_s3_cache_t *fsio_s3_cache, char *key);


#ifdef __cplusplus
}
#endif

#endif
