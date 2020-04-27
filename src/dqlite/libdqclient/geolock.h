#ifndef __GEOLOCK_H__
#define __GEOLOCK_H__

#include "dqclient.h"
#include "glm_ops.h"

/* Function declarations */
int geolock_create_lock_tbl(struct cdq_client *client);

/* Acquire-release declarations */
int geolock_acquire_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec);
int geolock_release_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec);

int geolock_clean_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec);

int geolock_get_lock_rec(struct cdq_client *client, const char *path_key,
		geo_lock_t *lock_rec, int *rec_count);

int geolock_list_lock_rec(struct cdq_client *client, char *from, char *lock);

int geolock_insert_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec);

int geolock_is_locked(struct cdq_client *client, const char *path_key,
		int *is_locked);
int geolock_lock(struct cdq_client *client, const char *path_key,
		uint64_t genid);
int geolock_unlock(struct cdq_client *client, const char *path_key,
		uint64_t genid);


/* only for testing */
int test_geolock_delete_tbl(struct cdq_client *client);

#endif /* __GEOLOCK_H__ */
