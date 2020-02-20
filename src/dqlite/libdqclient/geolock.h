#ifndef __GEOLOCK_H__
#define __GEOLOCK_H__

#include "dqclient.h"

/* Global locking structure */
typedef struct geo_lock {
	char		path[2048];
	uint64_t	genid;
	uint64_t	uvid;
	uint32_t	deleted;
	char		nhid[512];
	char		vmchid[512];
	uint64_t	segid;
	unsigned int	size;
	int64_t		lock_time;
	int		lock_state;
} geo_lock_t;

/* Function declarations */
int geolock_create_lock_tbl(struct cdq_client *client);
int geolock_get_lock_rec(struct cdq_client *client, const char *path_key,
		geo_lock_t *lock_rec, int *rec_count);
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
