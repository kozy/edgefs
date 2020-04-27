/*
 * Copyright (c) 2020-2020 Nexenta Systems, inc.
 *
 * This file is part of EdgeFS Project
 * (see https://github.com/Nexenta/edgefs).
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <stdio.h>
#include <errno.h>

#include "ccow.h"
#include "dqclient.h"
#include "geolock.h"
#include "glm_ops.h"

#ifdef USE_JE_MALLOC
#include <jemalloc/jemalloc.h>
#else
#define je_calloc       calloc
#define je_free         free
#endif

#define FSIO_GLM_LOCKS_DB	"fsio_locks.db"
#define TIMER_INTERVAL		5

#define MAX_CCOW_JSON_SIZE	16383
#define CCOW_CONFIG_FILE	"/opt/nedge/etc/ccow/ccow.json"

static char ccow_buf[MAX_CCOW_JSON_SIZE + 1];

static void
__prepare_lock_rec(char *ino_str, unsigned int oid_size,
		char *nhid, char *vmchid, uint64_t genid,
		uint32_t deleted, geo_lock_t *lock_rec)
{
	int err;

	strcpy(lock_rec->path, ino_str);
	if (nhid) {
		strncpy(lock_rec->nhid, nhid, sizeof lock_rec->nhid - 1);
	} else {
		lock_rec->nhid[0] = '\0';
	}
	if (vmchid) {
		strncpy(lock_rec->vmchid, vmchid, sizeof lock_rec->vmchid - 1);
	} else {
		lock_rec->vmchid[0] = '\0';
	}
	lock_rec->genid = genid;
	lock_rec->uvid = 0;
	lock_rec->segid = 0;
	lock_rec->deleted = deleted;
	lock_rec->size = oid_size;
}

static void
_inode_wait_for_glm_mutex(struct cdq_client *client, char *ino_str)
{
	int locked = 1, err;

	do {
		sleep(TIMER_INTERVAL);
		err = geolock_is_locked(client, ino_str, &locked);
		if (err)
			break;
	} while(locked);
}

static int
dqlite_glm_ilock(void *cl, pthread_mutex_t *mutex, char *ino_str,
		unsigned int oid_size, char *nhid, char *vmchid,
		uint64_t genid, uint32_t deleted, unsigned int *cached)
{
	int err, locked = 0;
	struct cdq_client *client = cl;
	geo_lock_t lock_rec;

	err = geolock_is_locked(client, ino_str, &locked);
	if (err != 0) {
		if (err == -ENOENT) {
			memset(&lock_rec, 0, sizeof lock_rec);
			/* Create lock record */
			__prepare_lock_rec(ino_str, oid_size, nhid, vmchid,
					genid, deleted, &lock_rec);

			pthread_mutex_lock(mutex);
			err = geolock_insert_lock_rec(client, &lock_rec);
			if (err == 0) {
				*cached = 1;
			}
			pthread_mutex_unlock(mutex);

		}
		return err;
	}

	if (locked) {
		_inode_wait_for_glm_mutex(client, ino_str);
	}

	err = geolock_lock(client, ino_str, genid);
	if (err == 0) {
		pthread_mutex_lock(mutex);
		*cached = 1;
		pthread_mutex_unlock(mutex);
	}
	return err;
}

static int
dqlite_glm_iunlock(void *cl, pthread_mutex_t *mutex, char *ino_str,
		uint64_t genid, unsigned int *cached)
{
	int err = 0;
	struct cdq_client *client = cl;

	err = geolock_unlock(client, ino_str, genid);
	if (err == 0) {
		pthread_mutex_lock(mutex);
		*cached = 0;
		pthread_mutex_unlock(mutex);
	}
	return err;
}


static int
dqlite_glm_acquire(void *cl, geo_lock_t *lock_rec)
{
	struct cdq_client *client = cl;
	return geolock_acquire_lock_rec(client, lock_rec);
}

static int
dqlite_glm_release(void *cl, geo_lock_t *lock_rec)
{
	struct cdq_client *client = cl;
	return geolock_release_lock_rec(client, lock_rec);
}

static int
dqlite_glm_clean(void *cl, geo_lock_t *lock_rec)
{
	struct cdq_client *client = cl;
	return geolock_clean_lock_rec(client, lock_rec);
}

static int
dqlite_glm_get(void *cl, const char *path_key, geo_lock_t *lock_rec)
{
	struct cdq_client *client = cl;
	int rec_count = 0;
	int err = geolock_get_lock_rec(client, path_key, lock_rec, &rec_count);
	if (err) {
		return err;
	}
	if (rec_count == 0) {
		return -ENOENT;
	}
	return 0;
}



static int
dqlite_glm_disconnect(void *cl)
{
	struct cdq_client *client = cl;

	cdq_stop(client);
	je_free(client);
	return 0;
}

static int
dqlite_glm_connect(char *jsonstr, void **cl)
{
	int err;
	uint64_t leader_id;
	char *leader_ip = NULL;
	struct cdq_client *client = NULL;

	if (cl == NULL)
		return -EINVAL;

	*cl = NULL;
	client = je_calloc(1, sizeof *client);
	if (client == NULL)
		return -ENOMEM;

	err = cdq_get_leader(jsonstr, &leader_id, &leader_ip);
	if (err != 0) {
		je_free(client);
		return err;
	}

	if (leader_ip == NULL)
		return -ENOENT;

	client->srv_id = leader_id;
	strcpy(client->srv_ipaddr, leader_ip);

	err = cdq_start(client);
	if (err) {
		je_free(client);
		return err;
	}

	err = cdq_db_open(client, FSIO_GLM_LOCKS_DB);
	if (err) {
		dqlite_glm_disconnect(client);
		return err;
	}
	err = geolock_create_lock_tbl(client);
	if (err) {
		dqlite_glm_disconnect(client);
	} else {
		*cl = (void *)client;
	}
	return err;
}

struct dqlite_glm_ops dqlite_glm_ops = {
	.connect = dqlite_glm_connect,
	.disconnect = dqlite_glm_disconnect,
	.lock = dqlite_glm_ilock,
	.unlock = dqlite_glm_iunlock,
	.acquire = dqlite_glm_acquire,
	.release = dqlite_glm_release,
	.clean = dqlite_glm_clean,
	.get = dqlite_glm_get
};
