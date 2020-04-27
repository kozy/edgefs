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

#include <dlfcn.h>
#include <ctype.h>
#include <string.h>

#include "ccow.h"
#include "fsio_inode.h"
#include "fsio_system.h"
#include "glm_ops.h"
#include "tc_pool.h"

#define FSIO_GLM_LOCKS_DB	"fsio_locks.db"
#define TIMER_INTERVAL		5
#define ACQUIRE_TIMEOUT		20
#define RECONNECT_TIMEOUT	40

#define MAX_CCOW_JSON_SIZE	16383
#define CCOW_CONFIG_FILE	"/opt/nedge/etc/ccow/ccow.json"

static char ccow_buf[MAX_CCOW_JSON_SIZE + 1];

// Acquire/release/get global methods
static char *
str_geo_lock(geo_lock_t *l, char *buf) {
	sprintf(buf,"%s\n genid: %lu\n uvid: %lu\n deleted: %u\n,"
	" nhid: %s\n vmchid: %s\n segid: %lX\n serverid: %lX\n size: %lu\n lock_time: %ld\n lock_state: %d",
		l->path, l->genid, l->uvid, l->deleted,
		l->nhid, l->vmchid, l->segid, l->serverid, l->size, l->lock_time, l->lock_state);
	return buf;
}

static char *
oid_from_path(char *path) {
	if (path == NULL)
		return NULL;
	char *p = strchr(path,'/');
	if (p == NULL)
		return NULL;
	p = strchr(p + 1,'/');
	if (p == NULL)
		return NULL;
	p = strchr(p + 1,'/');
	if (p == NULL)
		return NULL;
	return (p + 1);
}

static int
get_local_headers(ci_t *ci, inode_t inode, geo_lock_t *lvm) {
	char *oid;
	ccow_t tc;
    int err;

	oid = oid_from_path(lvm->path);
	if (oid == NULL) {
		log_error(fsio_lg, "Invalid path");
		return -EINVAL;
	}

	/* Get tenant context */
	err = tc_pool_get_tc(ci->tc_pool_handle, inode, &tc);
	if (err) {
		log_error(fsio_lg, "Failed to get TC. err: %d", err);
		return err;
	}

	ccow_completion_t c;
	int cont_flags = 0;
	lvm->genid = 0;

	ccow_lookup_t iter;

	err = ccow_create_stream_completion(tc, NULL, NULL, 1, &c,
		ci->bid, ci->bid_size,
        oid, strlen(oid) + 1,
		&lvm->genid, &cont_flags, &iter);
	if (err) {
		log_trace(fsio_lg, "Failed to create stream completion err: %d", err);
		return err;
	}

	lvm->uvid = 0;
	lvm->size = 0;
	lvm->deleted = 0;

	if (cont_flags != CCOW_CONT_F_EXIST) {
		lvm->deleted = 1;
		uint512_dump(ccow_get_vm_content_hash_id(c), lvm->vmchid, UINT512_BYTES*2+1);
		uint512_dump(ccow_get_vm_name_hash_id(c), lvm->nhid, UINT512_BYTES*2+1);
	}

	struct ccow_metadata_kv *kv;
	while ((kv = ccow_lookup_iter(iter, CCOW_MDTYPE_METADATA, -1)) != NULL) {
		if (strcmp(kv->key, RT_SYSKEY_UVID_TIMESTAMP) == 0) {
			lvm->uvid = (uint64_t) ccow_kvconvert_to_int64(kv);
			continue;
		}

		if (strcmp(kv->key, RT_SYSKEY_TX_GENERATION_ID) == 0) {
			lvm->genid = (uint64_t) ccow_kvconvert_to_int64(kv);
			continue;
		}

		if (strcmp(kv->key, RT_SYSKEY_OBJECT_DELETED) == 0) {
			lvm->deleted = (uint64_t) ccow_kvconvert_to_int64(kv);
			continue;
		}

		if (strcmp(kv->key, RT_SYSKEY_LOGICAL_SIZE) == 0) {
			lvm->size = (uint64_t) ccow_kvconvert_to_int64(kv);
			continue;
		}

		if (strcmp(kv->key, RT_SYSKEY_NAME_HASH_ID) == 0) {
			uint512_dump((uint512_t *)kv->value, lvm->nhid, UINT512_BYTES*2+1);
			continue;
		}

		if (strcmp(kv->key, RT_SYSKEY_VM_CONTENT_HASH_ID) == 0) {
			uint512_dump((uint512_t *)kv->value, lvm->vmchid, UINT512_BYTES*2+1);
			continue;
		}
	}

	lvm->segid = ci->segid;
	lvm->serverid = ci->serverid;

	ccow_cancel(c);
	return 0;
}

static int
prefetch_global_vm(ci_t *ci, inode_t inode, geo_lock_t *gvm) {
	char *oid;
	ccow_t tc;
	int err;

	oid = oid_from_path(gvm->path);
	if (oid == NULL) {
		log_error(fsio_lg, "Invalid path");
		return -EINVAL;
	}

	/* Get tenant context */
	err = tc_pool_get_tc(ci->tc_pool_handle, inode, &tc);
	if (err) {
		log_error(fsio_lg, "Failed to get TC. err: %d", err);
		return err;
	}

	ccow_completion_t c;
	err = ccow_create_completion(tc, NULL, NULL, 2, &c);
	if (err) {
		log_error(fsio_lg, "Failed to create completion err: %d", err);
		return err;
	}

	set_isgw_bid(c, ci->bid);

	uint512_t vmchid;
	uint512_t nhid;

	uint512_fromhex(gvm->vmchid, (UINT512_BYTES * 2 + 1), &vmchid);
	uint512_fromhex(gvm->nhid, (UINT512_BYTES * 2 + 1), &nhid);

	err = ccow_vm_prefetch(tc, &vmchid, &nhid,
		ci->tid, ci->tid_size,
		ci->bid, ci->bid_size,
        oid, strlen(oid) + 1,
		gvm->segid, c);
	if (err) {
		ccow_drop(c);
		log_error(fsio_lg, "Prefetch error: %d", err);
		return err;
	}

	err = ccow_wait(c, 0);
	if (err) {
		log_error(fsio_lg, "Prefetch wait error: %d", err);
		return err;
	}

	return err;
}

static int
global_inode_delete(ci_t *ci, inode_t inode, char *oid)  {
	ccow_t tc;
	int err;

	/* Get tenant context */
	err = tc_pool_get_tc(ci->tc_pool_handle, inode, &tc);
	if (err) {
		log_error(fsio_lg, "Failed to get TC. err: %d", err);
		return err;
	}

	ccow_completion_t c;
	err = ccow_create_completion(tc, NULL, NULL, 2, &c);
	if (err) {
		log_error(fsio_lg, "Failed to create completion err: %d", err);
		return err;
	}

	err = ccow_delete_versioning(ci->bid, ci->bid_size, oid, strlen(oid) + 1, c);
	if (err) {
		ccow_drop(c);
		log_error(fsio_lg, "Delete error: %d", err);
		return err;
	}

	err = ccow_wait(c, 0);
	if (err) {
		log_error(fsio_lg, "Delete versioning wait error: %d", err);
	}
	return err;
}

static int
global_inode_prepare(ci_t *ci, inode_t inode, geo_lock_t *gvm) {
	int err;
	char *oid;
	char buf[4096];
	geo_lock_t lvm;

	if (gvm->genid == 0) {
		log_debug(fsio_lg, "Gvm not available, use local: %s", gvm->path);
		return 0;
	}

	oid = oid_from_path(gvm->path);
	if (oid == NULL) {
		log_error(fsio_lg, "Invalid path");
		return -EINVAL;
	}

	memset(&lvm, 0, sizeof lvm);
	strcpy(lvm.path, gvm->path);
	err = get_local_headers(ci, inode, &lvm);

	log_debug(fsio_lg, "Local vm get err: %d, path: %s", err, lvm.path);
	log_debug(fsio_lg, "Local vm: %s", str_geo_lock(&lvm, buf));

	int prep = 0;
	if (err == -ENOENT || lvm.genid == 0 || lvm.genid < gvm->genid ||
		(lvm.genid == gvm->genid && lvm.uvid < gvm->uvid)) {
		log_debug(fsio_lg, "Prefetch global vm: %s", str_geo_lock(gvm, buf));
		prep = 1;
		err = prefetch_global_vm(ci, inode, gvm);
		if (err) {
			log_error(fsio_lg, "Prefetch vm err: %d, path: %s", err, gvm->path);
			return err;
		}
	}

	if (gvm->deleted > 0 && lvm.deleted == 0 && lvm.genid > 0) {
		prep = 2;
		log_debug(fsio_lg, "Delete global vm: %s genid: %lu", gvm->path, gvm->genid);
		err = global_inode_delete(ci, inode, oid);
		if (err) {
			log_error(fsio_lg, "Inode delete err: %d, path: %s", err, gvm->path);
		}
	}

	log_debug(fsio_lg, "Prepere global vm: %s prep: %d", gvm->path, prep);

	return 0;
}

static int
global_reconnect(ci_t *ci, int idx) {
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	int err = 0;
	int wait = 0;
	while (wait < RECONNECT_TIMEOUT) {
		uint64_t age = (time(NULL) - ci->glm_connect_time[idx]);
		if (age < 15) {
			return 0;
		}
		log_info(fsio_lg, "glm reconnecting index: %d", idx);
		glm_ops->disconnect(ci->glm_client[idx]);
		err = glm_ops->connect(ci->json_buf, &ci->glm_client[idx]);
		if (err) {
			log_error(fsio_lg, "glm reconnecting error: %d", err);
		} else {
			ci->glm_connect_time[idx] = time(NULL);
			return 0;
		}
		sleep(5);
		wait += 5;
	}
	return err;
}

static int
get_glm(ci_t *ci) {
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	int ifree = -1;
	while (ifree < 0) {
		pthread_mutex_lock(&ci->glm_mutex);
		for (int i=0; i < ci->glm_pool_size; i++) {
			if (ci->glm_use[i] == 0) {
				ci->glm_use[i] = 1;
				ifree = i;
				break;
			}
		}
		// Grow pool
		if (ifree < 0 && ci->glm_pool_size < DQLITE_MAX_POOL_SIZE) {
			log_debug(fsio_lg, "Grow dqlite pool [%d]", ci->glm_pool_size);
			int err = glm_ops->connect(ci->json_buf, &ci->glm_client[ci->glm_pool_size]);
			if (err) {
				log_error(fsio_lg, "Unable to connect to dqlite glm[%d], err: %d",
						ci->glm_pool_size, err);
			} else {
				ci->glm_connect_time[ci->glm_pool_size] = time(NULL);
				ifree = ci->glm_pool_size;
				ci->glm_pool_size++;
			}
		}
		pthread_mutex_unlock(&ci->glm_mutex);
		if (ifree < 0) {
			usleep(100000);
		}
	}
	ci->glm_use_counter++;
	log_debug(fsio_lg, "Glm pool size: %d, use count: %d", ci->glm_pool_size, ci->glm_use_counter);
	return ifree;
}

static void
put_glm(ci_t *ci, int index) {
	pthread_mutex_lock(&ci->glm_mutex);
	ci->glm_use[index] = 0;
	ci->glm_use_counter--;
	pthread_mutex_unlock(&ci->glm_mutex);
}

static int
gvm_get(ci_t *ci, char *path, geo_lock_t *gvm) {
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	int idx = get_glm(ci);
	int err = glm_ops->get(ci->glm_client[idx], path, gvm);
	if (err) {
		log_debug(fsio_lg, "Glm get %s error : %d", path, err);
	}
	if (err == -EINVAL || err == -EIO) {
		err = global_reconnect(ci, idx);
		if (!err) {
			err = glm_ops->get(ci->glm_client[idx], path, gvm);
		}
	}
	put_glm(ci, idx);
	return err;
}

static int
gvm_acquire(ci_t *ci, geo_lock_t *gvm) {
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	gvm->segid = ci->segid;
	gvm->serverid = ci->serverid;
	int idx = get_glm(ci);
	int err = glm_ops->acquire(ci->glm_client[idx], gvm);
	if (err) {
		log_debug(fsio_lg, "Glm acquire %s error : %d", gvm->path, err);
	}
	if (err == -EINVAL || err == -EIO) {
		err = global_reconnect(ci, idx);
		if (!err) {
			err = glm_ops->acquire(ci->glm_client[idx], gvm);
		}
	}
	put_glm(ci, idx);
	return err;
}

static int
gvm_release(ci_t *ci, geo_lock_t *gvm) {
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	gvm->segid = ci->segid;
	gvm->serverid = ci->serverid;
	int idx = get_glm(ci);
	int err = glm_ops->release(ci->glm_client[idx], gvm);
	if (err) {
		log_debug(fsio_lg, "Glm release %s error : %d", gvm->path, err);
	}
	if (err == -EINVAL || err == -EIO) {
		err = global_reconnect(ci, idx);
		if (!err) {
			err = glm_ops->release(ci->glm_client[idx], gvm);
		}
	}
	put_glm(ci, idx);
	return err;
}


int
global_inode_get(ci_t *ci, inode_t inode, char *oid) {
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	geo_lock_t gvm;
	int err;
	char buf[4096];

	/* Make sure geo locking is enabled */
	if (!ci->glm_enabled) {
		return 0;
	}

	memset(&gvm, 0, sizeof gvm);
	sprintf(gvm.path,"%s/%s/%s/%s", ci->cid, ci->tid, ci->bid, oid);

	err = gvm_get(ci, gvm.path, &gvm);
	if (err) {
		return err;
	}

	log_debug(fsio_lg, "Global get vm: %s", str_geo_lock(&gvm, buf));

	err = global_inode_prepare(ci, inode, &gvm);
	if (err) {
		log_error(fsio_lg, "Prepare vm failed err: %d, path: %s", err, gvm.path);
	}
	return err;
}

// Acquire global VM record lock
int
global_inode_acquire(ci_t *ci, inode_t inode, char *oid) {
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	geo_lock_t gvm;
	int err;
	char buf[4096];

	/* Make sure geo locking is enabled */
	if (!ci->glm_enabled) {
		return 0;
	}

	memset(&gvm, 0, sizeof gvm);
	sprintf(gvm.path,"%s/%s/%s/%s", ci->cid, ci->tid, ci->bid, oid);

	int wait = 0;
	while (wait < ACQUIRE_TIMEOUT) {
		err = gvm_acquire(ci, &gvm);
		if (!err)
			break;
		sleep(1);
		wait += 1;
		log_debug(fsio_lg, "Retry acquire %s err: %d", gvm.path, err);
	}

	if (err) {
		log_warn(fsio_lg, "Acquire global lock failed err: %d, path: %s", err, gvm.path);
		return err;
	}

	log_debug(fsio_lg, "Global acquire %s, vm: %s", gvm.path, str_geo_lock(&gvm, buf));

	err = global_inode_prepare(ci, inode, &gvm);
	if (err) {
		log_error(fsio_lg, "Prepare vm failed err: %d, path: %s", err, gvm.path);
	}
	return err;
}

// Release global VM record lock
int
global_inode_release(ci_t *ci, inode_t inode, char *oid) {
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	geo_lock_t old;
	geo_lock_t gvm;
	int err;
	char buf[4096];

	/* Make sure geo locking is enabled */
	if (!ci->glm_enabled) {
		return 0;
	}

	memset(&old, 0, sizeof old);
	sprintf(old.path,"%s/%s/%s/%s", ci->cid, ci->tid, ci->bid, oid);

	memset(&gvm, 0, sizeof gvm);
	strcpy(gvm.path, old.path);

	err = gvm_get(ci, old.path, &old);
	if (err) {
		log_error(fsio_lg, "Hunging lock: obtain global vm record failed err: %d, path: %s", err, old.path);
		return err;
	}

	if (old.lock_state == 0) {
		log_debug(fsio_lg, "Lock already released path: %s", old.path);
		return 0;
	}

	err = get_local_headers(ci, inode, &gvm);
	log_debug(fsio_lg, "Global release local headers: %s", str_geo_lock(&gvm, buf));
	if (err) {
		log_error(fsio_lg, "Get local headers error: %d, path: %s", err, gvm.path);
		memcpy(&gvm, &old, sizeof gvm);
	} else if (gvm.deleted > 0) {
		memcpy(&gvm, &old, sizeof gvm);
		gvm.deleted = 1;
	}

	err = gvm_release(ci, &gvm);
	log_debug(fsio_lg, "Global release %s, err: %d, vm: %s", gvm.path, err, str_geo_lock(&gvm, buf));
	if (err) {
		log_error(fsio_lg, "Hunging lock: release global lock failed err: %d, path: %s", err, gvm.path);
	}

	return err;
}




// Lock/unlock methods
int
ccow_fsio_inode_glm_lock(ccowfs_inode * inode)
{
	int err, locked = 0;
	struct cdq_client *client = inode->ci->glm_client[0];
	struct dqlite_glm_ops *glm_ops = inode->ci->glm_ops;
	uint512_t nhid;
	char nhid_str[UINT512_STR_BYTES + 1],
		vmhid_str[UINT512_STR_BYTES + 1];

	/* Make sure geo locking is enabled */
	if (!inode->ci->glm_enabled) {
		return 0;
	}

	/* Check if GLM is locally cached */
	pthread_mutex_lock(&inode->glm_mutex);
	if (inode->glm_cached) {
		assert(inode->glm_ref_count >= 1);
		/* GLM is cached. Increment ref count */
		/* Then use regular lock */
		inode->glm_ref_count++;
		pthread_mutex_unlock(&inode->glm_mutex);
		return 0;
	}
	pthread_mutex_unlock(&inode->glm_mutex);

	err = ccow_calc_nhid(inode->ci->cid, inode->ci->tid,
			inode->ci->bid, inode->oid, &nhid);

	/* If we cannot calculate nhid, set it to NULL */
	if (err)
		nhid = uint512_null;

	uint512_dump(&nhid, nhid_str, UINT512_STR_BYTES);
	uint512_dump(&inode->vmchid, vmhid_str, UINT512_STR_BYTES);

	err = glm_ops->lock(inode->ci->glm_client[0], &inode->glm_mutex,
			inode->ino_str, inode->oid_size, nhid_str,
			vmhid_str, inode->genid,
			inode->deleted, &inode->glm_cached);
	if (err == 0) {
		assert(inode->glm_cached == 1);
		pthread_mutex_lock(&inode->glm_mutex);
		inode->glm_ref_count++;
		pthread_mutex_unlock(&inode->glm_mutex);
	}
	return err;
}

int
ccow_fsio_inode_glm_unlock(ccowfs_inode * inode)
{
	int err = 0;
	struct dqlite_glm_ops *glm_ops;

	/* Make sure geo locking is enabled */
	if (!inode->ci->glm_enabled) {
		return 0;
	}

	glm_ops = inode->ci->glm_ops;
	/* Regular lock should be released. Then release glm lock */
	pthread_mutex_lock(&inode->glm_mutex);
	assert(inode->glm_cached == 1);
	assert(inode->glm_ref_count >= 1);

	inode->glm_ref_count--;
	if (inode->glm_ref_count == 0) {
		pthread_mutex_unlock(&inode->glm_mutex);
		/* If there is an error, we assume that we still have lock */
		err = glm_ops->unlock(inode->ci->glm_client[0],
				&inode->glm_mutex,
				inode->ino_str, inode->genid,
				&inode->glm_cached);
	} else {
		pthread_mutex_unlock(&inode->glm_mutex);
	}
	return err;
}


static int
_is_service_valid(ccow_t tc, char *service_name)
{
	int err, found = 0;
	ccow_lookup_t list;
	struct ccow_metadata_kv *kv;

	err = ccow_bucket_lookup(tc, service_name,
				 strlen(service_name) + 1, 1, &list);
	if (err)
		return 0;

	while ((kv = (struct ccow_metadata_kv *)ccow_lookup_iter(list,
                CCOW_MDTYPE_NAME_INDEX, -1))) {
		if (!kv->key_size)
			continue;
		found = 1;
		break;
	}
	ccow_lookup_release(list);
	return found;
}

static void
_add_node_to_json_buf(char *json_buf, char *id, char *ip_port)
{
	sprintf(json_buf, "%s\t\t{\n\t\t\t\"id\": "
			"%s,\n", json_buf, id);
	sprintf(json_buf, "%s\t\t\t\"ipaddr_port\": \"%s\"\n",
			json_buf, ip_port);
	sprintf(json_buf, "%s\t\t},\n", json_buf);
}

static int
_get_glm_cluster_ips(ccow_lookup_t iter, char *json_buf)
{
	int err = 0, pos = 0;
	struct ccow_metadata_kv *kv = NULL;
	char attr[512] = "", *tmp;

	if (json_buf == NULL)
		return -EINVAL;

	sprintf(json_buf, "{\n\t\"nodes\": [\n");
	while ((kv = ccow_lookup_iter(iter, CCOW_MDTYPE_NAME_INDEX, pos++))) {
		if (kv->type == CCOW_KVTYPE_RAW && kv->key) {
			strncpy(attr, kv->key, kv->key_size);
			if (attr[0] != 0) {
				char *id, *addr, *bpath;

				id = &attr[0];
				tmp = strchr(attr, ',');
				bpath = tmp + 1;
				*tmp = '\0';

				tmp = strchr(bpath, '@');
				addr = tmp + 1;
				*tmp = '\0';

				_add_node_to_json_buf(json_buf, id, addr);
			} else {
				err = -EINVAL;
			}
		}
		attr[0] = '\0';
	}
	sprintf(json_buf, "%s\t]\n}", json_buf);
	/* Remove ',' after the last item */
	tmp = strrchr(json_buf, ',');
	*tmp = ' ';
	return err;
}

static int
_verify_glm_procs()
{
	/* TODO: Verify dqlite process[es] is[are] running */
	return 0;
}

static int
_glm_get_locknodes(char *service_name, char *ccow_config_file, char *jsonstr)
{
	int err, ccow_fd;
	ccow_t tc;
	ccow_completion_t c;
	ccow_lookup_t iter = NULL;
	struct iovec iov = { .iov_base = "", .iov_len = 1 };

	if (service_name == NULL || ccow_config_file == NULL ||
			jsonstr == NULL)
		return -EINVAL;

	ccow_fd = open(ccow_config_file, O_RDONLY);
	if (ccow_fd < 0) {
		log_error(fsio_lg, "Unable to open config file: %s",
				ccow_config_file);
		return -EIO;
	}

	err = read(ccow_fd, ccow_buf, MAX_CCOW_JSON_SIZE);
	close(ccow_fd);
	if (err < 0) {
		log_error(fsio_lg, "Unable to read config file: %s",
				ccow_config_file);
		return -EIO;
	}

	err = ccow_admin_init(ccow_buf, "", 1, &tc);
	if (err != 0)
		return err;

	err = ccow_create_completion(tc, NULL, NULL, 1, &c);
	if (err != 0) {
		ccow_tenant_term(tc);
		return err;
	}

	err = ccow_admin_pseudo_get("", 1, "svcs", 5, service_name,
			strlen(service_name) + 1, "", 1, &iov,
			1, 20, CCOW_GET_LIST, c, &iter);
	if (err != 0) {
		ccow_tenant_term(tc);
		return err;
	}
	err = ccow_wait(c, -1);
	if (err != 0) {
		ccow_tenant_term(tc);
		return err;
	}
	err = _get_glm_cluster_ips(iter, jsonstr);
	if (err) {
		log_error(fsio_lg, "Could not find IP addresses of lock nodes");
		ccow_tenant_term(tc);
		return err;
	}
	ccow_tenant_term(tc);
	return _verify_glm_procs();
}


static int
_node_str_to_json(char *nodes, char *config_buf)
{
	char list[512];
	char *node, *id_str, *ip;
	int id, err;

	if (nodes == NULL)
		return -EINVAL;

	strcpy(list, nodes);

	sprintf(config_buf, "{\n\t\"nodes\": [\n");
	node = strtok(list, ";");
	while (node != NULL) {
		id_str = strchr(node, ',');
		ip = id_str + 1;
		*id_str = '\0';

		sprintf(config_buf, "%s\t\t{\n\t\t\t\"id\": %s,\n",
				config_buf, node);
		sprintf(config_buf, "%s\t\t\t\"ipaddr_port\": \"%s\"\n",
				config_buf, ip);
		node = strtok(NULL, ";");
		if (node != NULL)
			sprintf(config_buf, "%s\t\t},\n", config_buf);
		else
			sprintf(config_buf, "%s\t\t}\n", config_buf);
	}
	sprintf(config_buf, "%s\t]\n}", config_buf);
	return 0;
}

static int
is_ip_valid(char *ipport)
{
	char *ip;
	struct in_addr in_addr;
	int err;

	err = inet_aton(ipport, &in_addr);
	if (err) {
		log_error(fsio_lg, "Invalid IP-address: %s" "\n", ipport);
		return 0;
	}
	return 1;
}

static int
_is_nodes_str_valid(char *nodes)
{
	char list[512];
	char *node, *id_str, *ip;
	int id, err;

	if (nodes == NULL)
		return 0;

	strcpy(list, nodes);

	node = strtok(list, ";");
	while (node != NULL) {
		id_str = strchr(node, ',');
		if (id_str == NULL) {
			log_error(fsio_lg, "Expecting nodeid and IP address\n");
			return 0;
		}
		ip = id_str + 1;
		*id_str = '\0';
		err = sscanf(node, "%d", &id);
		if (err <= 0 || !is_ip_valid(ip)) {
			log_error(fsio_lg, "Invalid node id %d(%s) or IP-port %s\n",
					id, node, ip);
			return 0;
		}
		node = strtok(NULL, ";");
	}
	return 1;
}

static int
glm_init(ci_t *ci)
{
	int err;
	struct dqlite_glm_ops *glm_ops;
	void *dqclient_handle;

	err = pthread_mutex_init(&ci->glm_mutex, NULL);
	if (err) {
		log_error(fsio_lg, "glm pthread_mutex_init error %d\n", err);
		return -EAGAIN;
	}
	ci->glm_use_counter = 0;

	dqclient_handle = dlopen("libdqclient.so", RTLD_LAZY | RTLD_LOCAL);
	if (dqclient_handle == NULL) {
		log_error(fsio_lg, "Unable to load libdqclient.so");
		return -ENOENT;
	}
	glm_ops = dlsym(dqclient_handle, "dqlite_glm_ops");
	if (glm_ops == NULL) {
		log_error(fsio_lg, "Unable to find symbol dqlite_glm_ops");
		return -ENOENT;
	}

	int err_count = 0;
	ci->glm_pool_size = DQLITE_START_POOL_SIZE;
	for (int i=0; i<ci->glm_pool_size; i++) {
		err = glm_ops->connect(ci->json_buf, &ci->glm_client[i]);
		if (err) {
			log_error(fsio_lg, "Unable to connect to dqlite glm[%d], err: %d",
					i, err);
			ci->glm_use[i] = 1;
			err_count++;
		} else {
			ci->glm_connect_time[i] = time(NULL);
		}
	}
	if (err_count >= ci->glm_pool_size) {
		log_error(fsio_lg, "Unable to connect to dqlite glm, err_count: %d",
				err_count);
		ci->glm_enabled = 0;
		ci->glm_ops = NULL;
		dlclose(glm_ops);
	} else {
		log_info(fsio_lg, "Connected to dqlite glm, err_count: %d", err_count);
		ci->glm_enabled = 1;
		ci->glm_ops = glm_ops;
	}
	return err;
}


int
ccow_fsio_glm_init(char *service_name, char *ccow_config_file, ci_t *ci)
{
	int err;
	struct dqlite_glm_ops *glm_ops;
	void *dqclient_handle;

	err = _glm_get_locknodes(service_name, ccow_config_file, ci->json_buf);
	if (err != 0)
		return -EINVAL;

	return glm_init(ci);
}

int
ccow_fsio_glm_nodes_init(char *nodes, ci_t *ci)
{
	int err;
	struct dqlite_glm_ops *glm_ops;
	void *dqclient_handle;

	log_info(fsio_lg, "Nodes init start");

	if (!_is_nodes_str_valid(nodes)) {
		log_error(fsio_lg, "Invalid nodes: %s", nodes);
		return -EINVAL;
	}

	log_info(fsio_lg, "Nodes init nodes: %s", nodes);

	err = _node_str_to_json(nodes, ci->json_buf);
	if (err) {
		log_error(fsio_lg, "Could not convert nodes str to json: %s, err: %d", nodes, err);
		return -EINVAL;
	}
	log_info(fsio_lg, "Nodes json string: %s", ci->json_buf);

	return glm_init(ci);
}

void
ccow_fsio_glm_clean(ci_t *ci)
{
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
    geo_lock_t gvm;
	gvm.segid = ci->segid;
	gvm.serverid = ci->serverid;
	int idx = get_glm(ci);
	glm_ops->clean(ci->glm_client[idx], &gvm);
	put_glm(ci, idx);

}


void
ccow_fsio_glm_term(ci_t *ci)
{
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	for (int i=0; i<ci->glm_pool_size; i++) {
		glm_ops->disconnect(ci->glm_client[i]);
		ci->glm_client[i] = NULL;
		ci->glm_use[i] = 1;
	}
	ci->glm_enabled = 0;
	ci->glm_ops = NULL;
}
