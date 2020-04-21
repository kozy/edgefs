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

#include "ccow.h"
#include "fsio_inode.h"
#include "fsio_system.h"
#include "glm_ops.h"

#define FSIO_GLM_LOCKS_DB	"fsio_locks.db"
#define TIMER_INTERVAL		5

#define MAX_CCOW_JSON_SIZE	16383
#define CCOW_CONFIG_FILE	"/opt/nedge/etc/ccow/ccow.json"

static char ccow_buf[MAX_CCOW_JSON_SIZE + 1];

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

int
ccow_fsio_inode_glm_lock(ccowfs_inode * inode)
{
	int err, locked = 0;
	struct cdq_client *client = inode->ci->glm_client;
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

	err = glm_ops->lock(inode->ci->glm_client, &inode->glm_mutex,
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
		err = glm_ops->unlock(inode->ci->glm_client,
				&inode->glm_mutex,
				inode->ino_str, inode->genid,
				&inode->glm_cached);
	} else {
		pthread_mutex_unlock(&inode->glm_mutex);
	}
	return err;
}

int
ccow_fsio_glm_init(char *service_name, char *ccow_config_file, ci_t *ci)
{
	int err;
	char json_buf[4096];
	struct dqlite_glm_ops *glm_ops;
	void *dqclient_handle;

	err = _glm_get_locknodes(service_name, ccow_config_file, json_buf);
	if (err != 0)
		return -EINVAL;

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
	err = glm_ops->connect(json_buf, &ci->glm_client);
	if (err != 0) {
		log_error(fsio_lg, "Unable to connect to dqlite glm, err: %d",
				err);
		ci->glm_client = NULL;
		ci->glm_enabled = 0;
		ci->glm_ops = NULL;
		dlclose(glm_ops);
	} else {
		ci->glm_enabled = 1;
		ci->glm_ops = glm_ops;
	}
	return err;
}

void
ccow_fsio_glm_term(ci_t *ci)
{
	struct dqlite_glm_ops *glm_ops = ci->glm_ops;
	glm_ops->disconnect(ci->glm_client);
	ci->glm_enabled = 0;
	ci->glm_ops = NULL;
	ci->glm_client = NULL;
}
