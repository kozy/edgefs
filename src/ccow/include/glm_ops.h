/*
 * Copyright (c) 2020 Nexenta Systems, inc.
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

#ifndef __GLMOPS_H_
#define __GLMOPS_H_

/* Global locking structure */
typedef struct geo_lock {
	char		path[2048];
	uint64_t	genid;
	uint64_t	uvid;
	uint32_t	deleted;
	char		nhid[512];
	char		vmchid[512];
	uint64_t	segid;
	uint64_t	serverid;
	uint64_t	size;
	int64_t		lock_time;
	int		lock_state;
} geo_lock_t;

struct dqlite_glm_ops {
	int (*connect) (char *json_cluster, void **client);
	int (*disconnect) (void *client);
	int (*lock) (void *client, pthread_mutex_t *mutex, char *ino_str,
			unsigned int oid_size, char *nhid, char *vmchid,
			uint64_t genid, uint32_t deleted, unsigned int *cached);
	int (*unlock) (void *client, pthread_mutex_t *mutex, char *ino_str,
			uint64_t genid, unsigned int *cached);
	int (*acquire) (void *cl, geo_lock_t *lock_rec);
	int (*release) (void *cl, geo_lock_t *lock_rec);
	int (*clean) (void *cl, geo_lock_t *lock_rec);
	int (*get) (void *cl, const char *path_key, geo_lock_t *lock_rec);
};

#endif /* __GLMOPS_H_ */
