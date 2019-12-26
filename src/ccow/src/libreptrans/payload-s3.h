/*
 * Copyright (c) 2015-2018 Nexenta Systems, inc.
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

#ifndef NEDGE_PAYLOAD_S3_H
#define NEDGE_PAYLOAD_S3_H

#include <curl/curl.h>

#define EDGE_USER_AGENT "EdgeRTRD/1.0"

struct get_cache;

struct payload_s3 {
	int ssl_en;
	char host[1024];
	char path[2048];
	int port;
	char *bucket_url;
	char *access_key;
	char *secret_key;
	char *aws_region;
	CURLSH *share;
	pthread_mutex_t conn_lock;
	pthread_mutex_t get_lock;
	pthread_cond_t get_cond;
	uint64_t parallel_gets;
	uint64_t parallel_gets_max;
	struct get_cache* cache;
};

int
payload_s3_init(char *url, char *region, char *keyfile, uint64_t cache_entries_max,
	struct payload_s3 **ctx_out);
void payload_s3_destroy(struct payload_s3 *ctx);
int payload_s3_put(struct payload_s3 *ctx, const char* key, uv_buf_t *data);
int payload_s3_put_multi(struct payload_s3 *ctx, uv_buf_t* keys, uv_buf_t* data, size_t n);
int payload_s3_get(struct payload_s3 *ctx, const char* key, uv_buf_t *outbuf);
int payload_s3_delete(struct payload_s3 *ctx, const char* key);
int payload_s3_delete_multi(struct payload_s3 *ctx, uv_buf_t* keys, size_t n);

#endif
