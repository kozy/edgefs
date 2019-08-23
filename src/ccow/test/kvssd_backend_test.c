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
#include <dlfcn.h>
#include <assert.h>
#include <kvs-backend.h>
#include "errno.h"

#define KVSSD_PATH "/dev/nvme0n1"
Logger lg;

static void
random_buffer(iobuf_t buf) {
	for (size_t n = 0; n < buf.len/sizeof(uint32_t); n++) {
		if (buf.len >= sizeof(uint32_t)) {
			uint32_t val = random();
			memcpy(buf.base, &val, sizeof(uint32_t));
			buf.base += sizeof(uint32_t);
			buf.len -= sizeof(uint32_t);
		}
	}
}

int
main (int argc, char** argv) {
	static void *kvs_lib_handle = NULL;
	static kvs_backend_t* kvs_be = NULL;
	static kvs_backend_handle_t h = NULL;

	const char* lib_name = "libkvssd_backend.so";
	char sym_name[256];

	lg = Logger_create("kvssd_backend_test");

	kvs_backend_t* res = NULL;

	kvs_lib_handle = dlopen(lib_name, RTLD_LAZY | RTLD_LOCAL);
	if (!kvs_lib_handle) {
		char *errstr = dlerror();
		fprintf(stderr, "ERROR: Cannot open backend library %s: %s\n", lib_name, errstr);
		return -1;
	}
	kvs_be = dlsym(kvs_lib_handle, "kvssd_vtbl");
	if (!kvs_be) {
		fprintf(stderr, "ERROR: Cannot export the kvsadi_vtbl symbol from %s\n",
			lib_name);
		return -2;
	}

	json_value vemu;
	vemu.type = json_integer;
	vemu.u.integer = 1;

	json_object_entry oemu = {.name = "emulator", .name_length=9, .value = &vemu};

	json_value oval = {.type = json_object};
	oval.u.object.length = 1;
	oval.u.object.values = &oemu;

	int err = kvs_be->init(KVSSD_PATH, &oval, &h);
	if (err) {
		fprintf(stderr, "ERROR: Cannot initialize the KVSSD device at %s\n",
			KVSSD_PATH);
		return -3;
	}
	size_t n_bufs = 100;
	size_t key_len = 16;
	size_t value_len = 4096;
	int8_t ttag = 5;

	/* Put test. adding several different KV objects */
	iobuf_t* keys = malloc(n_bufs*sizeof(*keys));
	iobuf_t* values = malloc(n_bufs*sizeof(*values));

	kvs_backend_info_t info;
	kvs_be->info(h, &info);
	size_t batch_size = n_bufs;
	if (info.put_bulk_size < n_bufs)
		batch_size = info.put_bulk_size;
	int it = 0;
	while (n_bufs) {
		size_t sz = n_bufs > batch_size ? batch_size : n_bufs;
		for (size_t i = 0; i < sz; i++) {
			iobuf_t* kb = keys + it*batch_size + i;
			kb->base = malloc(key_len);
			kb->len = key_len;
			assert(kb->base);
			random_buffer(*kb);
			iobuf_t* vb = values + it*batch_size + i;
			vb->base = malloc(value_len);
			vb->len = value_len;
			assert(vb->base);
			random_buffer(*vb);
		}
		int err = kvs_be->put(h, ttag, keys + it*batch_size,
			values + it*batch_size, sz);
		assert(err == 0);
		n_bufs -= sz;
		it++;
	}

	/* Trying to retrieve them */
	for (size_t i = 0; i < n_bufs; i++) {
		iobuf_t val;
		int err = kvs_be->get(h, ttag, keys[i], &val);
		assert(err == 0);
		assert(memcmp(val.base, values[i].base, values[i].len) == 0);
		free(val.base);
	}

	/* deleting */
	batch_size = n_bufs;
	if (info.del_bulk_size < n_bufs)
		batch_size = info.del_bulk_size;
	it = 0;
	size_t left = n_bufs;
	while (left) {
		size_t sz = left > batch_size ? batch_size : left;
		assert(kvs_be->remove(h, ttag, keys + it*batch_size, sz) == 0);
		it++; left -= sz;
	}
	/* Ensure they were removed */
	for (size_t i = 0; i < n_bufs; i++) {
		iobuf_t val;
		int err = kvs_be->get(h, ttag, keys[i], &val);
		assert(err == -ENOENT);
		free(val.base);
	}
	free(keys);
	free(values);
	return 0;
}
