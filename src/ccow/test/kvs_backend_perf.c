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

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <ccowutil.h>
#include <dlfcn.h>
#include <pthread.h>
#include <signal.h>
#include "../src/libreptrans/rtkvs/kvs-backend.h"

/* Configuration is provided in a dedicated JSON file. Its structure:
 *
 * {
 * 	"backend": "<backend_name>",
 * 	"path": "<device_path>",
 * 	"options": {
 * 	 ... (backend-specific options go here)
 * 	},
 * 	"chunks": {"size": <size_in_bytes>,
 * 		"job_size": <number_of_chunks>,
 * 		"bulk_size": <bulk_size_entries",
 * 		"key_size": <sizeof_of_a_key_bytes>,
 * 		"key_seq": <1 | 0>
 * 	}
 * }
 *
 */
#define BE_PERF_VERSION         1
#define KEY_LEN_META_SIZE_MAX   (16*1024L)
#define PERF_CFG_KEY            "be_perf_cfg"
#define DATA_HT                 2


struct be_perf_cfg {
	uint64_t version;
	uint64_t key_len_index_first;
	uint64_t key_len_index_last;
	uint64_t written;
	uint64_t deleted;
};

struct key_len {
	uint64_t key;
	uint64_t key_len;
	uint64_t val_len;
};

struct be_job {
	kvs_backend_t* be;
	kvs_backend_handle_t h;
	size_t chunk_size;
	size_t chunk_size_max;
	size_t bulk_size;
	size_t bulk_size_max;
	size_t n_chunks;
	size_t key_size;
	size_t del_percent;
	int key_seq;
	struct key_len* key_len_array;
	size_t n_key_len;
	uint64_t key_len_index;
};

Logger lg;
static void *kvs_lib_handle = NULL;

uint64_t g_iops = 0;
volatile int g_term = 0;
static struct be_perf_cfg g_cfg = {0};

static void
signal_handler(int signum) {
	if (signum == SIGINT)
		g_term = 1;
	else {
		fprintf(stderr, "Signal %d received, exiting\n", signum);
		exit(-1);
	}
}

static inline void
append_written(uint64_t written, size_t ios) {
	atomic_add64(&g_cfg.written, written);
	atomic_add64(&g_iops, ios);
}

static kvs_backend_t*
kvs_backend_get(const char* name) {
	char lib_name[256];
	char sym_name[256];

	kvs_backend_t* res = NULL;
	sprintf(lib_name, "lib%s_backend.so", name);
	kvs_lib_handle = dlopen(lib_name, RTLD_LAZY | RTLD_LOCAL);
	if (!kvs_lib_handle) {
		char *errstr = dlerror();
		fprintf(stderr, "Cannot open backend library %s\n", lib_name);
		return NULL;
	}
	sprintf(sym_name, "%s_vtbl", name);
	res = dlsym(kvs_lib_handle, sym_name);
	if (!res)
		fprintf(stderr, "Cannot export a vtbl from %s\n", lib_name);
	if (strcmp(res->name, name))
		fprintf(stderr, "Backend name mismatch: %s vs %s\n", name, res->name);
	return res;
}

static int
parse_be_job(struct be_job* job, const json_value* o) {
	assert(job);
	assert(o);
	job->chunk_size = 128;
	job->bulk_size = 16;
	job->n_chunks = 1024;
	job->key_size = 16;
	job->key_seq = 1;
	for (size_t i = 0; i < o->u.object.length; i++) {
		if (strcmp(o->u.object.values[i].name, "job_size") == 0) {
			if (o->u.object.values[i].value->type != json_integer) {
				fprintf(stderr, "Job size has to be an integer\n");
				return -EINVAL;
			} else
				job->n_chunks = o->u.object.values[i].value->u.integer;
		} else if (strcmp(o->u.object.values[i].name, "chunk_size") == 0) {
			if (o->u.object.values[i].value->type != json_integer) {
				fprintf(stderr, "Chunks size has to be an integer\n");
				return -EINVAL;
			} else
				job->chunk_size = o->u.object.values[i].value->u.integer;
		} else if (strcmp(o->u.object.values[i].name, "chunk_size_max") == 0) {
			if (o->u.object.values[i].value->type != json_integer) {
				fprintf(stderr, "Chunk size max has to be an integer\n");
				return -EINVAL;
			} else
				job->chunk_size_max = o->u.object.values[i].value->u.integer;
		} else if (strcmp(o->u.object.values[i].name, "bulk_size") == 0) {
			if (o->u.object.values[i].value->type != json_integer) {
				fprintf(stderr, "Bulk size has to be an integer\n");
				return -EINVAL;
			} else
				job->bulk_size = o->u.object.values[i].value->u.integer;
		} else if (strcmp(o->u.object.values[i].name, "bulk_size_max") == 0) {
			if (o->u.object.values[i].value->type != json_integer) {
				fprintf(stderr, "Bulk size max has to be an integer\n");
				return -EINVAL;
			} else
				job->bulk_size_max = o->u.object.values[i].value->u.integer;
		} else if (strcmp(o->u.object.values[i].name, "key_size") == 0) {
			if (o->u.object.values[i].value->type != json_integer) {
				fprintf(stderr, "The key size has to be an integer\n");
				return -EINVAL;
			} else
				job->key_size = o->u.object.values[i].value->u.integer;
		} else if (strcmp(o->u.object.values[i].name, "key_seq") == 0) {
			if (o->u.object.values[i].value->type != json_integer) {
				fprintf(stderr, "The key seq has to be an integer\n");
				return -EINVAL;
			} else
				job->key_seq = o->u.object.values[i].value->u.integer;
		} else if (strcmp(o->u.object.values[i].name, "del_percent") == 0) {
			if (o->u.object.values[i].value->type != json_integer) {
				fprintf(stderr, "The del_percent has to be an integer in range 0..100\n");
				return -EINVAL;
			} else
				job->del_percent = o->u.object.values[i].value->u.integer;
		}
	}
	return 0;
}

static void*
perf_thread(void* arg) {
	uint64_t ts = get_timestamp_us();
	uint64_t written_prev = 0, iops_prev = 0;
	while (!g_term) {
		usleep(1000000);
		double dt = (get_timestamp_us() - ts)/1000000.0;
		uint64_t written = g_cfg.written - written_prev;
		if (!written)
			continue;
		uint64_t iops = g_iops - iops_prev;
		written_prev = g_cfg.written;
		iops_prev = g_iops;
		ts = get_timestamp_us();
		double trougput = written/dt;
		double fiops = iops/dt;
		printf("Perf: dt %0.4f, total %lu MB, %.3f MB/s, %.3f IOPs\n",
			dt, written_prev/(1024*1024), trougput/(1024.0f*1024.0f), fiops);
	}
	return NULL;
}

static int
be_save_config(kvs_backend_t* be, kvs_backend_handle_t h,
	struct be_perf_cfg* be_cfg) {
	iobuf_t key = {.base = PERF_CFG_KEY, .len = strlen(PERF_CFG_KEY) +1 };
	iobuf_t val = { .base = (char*)be_cfg, .len = sizeof(*be_cfg) };
	return be->put(h, 0, &key, &val, 1);
}

static int
be_load_config(kvs_backend_t* be, kvs_backend_handle_t h,
	struct be_perf_cfg* be_cfg) {
	struct be_perf_cfg l_be_cfg = {.version = 0};
	iobuf_t key = {.base = PERF_CFG_KEY, .len = strlen(PERF_CFG_KEY) +1 };
	iobuf_t val = { .base = (char*)&l_be_cfg, .len = sizeof(l_be_cfg) };

	int err = be->get(h, 0, key, &val);
	if (err) {
		if (err == -ENOENT) {
			printf("Configuration record not found, using default\n");
			l_be_cfg.version = BE_PERF_VERSION;
			l_be_cfg.key_len_index_first = 0;
			l_be_cfg.key_len_index_last = 0;
		} else {
			fprintf(stderr, "Error reading configuration record: %d\n", err);
		}
	} else
		*be_cfg = l_be_cfg;
	return err;
}


static int
be_sync_key_len(kvs_backend_t* be, kvs_backend_handle_t h, struct be_job* job) {
	char keystr[128];
	sprintf(keystr, "KeyArray%08lu", job->key_len_index);
	iobuf_t key_buf = {.base = keystr, .len = strlen(keystr) + 1};
	iobuf_t val_buf = {.base = (char*)job->key_len_array,
		.len = job->n_key_len*sizeof(struct key_len)};
	int err = be->put(h, 0, &key_buf, &val_buf, 1);
	if (!err) {
		g_cfg.key_len_index_last = job->key_len_index;
		err = be_save_config(be, h, &g_cfg);
		if (err) {
			fprintf(stderr, "Configuration put error %d\n", err);
		}
	}
	printf("Synced keys to entry %s, length %lu, err %d\n", keystr,
		val_buf.len, err);
	return err;
}

static int
be_add_key_len_entry(kvs_backend_t* be, kvs_backend_handle_t h, struct be_job* job,
	uint64_t key, uint64_t key_len, uint64_t val_len) {
	int err = 0;
	job->key_len_array[job->n_key_len].key = key;
	job->key_len_array[job->n_key_len].key_len = key_len;
	job->key_len_array[job->n_key_len].val_len = val_len;
	if (++job->n_key_len >= KEY_LEN_META_SIZE_MAX) {
		err = be_sync_key_len(be, h, job);
		if (job->n_key_len >= KEY_LEN_META_SIZE_MAX) {
			job->n_key_len = 0;
			job->key_len_index++;
		}
	}
	return err;
}

static int
be_load_key_len(kvs_backend_t* be, kvs_backend_handle_t h, struct be_job* job) {
	char keystr[128];
	job->key_len_index = g_cfg.key_len_index_last;
	int err = 0;
	iobuf_t key, val;
	sprintf(keystr, "KeyArray%08lu", job->key_len_index);
	key.base = keystr;
	key.len = strlen(keystr) + 1;
	val.base = (char*)job->key_len_array;
	val.len = KEY_LEN_META_SIZE_MAX * sizeof(struct key_len);
	err = be->get(h, 0, key, &val);
	if (!err) {
		printf("Found key_len entry at index %lu\n", job->key_len_index);
		job->n_key_len = val.len / sizeof(struct key_len);
	}
	return err && err != -ENOENT ? err : 0;
}

static int
be_delete_entries(kvs_backend_t* be, kvs_backend_handle_t h, struct be_job* job,
	size_t amount) {

	kvs_backend_info_t binfo = {0};
	size_t deleted = 0;
	int err = be->info(h, &binfo);
	if (err)
		return err;

	iobuf_t* keys = je_calloc(binfo.del_bulk_size, sizeof(iobuf_t));
	if (!keys)
		return -ENOMEM;
	size_t n_keys = 0;
	for (uint64_t n = g_cfg.key_len_index_first; n <= g_cfg.key_len_index_last; n++) {
		char keystr[128] = {0};
		struct key_len* kl = NULL;
		size_t alen = 0;
		if (job && n == job->key_len_index) {
			kl = job->key_len_array;
			alen = job->n_key_len;
		} else {
			iobuf_t key, val;
			sprintf(keystr, "KeyArray%08lu", n);
			key.base = keystr;
			key.len = strlen(keystr) + 1;
			val.base = je_malloc(KEY_LEN_META_SIZE_MAX * sizeof(struct key_len));
			val.len = KEY_LEN_META_SIZE_MAX * sizeof(struct key_len);
			int err = be->get(h, 0, key, &val);
			if (err) {
				fprintf(stderr, "Unable to get key_len array at key %s: %d", keystr, err);
				je_free(val.base);
				return -EIO;
			}
			alen = val.len / sizeof(struct key_len);
			kl = (void*)val.base;
		}
		size_t i = 0;
		if (alen) for (i = alen - 1; i != 0; i--) {
			keys[n_keys].len = kl[i].key_len;
			if (!keys[n_keys].base)
				keys[n_keys].base = je_malloc(128);
			assert(keys[n_keys].base);
			memset(keys[n_keys].base, 0, 128);
			size_t* k = (size_t*)keys[n_keys].base;
			*k = kl[i].key;
			deleted += kl[i].val_len;
			if (++n_keys >= binfo.del_bulk_size) {
				err = be->remove(h, DATA_HT, keys, n_keys);
				if (err) {
					fprintf(stderr, "Keys delete error %d, n_keys %lu\n",
						err, n_keys);
				}
				n_keys = 0;
			}
			if (deleted >= amount)
				break;
		}
		if (n_keys) {
			err = be->remove(h, DATA_HT, keys, n_keys);
			if (err) {
				fprintf(stderr, "Keys delete error %d, n_keys %lu\n",
					err, n_keys);
			}
			n_keys = 0;
		}
		if (strlen(keystr)) {
			iobuf_t key;
			key.base = keystr;
			key.len = strlen(keystr) + 1;
			if (!i) {
				/* Remove record if all entries has been deleted */
				err = be->remove(h, 0, &key, 1);
				if (err) {
					fprintf(stderr, "Key %s delete error: %d\n",
						keystr, err);
				}
				g_cfg.key_len_index_first++;
				/* Update configuration record */
				err = be_save_config(be, h, &g_cfg);
				if (err) {
					fprintf(stderr, "Config save error %d\n",
						err);
				}
			} else {
				/* There are some unprocessed entries, overwrite the record */
				iobuf_t val;
				val.base = (void*)kl;
				val.len = i * sizeof(struct key_len);
				err = be->put(h, 0, &key, &val, 1);
				if (err) {
					fprintf(stderr, "Key %s overwrite error: %d\n",
						keystr, err);
				}
			}
		} else {
			job->n_key_len = i;
		}
		if (deleted >= amount)
			break;
	}
	if (keys) {
		for (size_t j = 0; j < binfo.del_bulk_size; j++) {
			if (keys[j].base)
				je_free(keys[j].base);
		}
		je_free(keys);
	}
	printf("Removed %lu bytes, index %lu, error %d\n", deleted, job->n_key_len, err);
	g_cfg.deleted += deleted;
	return err;
}

static void*
be_job_thread(void* arg) {
	struct be_job* job = arg;
	kvs_backend_t* be = job->be;
	kvs_backend_handle_t h = job->h;
	kvs_backend_info_t binfo = {0};

	be_load_key_len(be, h, job);
	srand(time(NULL));
	int err = be->info(h, &binfo);
	if (binfo.put_bulk_size < job->bulk_size_max) {
		printf("Adjusting put bulk size to %lu\n", binfo.put_bulk_size);
		job->bulk_size = binfo.put_bulk_size;
	}
	iobuf_t* keys = je_calloc(binfo.put_bulk_size, sizeof(iobuf_t));
	iobuf_t* vals = je_calloc(binfo.put_bulk_size, sizeof(iobuf_t));
	size_t n_val = 0, bulk_len = 0;

	printf("Preparing a buffer\n");
	size_t* buff = je_malloc(job->n_chunks + job->chunk_size_max);
	for (size_t i = 0; i < (job->n_chunks + job->chunk_size_max) / sizeof(size_t); i++) {
		buff[i] = rand();
	}

	size_t d_chunk_size = 0, d_bulk_size = 0, bulk_size = job->bulk_size;;
	if (job->chunk_size_max)
		d_chunk_size = job->chunk_size_max - job->chunk_size;
	if (job->bulk_size_max)
		d_bulk_size = job->bulk_size_max - job->bulk_size;

	for (size_t i = 0; i < job->n_chunks && !g_term; i++) {
		keys[n_val].len = job->key_size;
		if (!keys[n_val].base)
			keys[n_val].base = je_malloc(keys[n_val].len);
		memset(keys[n_val].base, 0, keys[n_val].len);
		if (job->key_seq) {
			size_t* k = (size_t*)keys[n_val].base;
			*k = i;
		} else {
			size_t* k = (size_t*)keys[n_val].base;
			*k = rand();
		}
		vals[n_val].len = job->chunk_size + (d_chunk_size ? rand() % d_chunk_size : 0);
		vals[n_val].base = ((char*)buff) + i;
		bulk_len += vals[n_val].len;
		if (++n_val >= bulk_size) {
			err = be->put(h, 2, keys, vals, n_val);
			if (err) {
				fprintf(stderr, "Backend PUT error %d\n", err);
				goto _exit;
			}
			append_written(bulk_len, n_val);
			for (size_t n = 0; n < n_val; n++) {
				size_t key = *((size_t*)keys[n].base);
				size_t len = vals[n].len;
				err = be_add_key_len_entry(be, h, job, key,
					keys[n].len, len);
				if (err) {
					fprintf(stderr, "KeyArray PUT error %d\n", err);
					goto _exit;
				}
			}
			if (job->del_percent && g_cfg.written > 50UL*1024UL*1024UL*1024UL)
				be_delete_entries(be, h, job,
					bulk_len*job->del_percent/100);
			n_val = 0;
			bulk_len = 0;
			bulk_size = job->bulk_size + (d_bulk_size ? rand() % d_bulk_size : 0);
		}
	}

	if (n_val) {
		err = be->put(h, DATA_HT, keys, vals, n_val);
		if (err) {
			fprintf(stderr, "Backend PUT error %d\n", err);
		}
	}
_exit:
	be_sync_key_len(be, h, job);
	if (keys) {
		for (size_t i = 0; i < job->bulk_size; i++) {
			if (keys[i].base)
				je_free(keys[i].base);
		}
		je_free(keys);
	}
	if (vals)
		je_free(vals);
	return NULL;
}

int main(int argc, char** argv) {
	kvs_backend_t* be = NULL;
	if (argc < 2) {
		printf("Usage: kvs_backend_perf <filename.json>\n");
		exit(1);
	}

	signal(SIGPIPE, SIG_IGN);       // Ignore SIG_IGN
	signal(SIGHUP, signal_handler);
	signal(SIGUSR1, signal_handler);
	signal(SIGUSR2, signal_handler);
	//signal(SIGABRT, signal_handler);
	//signal(SIGSEGV, signal_handler);
	//signal(SIGBUS, signal_handler);
	signal(SIGINT, signal_handler);


	lg = Logger_create("kvs_backed_perf");
	const char* cfg_name = argv[1];
	int err = 0;

	struct stat st;
	if ((err = stat(cfg_name, &st)) != 0) {
		fprintf(stderr, "Cannot access configuration file %s: %s\n",
			cfg_name, strerror(errno));
		return 1;
	}

	char* buff = je_malloc(st.st_size);
	assert(buff);

	int fd = open(cfg_name, O_RDONLY);
	if (fd == -1) {
		je_free(buff);
		fprintf(stderr, "Cannot open configuration file %s: %s\n",
			cfg_name, strerror(errno));
		return 1;
	}
	int len = read(fd, buff, st.st_size);
	if (len == -1) {
		close(fd);
		je_free(buff);
		fprintf(stderr, "Cannot read configuration file %s: %s\n",
			cfg_name, strerror(errno));
		return 1;
	}
	close(fd);

	json_value *o = json_parse(buff, st.st_size);
	if (!o) {
		fprintf(stderr, "Cannot parse configuration file %s\n",
			cfg_name);
		je_free(buff);
		return 1;
	}

	/* syntax error */
	if (o->type != json_object) {
		fprintf(stderr, "Syntax error: not an object\n");
		return -1;
	}

	json_value* backend_opt = NULL;
	json_value* chunks = NULL;
	char *device = NULL;
	char* backend_name = NULL;
	for (size_t i = 0; i < o->u.object.length; i++) {
		if (strcmp(o->u.object.values[i].name, "backend") == 0) {
			backend_name = je_strdup(o->u.object.values[i].value->u.string.ptr);
		} else if (strcmp(o->u.object.values[i].name, "path") == 0) {
			device = je_strdup(o->u.object.values[i].value->u.string.ptr);
		} else if (strcmp(o->u.object.values[i].name, "options") == 0) {
			if (o->u.object.values[i].value->type != json_object)
				fprintf(stderr, "Backend opts has to be an object\n");
			else
				backend_opt = o->u.object.values[i].value;
		} else if (strcmp(o->u.object.values[i].name, "chunks") == 0) {
			if (o->u.object.values[i].value->type != json_object)
				fprintf(stderr, "Chunks opts has to be an object\n");
			else
				chunks = o->u.object.values[i].value;
		}
	}
	if (!backend_name) {
		fprintf(stderr, "Cannot find a backend name in %s\n", cfg_name);
		err = 1;
		goto _exit;
	}
	if (!device) {
		fprintf(stderr, "Couldn't find device/mounpoint path in %s\n", cfg_name);
		err = 1;
		goto _exit;
	}
	if (!backend_opt) {
		fprintf(stderr, "Backend options aren't specified in %s\n", cfg_name);
		err = 1;
		goto _exit;
	}

	if (!chunks) {
		fprintf(stderr, "Jobs configuration isn't specified in %s\n", cfg_name);
		err = 1;
		goto _exit;
	}

	be = kvs_backend_get(backend_name);
	if (!be) {
		fprintf(stderr, "Unable to load backend %s\n", backend_name);
		err = 1;
		goto _exit;
	}
	printf("\nLoaded backend %s\n", be->name);
	kvs_backend_handle_t h = NULL;
	err = be->init(device, backend_opt, &h);
	if (!h) {
		fprintf(stderr, "Backend %s init error: %d\n", be->name, err);
		goto _exit;
	}
	err = be_load_config(be, h, &g_cfg);
	if (!err) {
		printf("Fetched configuration entry\n");
	}
	printf("Initialized backend %s\n", be->name);
	struct be_job job = {.be = be, .h = h, .n_key_len = 0};
	job.key_len_array = je_calloc(KEY_LEN_META_SIZE_MAX, sizeof(struct key_len));
	err = parse_be_job(&job, chunks);
	pthread_t thr[2];

	err = pthread_create(thr, NULL, perf_thread, NULL);
	if (err) {
		fprintf(stderr, "Unable t create a performance thread: %d\n", err);
		goto _exit;
	}

	err = pthread_create(thr+1, NULL, be_job_thread, &job);
	if (err) {
		fprintf(stderr, "Unable t create a bacned job thread: %d\n", err);
		goto _exit;
	}

	pthread_join(thr[1], NULL);

	g_term = 1;

	pthread_join(thr[0], NULL);

_exit:
	if (be && h)
		be->exit(h);
	if (buff)
		je_free(buff);
	return err;
}


