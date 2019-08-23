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
 *
 * A Samsung KV ADI backend for EdgeFS rtkvs reptrans driver.
 *
 */
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include <stdbool.h>

#include <kvssd/kvs_api.h>
#include <kvs-backend.h>

#define NVME_CMD "/usr/sbin/nvme"

static volatile uint64_t g_inst_count = 0;

#define KVSADI_MAX_BATCH_SIZE	(256)

static int
kvssd_delete(kvs_backend_handle_t handle, int8_t ttag, iobuf_t* keys, size_t n_entries);

/* A simple timestamp generator to be used for performance measurement */
static inline uint64_t
get_timestamp_us() {
	struct timespec tp;
	(void)clock_gettime(CLOCK_REALTIME, &tp);
	return ((tp.tv_sec * 1000000000) + tp.tv_nsec) / 1000;
}


/* List of items to be freed upon function exit */
struct fl_item {
	struct fl_item* next;
	void* ptr;
};

/* Add a malloc-allocated item to a list for deferred delete */
static int
fl_append(struct fl_item** ph, void* buf) {
	struct fl_item* it = malloc(sizeof(*it));
	if (!it)
		return -ENOMEM;
	it->next = *ph;
	it->ptr = buf;
	*ph = it;
	return 0;
}

/* Destroy a deferred delete list */
static void
fl_destroy(struct fl_item* ph) {
	void* ptr = NULL;
	while (ph) {
		free(ph->ptr);
		void* ptr = ph;
		ph = ph->next;
		free(ptr);
	}
}

typedef struct kvssd_handle {
	char* path;
	bool emul;
	kvs_device_handle dev_hdl;
	kvs_container_handle con_hdl;
	volatile int term;
	uint64_t op_count;
} kvssd_handle_t;


/* A message to be passed to a submission queue */
typedef struct kvssd_msg {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int busy;
	kvs_result status;
	void* priv_data;
	uint64_t ts;
} kvssd_msg_t;

/**
 * Initialize a KV device
 *
 * @param path   [in] is a path to KV device
 * @param opts   [in] backend configuration options
 * @param handle [out] a create instance handle
 *
 * @returns 0 on success or error code otherwise
 */
static int
kvssd_init(const char* path, json_value *o, kvs_backend_handle_t* handle) {
	kvs_init_options dev_init_opts;
	kvssd_handle_t* h = calloc(1, sizeof(kvssd_handle_t));
	if (!h)
		return -ENOMEM;
	int use_emulator = 0;
	if (o) {
		for (size_t i = 0; i < o->u.object.length; i++) {
			char *namekey = o->u.object.values[i].name;
			json_value *v = o->u.object.values[i].value;
			if (strcmp(namekey, "emulator") == 0) {
				if (v->type != json_integer) {
					log_error(lg, "rt-kvs.json error: \"emulator\" "
						"option has to be set to 0 or 1");
					continue;
				}
				use_emulator = v->u.integer;
			}
		}
	}

	kvs_result res = kvs_init_env_opts(&dev_init_opts);
	dev_init_opts.aio.iocoremask = 0;
	dev_init_opts.memory.use_dpdk = 0;
	dev_init_opts.aio.queuedepth = KVSADI_MAX_BATCH_SIZE;
	if (res != KVS_SUCCESS) {
		log_error(lg, "Dev(%s) kvs_init_env_opts error %d", path, res);
		return -EIO;
	}
	h->path = strdup(path ? path : "void");
	if (use_emulator) {
		h->emul = 1;
		dev_init_opts.emul_config_file = "/opt/nedge/etc/ccow/kvssd_emul.conf";
		path = "/dev/kvemul";
	} else {
		dev_init_opts.emul_config_file = "";
		struct stat st;
		int err = stat(NVME_CMD, &st);
		if (err) {
			log_error(lg, "Dev(%s) couldn't detect the nvme tool."
				"The sanitize call won't work", h->path);
		}
	}

	char *found;
	found = strchr(path, ':');
	  if(found) { // spdk driver
		dev_init_opts.memory.use_dpdk = 1;
		char *core;
		core = dev_init_opts.udd.core_mask_str;
		*core = '0';
		core = dev_init_opts.udd.cq_thread_mask;
		*core = '0';
		dev_init_opts.udd.mem_size_mb = 1024;
		dev_init_opts.udd.syncio = 0; // use async IO mode
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(0, &cpuset); // CPU 0
		sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
	} else {
		dev_init_opts.memory.use_dpdk = 0;
	}
	res = kvs_init_env(&dev_init_opts);
	if (res != KVS_SUCCESS) {
		log_error(lg, "Dev(%s) kvs_init_env error %d", path, res);
		return -EIO;
	}

	 h->dev_hdl = NULL;

	/* initialize the device */
	res = kvs_open_device(path, &h->dev_hdl);
	if (res != KVS_SUCCESS) {
		log_error(lg, "Dev(%s) kvs_open_device error: %d", path, res);
		return -EINVAL;
	}

	kvs_container_context ctx;
	res = kvs_create_container(h->dev_hdl, "edgefs", 6, &ctx);
	if (res != KVS_SUCCESS) {
		log_error(lg, "Dev(%s) kvs_create_container error %d", path, res);
		return -EIO;
	}

	res = kvs_open_container(h->dev_hdl, "edgefs", &h->con_hdl);
	if (res != KVS_SUCCESS) {
		log_error(lg, "Dev(%s) kvs_open_container error %d", path, res);
		return -EIO;
	}

	h->term = 0;
	*handle = h;
	g_inst_count++;
	log_notice(lg, "kvssd backend for device %s has been initialized", path);
	return 0;
}

/*
 * Compose a backend key from a type tag and rtkvs key.
 * The type tag in placed in first tow bytes of the key in order to
 * implement a per type tag iterator
 */
static int
kvsadi_compose_key(int8_t ttag, iobuf_t key, iobuf_t* key_out) {
	uint8_t hdr[2] = {0};
	hdr[0] = ttag;
	char* new_key = malloc(key.len+2);
	if (!new_key)
		return -ENOMEM;
	new_key[0] = hdr[0];
	new_key[1] = hdr[1];
	memcpy(new_key+sizeof(hdr), key.base, key.len);
	key_out->base = new_key;
	key_out->len = key.len + sizeof(hdr);
	return 0;
}

/* Extract a type tag and an rtkvs key from a backend key */
static int
kvsadi_decompose_key(int8_t* ttag, iobuf_t key, iobuf_t* key_out) {
	char* new_key = malloc(key.len-2);
	if (!new_key)
		return -ENOMEM;
	memcpy(new_key, key.base+2, key.len-2);
	*ttag = key.base[0] & 0x0F;
	key_out->base = new_key;
	key_out->len = key.len - 2;
	return 0;
}

static void
kvssd_put_callback(kvs_callback_context* ioctx) {
	kvs_container_handle *cont_hd = ioctx->cont_hd;
	kvssd_msg_t* msg = ioctx->private2;
	assert(ioctx->opcode == IOCB_ASYNC_PUT_CMD);
	if (ioctx->result != KVS_SUCCESS) {
		log_error(lg, "kvssd put error: %d", ioctx->result);
		msg->status = ioctx->result;
	}
	pthread_mutex_lock(&msg->lock);
	if (--msg->busy == 0)
		pthread_cond_signal(&msg->cond);
	msg->ts = get_timestamp_us() - msg->ts;
	pthread_mutex_unlock(&msg->lock);
}

/*
 * Put a number of KV pairs to the store
 *
 * @param handle [in] a backend handle
 * @param ttag   [in] type tag
 * @param keys   [in] array of rtkvs keys
 * @param values [in] array of rtkvs values
 * @param n_entries [in[ keys/values array size
 * @returns 0 on success or error code otherwise
 *
 * The call must guarantee atomicity: no KV pairs will be kept in a store
 * if at least one put fails.
 */
static int
kvssd_put(kvs_backend_handle_t handle, int8_t ttag, iobuf_t* keys, iobuf_t* values,
	size_t n_entries) {
	size_t i = 0;
	int err = 0;
	struct fl_item* free_list = NULL;
	kvssd_handle_t* h = handle;

	assert(h);

	if (h->term)
		return -ENODEV;

	if (!n_entries || n_entries > KVSADI_MAX_BATCH_SIZE)
		return -ERANGE;

	kvssd_msg_t* msg = malloc(sizeof(kvssd_msg_t));
	if (!msg)
		return -ENOMEM;

	__atomic_add_fetch(&h->op_count, 1, __ATOMIC_SEQ_CST);

	pthread_mutex_init(&msg->lock, NULL);
	pthread_cond_init(&msg->cond, NULL);
	msg->busy = n_entries;
	msg->status = 0;
	msg->priv_data = NULL;
	msg->ts = get_timestamp_us();

	kvs_key k[n_entries];
	kvs_value v[n_entries];
#if PUT_PERFSTAT
	size_t len_min = 0xFFFFFFFFFF;
	size_t len_max = 0;
#endif

	kvs_store_context ctx = {
		.option = {
			.st_type = KVS_STORE_POST,
			.kvs_store_compress = false
		},
		.private1 = handle,
		.private2 = msg
	};
	for (i = 0; i < n_entries; i++) {
		iobuf_t* value = values + i;
		iobuf_t* key = keys + i;
		char* base = value->base;
		size_t len = value->len;
		iobuf_t c_key;
#if PUT_PERFSTAT
		if (len > len_max)
			len_max = len;
		if (len < len_min)
			len_min = len;
#endif
		/* Transform the key adding the type tag field */
		err = kvsadi_compose_key(ttag, *key, &c_key);
		if (err) {
			log_error(lg, "kvsadi_compose_key() error %d", err);
			fl_destroy(free_list);
			goto _unwind;
		}
		fl_append(&free_list, c_key.base);
		int32_t min_val = 0;
		kvs_get_min_value_length(h->dev_hdl, &min_val);
		/* If there is a minimal value requirement: adjust it*/
		if (len < (size_t)min_val) {
			base = malloc(min_val);
			memcpy(base, value->base, value->len);
			len = min_val;
			fl_append(&free_list, base);
		}
		k[i].key = c_key.base;
		k[i].length = c_key.len;
		v[i].value = base;
		v[i].length = len;
		v[i].offset = 0;

		err = kvs_store_tuple_async (h->con_hdl, k + i, v + i, &ctx, kvssd_put_callback);
		if (err != KVS_SUCCESS) {
			log_error(lg, "kvs_store_tuple_async error %d", err);
			err = -EIO;
			fl_destroy(free_list);
			goto _unwind;
		}
	}
	pthread_mutex_lock(&msg->lock);
	while (msg->busy) {
		err = pthread_cond_wait(&msg->cond, &msg->lock);
		if (err)
			break;
	}
	pthread_mutex_unlock(&msg->lock);
#if PUT_PERFSTAT
	log_debug(lg, "KVS ADI put %lu chunks within %lu uS, len_min %lu, len_max %lu",
		n_entries, msg->ts, len_min, len_max);
#endif
	fl_destroy(free_list);
	if (err) {
		log_error(lg, "pthread_cond_wait() error %d", err);
		goto _unwind;
	}
	err = msg->status;
	if (err != KVS_SUCCESS) {
		log_error(lg, "kv_store() result error 0x%X", err);
		if (err == KVS_ERR_DEV_CAPACITY)
			err = -ENOSPC;
		else
			err = -EIO;
		goto _unwind;
	}
	free(msg);
	__atomic_sub_fetch(&h->op_count, 1, __ATOMIC_SEQ_CST);
	return 0;

_unwind:
	/* Got an error. Remove stored chunks */
	kvssd_delete(h, ttag, keys, i);
	free(msg);
	__atomic_sub_fetch(&h->op_count, 1, __ATOMIC_SEQ_CST);
	return err;
}

/**
 * De-initialize the backend instance
 */
static void
kvssd_exit(kvs_backend_handle_t handle) {
	kvssd_handle_t* h = handle;
	assert(h);
	if (h->term)
		return;
	h->term = 1;
	/* Waiting until all operations are done */
	while (__atomic_fetch_add(&h->op_count, 0, __ATOMIC_SEQ_CST) > 0) {
		usleep(100);
	}

	h->term = 2;

	kvs_close_container(h->con_hdl);
	if (--g_inst_count == 0)
		kvs_exit_env();
	free(h->path);
	free(h);
}

/*
 * Get a KV value by a key
 *
 * @param handle [in]  a backend handle
 * @param ttag   [in]  a type tag
 * @param key    [in]  value's key
 * @param value  [out] output value. NOTE: value memory buffer to be allocated
 *                     by a caller
 */
static int
kvssd_get(kvs_backend_handle_t handle, int8_t ttag, iobuf_t key, iobuf_t* value) {
	kvssd_handle_t* h = handle;
	assert(h);
	if (h->term)
		return -ENODEV;

	kvs_retrieve_context ctx = {
		.option = {
			.kvs_retrieve_delete = false,
			.kvs_retrieve_decompress = false
		}
	};

	/* Transform the key adding the type tag field */
	iobuf_t c_key;
	int err = kvsadi_compose_key(ttag, key, &c_key);
	if (err) {
		log_error(lg, "kvsadi_compose_key() error %d", err);
		return err;
	}
	kvs_key k = {.key = c_key.base, .length = c_key.len };
	kvs_value v = { .value = value->base, .length = value->len,
		.actual_value_size = value->len, .offset = 0};

	memset(value->base, 0, value->len);
	kvs_result res = kvs_retrieve_tuple(h->con_hdl, &k, &v, &ctx);
	if (res != KVS_SUCCESS) {
		log_error(lg, "kvs_retrieve_tuple error %d", res);
		err = -EIO;
	}
	free(c_key.base);
	return err;
}

static void
kvssd_del_callback(kvs_callback_context* ioctx) {
	kvs_container_handle *cont_hd = ioctx->cont_hd;
	kvssd_msg_t* msg = ioctx->private2;

	assert(ioctx->opcode == IOCB_ASYNC_DEL_CMD);
	if (ioctx->result != KVS_SUCCESS) {
		log_error(lg, "kvssd del error: %d", ioctx->result);
		msg->status = ioctx->result;
	}
	pthread_mutex_lock(&msg->lock);
	if (--msg->busy == 0)
		pthread_cond_signal(&msg->cond);
	msg->ts = get_timestamp_us() - msg->ts;
	pthread_mutex_unlock(&msg->lock);
}

/**
 * Delete a number of KV pairs
 *
 * @param handle    [in] a backend handle
 * @param ttag      [in] a type tag
 * @param keys      [in] an array of backend keys to be deleted
 * @param n_entries [in] keys array size
 * @returns 0 on success or error code otherwise
 */
static int
kvssd_delete(kvs_backend_handle_t handle, int8_t ttag, iobuf_t* keys, size_t n_entries) {

	struct fl_item* free_list = NULL;
	int err = 0;
	kvssd_handle_t* h = handle;

	assert(h);
	if (h->term)
		return -ENODEV;

	kvssd_msg_t* msg = malloc(sizeof(kvssd_msg_t));
	if (!msg)
		return -ENOMEM;

	pthread_mutex_init(&msg->lock, NULL);
	pthread_cond_init(&msg->cond, NULL);
	msg->busy = n_entries;
	msg->status = 0;
	msg->priv_data = NULL;

	const kvs_delete_context ctx = {
		.option = {
			.kvs_delete_error = false
		},
		.private2 = msg
	};

	kvs_key k[n_entries];

	for (size_t i = 0; i < n_entries; i++) {
		iobuf_t* key = keys + i;
		/* Transform the key adding the type tag field */
		iobuf_t c_key;
		err = kvsadi_compose_key(ttag, *key, &c_key);
		if (err) {
			log_error(lg, "kvsadi_compose_key() error %d", err);
			break;
		}
		fl_append(&free_list, c_key.base);
		k[i].key = c_key.base;
		k[i].length = c_key.len;

		kvs_result res = kvs_delete_tuple_async(h->con_hdl, k + i, &ctx, kvssd_del_callback);
		if (res != KVS_SUCCESS) {
			log_error(lg, "kvs_delete_tuple_async() error 0x%X at index %lu", res, i);
			err = -EIO;
			break;
		}
	}
	msg->ts = get_timestamp_us();
	pthread_mutex_lock(&msg->lock);
	while (msg->busy && !err) {
		err = pthread_cond_wait(&msg->cond, &msg->lock);
		if (err) {
			log_error(lg, "pthread_cond_wait error %d", err);
			break;
		}
	}
	pthread_mutex_unlock(&msg->lock);

	fl_destroy(free_list);
	if (!err) {
		err = msg->status;
		if (err) {
			log_error(lg, "kv_delete() completion error 0x%X",err);
			err = -EIO;
		}
	}
	free(msg);
	return err;
}

/**
 * Destroy the backend content
 *
 * @param handle [in] a backend handle
 * @returns 0 on success or error code otherwise
 */
static int
kvsadi_sanitize(kvs_backend_handle_t handle) {
	int err = 0;
	kvssd_handle_t* h = handle;
	assert(h);

	if (h->term)
		return -ENODEV;

	if (!h->emul) {
		char cmd[PATH_MAX];
		sprintf(cmd, "%s format %s", NVME_CMD, h->path);
		err = system(cmd);
	}
	return err;
}

/**
 * Retrieve backend information.
 *
 * @param handle [in]   a backend handle
 * @param bm_info [out] a backend information
 * @returns 0 on success or error code otherwise
 */
static int
kvssd_info(kvs_backend_handle_t handle, kvs_backend_info_t* bm_info) {
	kvssd_handle_t* h = handle;
	kvs_container info;
	int32_t len = 0;

	if (h->term)
		return -ENODEV;

	int64_t cap = 0;
	kvs_get_device_capacity(h->dev_hdl, &cap);
	bm_info->capacity = cap;
	bm_info->del_bulk_size = KVSADI_MAX_BATCH_SIZE;
	bm_info->put_bulk_size = KVSADI_MAX_BATCH_SIZE;
	bm_info->value_aligment = 8;
	bm_info->flags = KVS_FLAG_NO_MMAP_VALUE;

	kvs_get_min_value_length(h->dev_hdl, &len);
	bm_info->min_value_size = len;
	return 0;
}

/* Backend interface instance
 * The variable name has to be composed as <backend_name>_vtbl
 */
kvs_backend_t kvssd_vtbl = {
	.name = "kvssd",
	.init = kvssd_init,
	.info = kvssd_info,
	.exit = kvssd_exit,
	.get = kvssd_get,
	.put = kvssd_put,
	.remove = kvssd_delete,
	.erase = kvsadi_sanitize
};

/**
 * The KVS ADI iterators aren't used in the current RTKVS version.
 * NOTE: Outdated. Has to be re-implemented using KV API
 */
#if 0
static void
/* Return 0 if exists, -ENOENT if not exist, other code on error */
int
kvsadi_exist(kvsadi_handle_t* h,  int8_t ttag, iobuf_t key) {
	kv_namespace_handle ns_hdl = NULL;
	assert(h);
	if (h->term)
		return -ENODEV;
	get_namespace_default(h->dev_hdl, &ns_hdl);
	kvadi_msg_t* msg = malloc(sizeof(kvadi_msg_t));
	if (!msg)
		return -ENOMEM;

	pthread_mutex_init(&msg->lock, NULL);
	pthread_cond_init(&msg->cond, NULL);
	msg->busy = 1;
	msg->status = 0;
	msg->priv_data = NULL;
	kv_postprocess_function pfn = {.post_fn = put_post_fn, .private_data = msg};

	/* Transform the key adding the type tag field */
	iobuf_t c_key;
	int err = kvsadi_compose_key(ttag, key, &c_key);
	if (err) {
		log_error(lg, "kvsadi_compose_key() error %d", err);
		free(msg);
		return err;
	}
	kv_key k = {.key = c_key.base, .length = c_key.len};

	uint8_t buf;

	pthread_mutex_lock(&h->queue[_Q_EXIST].lock);
	kv_result res = kv_exist(h->queue[_Q_EXIST].s, ns_hdl, &k,
		1, 1, &buf, &pfn);
	pthread_mutex_unlock(&h->queue[_Q_EXIST].lock);

	if (res != KV_SUCCESS) {
		free(msg);
		free(c_key.base);
		log_error(lg, "kv_exist() error 0x%X", res);
		return -EIO;
	}

	pthread_mutex_lock(&msg->lock);
	while (msg->busy) {
		err = pthread_cond_wait(&msg->cond, &msg->lock);
		if (err)
			break;
	}
	pthread_mutex_unlock(&msg->lock);
	if (err)
		log_error(lg, "pthread_cond_wait() error %d", err);
	else {
		err = msg->status;
		if (!err) {
			err = buf ? 0 : -ENOENT;
		} else {
			log_error(lg, "kv_exist() statue error 0x%X", err);
		}
	}
	free(msg);
	free(c_key.base);
	return err;
}

iter_post_fn(kv_io_context *op) {

	if (op->opcode == KV_OPC_FLUSH)
		return;
	kvadi_msg_t* msg = op->private_data;
	assert(msg);
	if (op->opcode == KV_OPC_OPEN_ITERATOR) {
		kvsadi_iter_t* iter = msg->priv_data;
		iter->iter = op->result.hiter;
	}
	if (msg->status == KV_WRN_MORE)
		msg->status = KV_SUCCESS;

	if (--msg->busy == 0 || op->retcode != KV_SUCCESS) {
		pthread_mutex_lock(&msg->lock);
		msg->status = op->retcode;
		pthread_cond_signal(&msg->cond);
		pthread_mutex_unlock(&msg->lock);
	}
}


/* Returns 0 on success, -ENOSPC if no more iterators available
 *
 * IMPORTANT: the iterator doesn't aware of length of values whose real size
 * less than min_value, it always set them to KVSADI_MIN_VALUE_SIZE
 * NOTE: set  "keylen_fixed = false" in kvssd_emul.conf to get iterator buffer's
 * format as in specification. Otherwise key_lenght won't be provided
 **/
int
kvsadi_iterator_create(kvsadi_handle_t* h, kvsadi_iter_t** iter_out, int8_t ttag,
	size_t batch_size, int need_values) {
	assert(h);
	assert(iter_out);

	kvsadi_iter_t* iter = malloc(sizeof(*iter));
	if (!iter)
		return -ENOMEM;
	iter->h = h;
	iter->ttag = ttag;
	iter->need_values = need_values;
	iter->batch_size = batch_size;

	kv_namespace_handle ns_hdl = NULL;
	get_namespace_default(h->dev_hdl, &ns_hdl);

	kvadi_msg_t* msg = malloc(sizeof(kvadi_msg_t));
	if (!msg)
		return -ENOMEM;

	pthread_mutex_init(&msg->lock, NULL);
	pthread_cond_init(&msg->cond, NULL);
	msg->busy = 1;
	msg->status = 0;
	msg->priv_data = iter;
	kv_postprocess_function pfn = {.post_fn = iter_post_fn, .private_data = msg};
	kv_iterator_option opt = need_values ? KV_ITERATOR_OPT_KV : KV_ITERATOR_OPT_KEY;
	kv_group_condition cond = { .bitmask = 0x0F, .bit_pattern = ttag };
	kv_result rc = kv_open_iterator(h->queue[_Q_ITERATE].s, ns_hdl, opt, &cond, &pfn);
	if (rc != KV_SUCCESS) {
		log_warn(lg, "kv_open_iterator() returned 0x%X", rc);
		free(iter);
		free(msg);
		return rc == KV_ERR_QUEUE_MAX_QUEUE ? -EBUSY : -EIO;
	}
	msg->ts = get_timestamp_us();
	int err = 0;
	pthread_mutex_lock(&msg->lock);
	while (msg->busy && !msg->status) {
		err = pthread_cond_wait(&msg->cond, &msg->lock);
		if (err)
			break;
	}
	pthread_mutex_unlock(&msg->lock);
	if (err)
		log_error(lg, "pthread_cond_wait() error %d", err);
	else {
		err = msg->status;
		if (err) {
			log_error(lg, "kv_open_iterator status error 0x%X", err);
			err = -EIO;
		}
	}
	free(msg);

	if (!err) {
#if KVSADI_FIXED_KEY_SIZE
		iter->buffer_size = batch_size*(KVSADI_FIXED_KEY_SIZE+2);
#else
		iter->buffer_size = batch_size*KVSADI_MAX_KEY_SIZE;
#endif
		if (need_values)
			iter->buffer_size = batch_size*KVSADI_MAX_VALUE_SIZE;
		iter->buffer = malloc(iter->buffer_size);
		if (!iter->buffer) {
			free(iter);
			return -ENOMEM;
		}
		*iter_out = iter;
	} else {
		free(iter);
	}
	return rc;
}

int
kvsadi_iterator_next(kvsadi_iter_t* iter, rtbuf_t* keys, rtbuf_t* values) {
	kvsadi_handle_t* h = iter->h;

	kv_iterator_list il = {
		.num_entries = iter->batch_size,
		.size = iter->buffer_size,
		.end = 0,
		.it_list = iter->buffer
	};

	kv_namespace_handle ns_hdl = NULL;
	get_namespace_default(h->dev_hdl, &ns_hdl);

	kvadi_msg_t* msg = malloc(sizeof(kvadi_msg_t));
	if (!msg)
		return -ENOMEM;

	pthread_mutex_init(&msg->lock, NULL);
	pthread_cond_init(&msg->cond, NULL);
	msg->busy = 1;
	msg->status = 0;
	msg->priv_data = iter;
	kv_postprocess_function pfn = {.post_fn = iter_post_fn, .private_data = msg};
	memset(iter->buffer, 0, iter->buffer_size);

	pthread_mutex_lock(&h->queue[_Q_ITERATE].lock);
	kv_result rc = kv_iterator_next(h->queue[_Q_ITERATE].s, ns_hdl, iter->iter,
		&il, &pfn);
	pthread_mutex_unlock(&h->queue[_Q_ITERATE].lock);
	if (rc != KV_SUCCESS) {
		log_error(lg, "kv_iterator_next() error 0x%X", rc);
		free(msg);
		return rc;
	}
	msg->ts = get_timestamp_us();
	int err = 0;
	pthread_mutex_lock(&msg->lock);
	while (msg->busy && !msg->status) {
		err = pthread_cond_wait(&msg->cond, &msg->lock);
		if (err)
			break;
	}
	pthread_mutex_unlock(&msg->lock);
	if (err)
		log_error(lg, "pthread_cond_wait() error %d", err);
	else {
		err = msg->status;
		if (err) {
			log_error(lg, "kv_iterator_next() status error 0x%X", err);
			err = -EIO;
		}
	}
	free(msg);
	if (err)
		return err;
	/* Prepare buffers for a quick insert */
	err = rtbuf_expand(keys, il.num_entries);
	if (err)
		return err;
	if (iter->need_values) {
		rtbuf_expand(values, il.num_entries);
		if (err)
			return err;
	}

	size_t pos = 0;
	char* buff = il.it_list;
	for (size_t n = 0; n < il.num_entries; n++) {
		int8_t tt = -1;
		/* Fetch a key */
#if !KVSADI_FIXED_KEY_SIZE
		kv_key_t keylength = *(kv_key_t*)(buff + pos); pos += sizeof(kv_key_t);
#else
		kv_key_t keylength = KVSADI_FIXED_KEY_SIZE + 2;
#endif
		iobuf_t ub_key = { .base = buff + pos, .len = keylength }; pos += keylength;
		iobuf_t ub_nkey;

		int err = kvsadi_decompose_key(&tt, ub_key, &ub_nkey);
		if (err) {
			log_error(lg, "kvsadi_decompose_key() error %d", err);
			return err;
		}
		assert(tt == iter->ttag);
		err = rtbuf_set_alloc(keys, n, &ub_nkey, 1);
		if (err) {
			free(ub_nkey.base);
			return err;
		}
		if (!iter->need_values)
			continue;
		/* Fetch value */
		kv_value_t vallength = *(kv_value_t*)(buff + pos); pos += sizeof(kv_value_t);
		iobuf_t ub_val = {.len = vallength };
		ub_val.base = malloc(vallength);
		if (!ub_val.base)
			return -ENOMEM;
		memcpy(ub_val.base, buff + pos, vallength); pos+= vallength;
		err = rtbuf_set_alloc(values, n, &ub_val, 1);
		if (err) {
			free(ub_val.base);
			return -ENOMEM;
		}
	}
	return il.end ? 0 : -EAGAIN;
}

int
kvsadi_iterator_destroy(kvsadi_iter_t* iter) {
	kvsadi_handle_t* h = iter->h;
	kv_namespace_handle ns_hdl = NULL;
	get_namespace_default(h->dev_hdl, &ns_hdl);

	kvadi_msg_t* msg = malloc(sizeof(kvadi_msg_t));
	if (!msg)
		return -ENOMEM;

	pthread_mutex_init(&msg->lock, NULL);
	pthread_cond_init(&msg->cond, NULL);
	msg->busy = 1;
	msg->status = 0;
	kv_postprocess_function pfn = {.post_fn = iter_post_fn, .private_data = msg};
	pthread_mutex_lock(&h->queue[_Q_ITERATE].lock);
	kv_result rc = kv_close_iterator(h->queue[_Q_ITERATE].s, ns_hdl, iter->iter, &pfn);
	pthread_mutex_unlock(&h->queue[_Q_ITERATE].lock);

	if (rc != KV_SUCCESS) {
		log_error(lg, "kv_close_iterator() error 0x%X", rc);
		free(msg);
		return -EIO;
	}

	int err = 0;
	pthread_mutex_lock(&msg->lock);
	while (msg->busy && !msg->status) {
		err = pthread_cond_wait(&msg->cond, &msg->lock);
		if (err)
			break;
	}
	pthread_mutex_unlock(&msg->lock);
	if (err)
		log_error(lg, "pthread_cond_wait() error %d", err);
	else {
		err = msg->status;
		if (err) {
			log_error(lg, "kv_close_iterator() status error %d", err);
			err = -EIO;
		}
	}
	free(msg);
	if (!err) {
		free(iter->buffer);
		free(iter);
	}
	return err;
}
#endif
