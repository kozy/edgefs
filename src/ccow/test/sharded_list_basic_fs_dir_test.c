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
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

#include "cmocka.h"
#include "common.h"
#include "ccow.h"
#include "ccowd.h"
#include "ccowfsio.h"

#define NUMBER_OF_FILES 200
#define READDIR_LIMIT 30
#define	TEST_BID "test"
#define	TEST_OID "sharded_list_basic_fs_dir_test_oid"
#define	ENTRY_SIZE 127
#define TEST_BUCKET_NAME "sharded-list-bucket-test"

/* Test plan:
 * 1. Create directory like sharded list.
 * 2. Fill it with NUMBER_OF_FILES entries.
 * 3. Remove topmost entry.
 * 4. Read list, by READDIR_LIMIT entries, up to end.
 * 5. Verify to have no removed entries.
 * 6. Verify to have all entries left.
 * 7. Verify to have no duplicates.
 * 8. Repeat 2-7 with defferent CCOW context.
 * 9. Repeat 2-7 with same CCOW context, but different threads.
 * 10. Repeat 2-7 with defferent CCOW context and different thread.
 *
 * TODO: Random delete.
 */

int ccow_mh_immdir = 0;
int verbose = 0;
int dd = 0;
uint64_t global_i = 0;
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

/* Two CCOW contexts to test operations from different context. */
ccow_t tc[2];
/*
 * Directory list context same for both CCOW contexts, since it is only
 * info/naming initialization.
 */
ccow_shard_context_t dir_list_context;

char * bid = TEST_BID;
int bid_size = sizeof(TEST_BID);
char * oid = TEST_OID;
int oid_size = sizeof(TEST_OID);

static void
dir_create(void **state)
{
	ccow_completion_t c;
	struct iovec iov[2];
	int err;

	err = ccow_shard_context_create(oid, oid_size, FSIO_DIR_SHARD_COUNT,
	    &dir_list_context);
	assert_int_equal(err, 0);

	/*
	 * Directories are eventual in our model if X-MH-ImmDir == 0 (default).
	 */
	if (ccow_mh_immdir == 0) {
		printf("%s context set eventual: %s", __func__, oid);
		ccow_shard_context_set_eventual(dir_list_context, 1, 1);
	}

	ccow_shard_context_set_overwrite(dir_list_context, 0);

	err = ccow_sharded_list_create(tc[0], bid, bid_size, dir_list_context);
	assert_int_equal(err, 0);

	iov[0].iov_base = ".";
	iov[0].iov_len = strlen(iov[0].iov_base) + 1;
	iov[1].iov_base = "Just some test binary data";
	iov[1].iov_len = strlen(iov[1].iov_base) + 1;

	err = ccow_sharded_list_put_v2(tc[0], bid, bid_size, dir_list_context,
	    iov, 2, CCOW_CONT_F_INSERT_LIST_OVERWRITE);
	assert_int_equal(err, 0);
}

static void
dir_delete(void **state)
{
	int err;

	err = ccow_sharded_list_delete(tc[0], bid, bid_size, dir_list_context,
	    ".", strlen(".") + 1);
	assert_int_equal(err, 0);
	err = ccow_sharded_list_destroy(tc[0], bid, bid_size, dir_list_context);
	assert_int_equal(err, 0);
	ccow_shard_context_destroy(&dir_list_context);
}

static void
create_file(int i)
{
	struct iovec iov[2];
	char name[8];
	int err;

	sprintf(name, "file%d", i);
	iov[0].iov_base = name;
	iov[0].iov_len = strlen(name) + 1;
	iov[1].iov_base = "File attributes binary data";
	iov[1].iov_len = strlen(iov[1].iov_base) + 1;

	assert_non_null(tc[0]);
	assert_non_null(dir_list_context);
	/* add entry always adds to size and link count */
	err = ccow_sharded_list_put_with_md(tc[0], bid, bid_size,
		dir_list_context, oid, oid_size, name, strlen(name) + 1,
		iov, 2, ENTRY_SIZE, 1, 0);
	assert_int_equal(err, 0);
}

static void
delete_file(ccow_t tc, int i)
{
	char name[8];
	int err;

	sprintf(name, "file%d", i);
	/* remove entry always subtracts from size and link count */
	err = ccow_sharded_list_delete_with_md(tc, bid, bid_size,
	    dir_list_context, oid, oid_size,
	    name, strlen(name) + 1, -1 * ENTRY_SIZE, -1, 0);
	assert_int_equal(err, 0);
}

static void
fill_dir()
{
	int i;

	for (i = 0; i < NUMBER_OF_FILES; i++) {
		create_file(i);
	}
}

static int
readdir(char **list, int max, char *cookie)
{
	struct ccow_metadata_kv *kv = NULL;
	ccow_lookup_t iter = NULL;
	int err, pos = 0, read_count = 0;

	if (cookie == NULL)
		err = ccow_sharded_get_list(tc[0], bid, bid_size,
		    dir_list_context, NULL, 0, NULL, max, &iter);
	else
		err = ccow_sharded_get_list(tc[0], bid, bid_size,
		    dir_list_context, cookie, strlen(cookie) + 1, NULL, max,
		    &iter);
	assert_int_equal(err, 0);
	assert_non_null(iter);

	while ((kv = ccow_lookup_iter(iter, CCOW_MDTYPE_NAME_INDEX,
			    pos++)) != NULL && read_count < max) {

		if (kv->value == NULL || kv->key == NULL) {
			continue;
		}

		if (cookie && strcmp(kv->key, cookie) == 0)
			continue;

		strcpy(list[read_count], kv->key);
		read_count++;
	}

	ccow_lookup_release(iter);
	return (read_count);
}

static void
verify(int x)
{
	int d, entry[NUMBER_OF_FILES], fail, i, l, n;
	char cookie[8], *list[READDIR_LIMIT], *buff;

	buff = (char *)malloc(8 * READDIR_LIMIT);
	assert_non_null(buff);
	bzero(buff, 8 * READDIR_LIMIT);
	bzero(entry, sizeof(entry));
	for (i = 0; i < READDIR_LIMIT; i++) {
		list[i] = &buff[i * 8];
	}
	fail = 0;
	cookie[0] = '\0';
	/* Count entries in shard list. */
	for (l = x; l < NUMBER_OF_FILES; l += READDIR_LIMIT) {
		n = readdir(list, READDIR_LIMIT,
		    ((cookie[0] == '\0')?NULL:cookie));
		for (i = 0; i < n; i++) {
			/* Skip '.' and '..' entries. */
			if (list[i][0] == '.') {
				if (list[i][1] == '\0')
					continue;
				if ((list[i][1] == '.') && (list[i][2] == '\0'))
					continue;
			}
			d = strtol(list[i] + 4, NULL, 10);
			/* Check for invalid entries. */
			assert(d >= 0 && d < NUMBER_OF_FILES);
			entry[d]++;
		}
		strcpy(cookie, list[i - 1]);
	}
	/* Check for missing and duplicate entries. */
	for (l = x + 1; l < NUMBER_OF_FILES; l++) {
		if (entry[l] == 0) {
			printf("ERROR: Missing entry \"file%d\"\n", l);
			fail++;
		} else if (entry[l] > 1) {
			printf("ERROR: %d duplicating entries of \"file%d\"\n",
			    entry[l], l);
			fail++;
		}
	}
	/* Check for deleted entries. */
	for (l = 0; l <= x; l++) {
		if (entry[l] > 0) {
			printf("ERROR: Found %d times of deleted entry of "
			    "\"file%d\"\n", entry[l], l);
			fail++;
		}
	}
	assert_int_equal(fail, 0);
}

static void
test_single_thread_same_context()
{
	int i;

	fill_dir();

	for (i = 0; i < NUMBER_OF_FILES; i++) {
		delete_file(tc[0], i);
		verify(i);
	}
}

static void
lock()
{

	pthread_mutex_lock(&mtx);
}

static void
unlock()
{

	pthread_mutex_unlock(&mtx);
}

static void *
verifing_thread(void *arg)
{
	uint64_t i;

	while (true) {
		/* Wait for parent. */
		lock();
		/* Check if it time to exit. */
		if (global_i == UINT64_MAX) {
			unlock();
			return (NULL);
		}
		/*
		 * Verify if we have file$(i + 1) to file $NUMBER_OF_FILES
		 * and there is no duplicates.
		 */
		verify((int)global_i);
		/* Return control to parent. */
		unlock();
	}

	return (NULL);
}

static void
test_another_thread_same_context()
{
	pthread_t thrd;
	int i;

	fill_dir();
	/* Prevent future child thread from start its job yet. */
	lock();
	pthread_create(&thrd, NULL, verifing_thread, NULL);

	for (i = 0; i < NUMBER_OF_FILES; i++) {
		delete_file(tc[0], i);
		/* Notify child thread with new number. */
		global_i = (uint64_t)i;
		/* Unblock child thread. */
		unlock();
		lock();
	}

	/* Notify child thread to terminate. */
	global_i = UINT64_MAX;
	/* Let child to go to exit. */
	unlock();
	pthread_join(thrd, NULL);

}

static void
test_single_thread_another_context()
{
	int i;

	fill_dir();

	for (i = 0; i < NUMBER_OF_FILES; i++) {
		delete_file(tc[1], i);
		verify(i);
	}
}

static void
test_another_thread_another_context()
{
	pthread_t thrd;
	int i;

	fill_dir();
	/* Prevent future child thread from start its job yet. */
	lock();
	pthread_create(&thrd, NULL, verifing_thread, NULL);

	for (i = 0; i < NUMBER_OF_FILES; i++) {
		delete_file(tc[1], i);
		global_i = i;
		/* Unblock child thread. */
		unlock();
		lock();
	}

	/* Notify child thread to terminate. */
	global_i = UINT64_MAX;
	/* Let child to go to exit. */
	unlock();
	pthread_join(thrd, NULL);
}

// ----------------------------------------------------------------------------
// setup and tear down functions
// ----------------------------------------------------------------------------
static void
libccowd_setup(void **state) {
	if (!dd) {
		assert_int_equal(ccow_daemon_init(NULL), 0);
		usleep(2 * 1000000L);
	}
}

static void
get_tenant(ccow_t *tn, char *cid, char *tid)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/etc/ccow/ccow.json", nedge_path());
	int fd = open(path, O_RDONLY);
	assert_true(fd >= 0);
	char *buf = je_calloc(1, 16384);
	assert_non_null(buf);
	assert_true(read(fd, buf, 16383) != -1);
	assert_int_equal(close(fd), 0);
	assert_int_equal(ccow_tenant_init(buf, cid, strlen(cid) + 1,
	    tid, strlen(tid) + 1, tn), 0);
	je_free(buf);
}

static void
libccow_setup(void **state) {
	get_tenant(&tc[0], "cltest", "test");
	get_tenant(&tc[1], "cltest", "test");
}

static void
libccow_teardown(void **state)
{

	ccow_tenant_term(tc[0]);
	ccow_tenant_term(tc[1]);
}

static void
libccowd_teardown(void **state) {
	if (!dd) {
		ccow_daemon_term();
	}
}

// ----------------------------------------------------------------------------
// bucket create/delete
// ----------------------------------------------------------------------------
static void bucket_create(void **state) {
	assert_non_null(tc[0]);
	int err = ccow_bucket_create(tc[0], TEST_BUCKET_NAME,
	    strlen(TEST_BUCKET_NAME) + 1, NULL);
	if (err != -EEXIST)
		assert_int_equal(err, 0);
}

static void bucket_delete(void **state) {
	assert_non_null(tc[0]);
	int err = ccow_bucket_delete(tc[0], TEST_BUCKET_NAME,
	    strlen(TEST_BUCKET_NAME) + 1);
	if (err != -EEXIST)
		assert_int_equal(err, 0);
}

int
main(int argc, char **argv) {
	int opt;
	while ((opt = getopt(argc, argv, "nv")) != -1) {
		switch (opt) {
		case 'n':
			dd = 1;
			break;

		case 'v':
			verbose = 1;
			break;

		default:
			break;
		}
	}

	char* mh = getenv("CCOW_MH_IMMDIR");
	if (mh != NULL && strcmp(mh, "0") == 0) {
		ccow_mh_immdir = 0;
		printf("Context set eventual\n");
	} else {
		ccow_mh_immdir = 1;
		printf("Context set immediate\n");
	}

	const UnitTest tests[] = {
			unit_test(libccowd_setup),
			unit_test(libccow_setup),
			unit_test(bucket_create),
			unit_test(dir_create),
			unit_test(dir_delete),
			unit_test(dir_create),

			unit_test(test_single_thread_same_context),
			unit_test(test_another_thread_same_context),
			unit_test(test_single_thread_another_context),
			unit_test(test_another_thread_another_context),

			unit_test(dir_delete),
			unit_test(bucket_delete),
			unit_test(libccow_teardown),
			unit_test(libccowd_teardown)
	};
	return run_tests(tests);
}
