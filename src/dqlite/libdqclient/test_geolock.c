#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <time.h>

#include "geolock.h"

#define LOCK_COUNT 200
#define THREAD_COUNT 10

static void
print_lock_rec(geo_lock_t *lock_rec) {
	printf("Record\n====\n");
	printf("%s,\n", lock_rec->path);
	printf("genid: %lu,\n", lock_rec->genid);
	printf("uvid: %lu,\n", lock_rec->uvid);
	printf("deleted: %u,\n", lock_rec->deleted);
	printf("nhid: %s,\n", lock_rec->nhid);
	printf("vmchid: %s,\n", lock_rec->vmchid);
	printf("segid: %lu,\n", lock_rec->segid);
	printf("serverid: %lu,\n", lock_rec->serverid);
	printf("size: %lu,\n", lock_rec->size);
	printf("lock_time: %lu,\n", lock_rec->lock_time);
	printf("lock_state: %u\n", lock_rec->lock_state);
	printf("=====\n");
}

static void
test_one(struct cdq_client client, geo_lock_t *lock_rec, int i) {
	int err, is_locked = 0, rec_count = 0;
	geo_lock_t  db_rec;
	printf("\nAcquire lock %d:\n", i);
	sprintf(lock_rec->path,"cltest/test/bk1/testobj.%d", i*100);
	err = geolock_acquire_lock_rec(&client, lock_rec);
	printf("Acquire err: %d\n", err);
	if (err) {
		fprintf(stderr, "Aquire error: %d\n", err);
		exit(1);
	}

	err = geolock_get_lock_rec(&client, lock_rec->path, &db_rec, &rec_count);
	if (err) {
		fprintf(stderr, "Failed to fetch lock record after acquire err: %d\n", err);
		exit(1);
	}

	if (rec_count != 0) {
		printf("Fetched lock record after acquire\n");
		print_lock_rec(&db_rec);
	}

	/* Check if record is locked */
	err = geolock_is_locked(&client, lock_rec->path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}

	if (!is_locked) {
		fprintf(stderr, "Acquired record not locked\n");
		exit(2);
	}

	// Release lock record
	printf("\nRelease lock: %d\n", i);
	lock_rec->genid++;
	strcpy(lock_rec->vmchid, "vmchid-2_next");
	err = geolock_release_lock_rec(&client, lock_rec);
	if (err != 0) {
		fprintf(stderr, "Failed to relese lock err: %d\n", err);
		exit(1);
	}

	err = geolock_acquire_lock_rec(&client, lock_rec);
	printf("Acquire2 err: %d\n", err);
	if (err) {
		fprintf(stderr, "Aquire 2 error: %d\n", err);
		exit(1);
	}
	/* Check if record is locked */
	err = geolock_is_locked(&client, lock_rec->path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed 2 to fetch lock record\n");
		exit(1);
	}

	if (!is_locked) {
		fprintf(stderr, "Acquired 2 record not locked\n");
		exit(2);
	}

	err = geolock_release_lock_rec(&client, lock_rec);
	if (err != 0) {
		fprintf(stderr, "Failed 2 to relese lock err: %d\n", err);
		exit(1);
	}
	/* Check if record is locked */
	err = geolock_is_locked(&client, lock_rec->path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed 2 to fetch lock record\n");
		exit(1);
	}

	if (is_locked) {
		fprintf(stderr, "Released 2 record is locked\n");
		exit(2);
	}

	err = geolock_get_lock_rec(&client, lock_rec->path, &db_rec, &rec_count);
	if (err) {
		fprintf(stderr, "Failed 2 to fetch lock record after release err: %d\n", err);
		exit(1);
	}

	if (rec_count != 0) {
		printf("Fetched 2 lock record after release\n");
		print_lock_rec(&db_rec);
	}
}

typedef struct
{
	struct cdq_client *client;
	geo_lock_t *lock_rec;
	int i;
} thread_struct;

static void
*one_thread(void *data) {
	int err, is_locked = 0, rec_count = 0;
	geo_lock_t  db_rec;
	char path[128];
	geo_lock_t lock_rec;
	int i;

	thread_struct *t;
	t = (thread_struct *)data;
	i = t->i;
	memcpy(&lock_rec, t->lock_rec, sizeof lock_rec);

	printf("\n[%lu]Acquire lock %d:\n", pthread_self(), i);
	sprintf(lock_rec.path,"cltest/test/bk1/testobj.%d", i*100);
	err = geolock_acquire_lock_rec(t->client, &lock_rec);
	if (err) {
		fprintf(stderr, "[%lu]Aquire error: %d\n", pthread_self(), err);
		exit(1);
	}

	err = geolock_get_lock_rec(t->client, lock_rec.path, &db_rec, &rec_count);
	if (err) {
		fprintf(stderr, "[%lu]Failed to fetch lock record after acquire err: %d\n", pthread_self(), err);
		exit(1);
	}

	if (rec_count != 0) {
		printf("[%lu]Fetched lock record after acquire\n", pthread_self());
		print_lock_rec(&db_rec);
	}

	/* Check if record is locked */
	err = geolock_is_locked(t->client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "[%lu]Failed to fetch lock record\n", pthread_self());
		exit(1);
	}

	if (!is_locked) {
		fprintf(stderr, "[%lu]Acquired record not locked\n", pthread_self());
		exit(2);
	}

	// Release lock record
	printf("\n[%lu]Release lock: %d\n", pthread_self(), i);
	lock_rec.genid++;
	strcpy(lock_rec.vmchid, "vmchid-2_next");
	err = geolock_release_lock_rec(t->client, &lock_rec);
	if (err != 0) {
		fprintf(stderr, "[%lu]Failed to relese lock err: %d\n", pthread_self(), err);
		exit(1);
	}

	err = geolock_acquire_lock_rec(t->client, &lock_rec);
	printf("Acquire2 err: %d\n", err);
	if (err) {
		fprintf(stderr, "[%lu]Aquire 2 error: %d\n", pthread_self(), err);
		exit(1);
	}
	/* Check if record is locked */
	err = geolock_is_locked(t->client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "[%lu]Failed 2 to fetch lock record\n", pthread_self());
		exit(1);
	}

	if (!is_locked) {
		fprintf(stderr, "[%lu]Acquired 2 record not locked\n", pthread_self());
		exit(2);
	}

	err = geolock_release_lock_rec(t->client, &lock_rec);
	if (err != 0) {
		fprintf(stderr, "[%lu]Failed 2 to relese lock err: %d\n", pthread_self(), err);
		exit(1);
	}
	/* Check if record is locked */
	err = geolock_is_locked(t->client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "[%lu]Failed 2 to fetch lock record\n", pthread_self());
		exit(1);
	}

	if (is_locked) {
		fprintf(stderr, "[%lu]Released 2 record is locked\n", pthread_self());
		exit(2);
	}

	err = geolock_get_lock_rec(t->client, lock_rec.path, &db_rec, &rec_count);
	if (err) {
		fprintf(stderr, "[%lu]Failed 2 to fetch lock record after release err: %d\n", pthread_self(), err);
		exit(1);
	}

	if (rec_count != 0) {
		printf("[%lu]Fetched 2 lock record after release\n", pthread_self());
		print_lock_rec(&db_rec);
	}

	pthread_exit(0);
	return NULL;
}

static void
test_sequence(struct cdq_client client, geo_lock_t *lock_rec) {
	for (int i=0; i < LOCK_COUNT; i++) {
		test_one(client, lock_rec, i);
	}
}


static void
test_thread(struct cdq_client client, geo_lock_t *lock_rec) {
	pthread_t th[THREAD_COUNT];
	thread_struct id[THREAD_COUNT];
	int i;

	i = 0;
	while (i < LOCK_COUNT) {
		for (int t = 0; t < THREAD_COUNT; t++) {
			id[t].client = &client;
			id[t].i = i+t;
			id[t].lock_rec = lock_rec;
			if (id[t].i >= LOCK_COUNT)
				break;
			pthread_create(&th[t], NULL, one_thread, &id[t]);
		}

		for (int t = 0; t < THREAD_COUNT; t++) {
			if (id[t].i >= LOCK_COUNT)
				break;
			printf("Starting object %d\n", id[t].i);
			pthread_join(th[t], NULL);
		}


		i += THREAD_COUNT;
	}
}


int
main(int argc, char **argv)
{
	int err, is_locked = 0, rec_count = 0;
	unsigned stmt_id, last_insert_id = 0, rows_affected = 0;
	struct cdq_client client;
	struct rows rows;
	geo_lock_t lock_rec, db_rec;

	char *dbname = "locks.db";

	memset(&client, 0, sizeof client);
	if (argc < 3) {
		fprintf(stderr, "Usage: %s <server_id> <server_ip> [-t]\n", argv[0]);
		exit(1);
	}
	err = sscanf(argv[1], "%d", &client.srv_id);
	if (err == 0) {
		fprintf(stderr, "Expecting server id to be an integer\n");
		exit(1);
	}
	snprintf(client.srv_ipaddr, ADDRESS_MAX_LEN, "%s", argv[2]);

	err = cdq_start(&client);
	if (err !=0) {
		fprintf(stderr, "Failed to start client\n");
		exit(1);
	}
	printf("Started client ...\n");

	err = cdq_db_open(&client, dbname);
	if (err != 0) {
		fprintf(stderr, "Failed to open db - %s\n", dbname);
		exit(1);
	}
	printf("Open DB successful ...\n");

	/* Create global lock table */
	err = geolock_create_lock_tbl(&client);
	if (err != 0) {
		fprintf(stderr, "Failed to create global lock table err: %d\n", err);
		exit(1);
	}

    time_t seconds= time(NULL);

	/* Insert lock record for an object */
	memset(&lock_rec, 0, sizeof lock_rec);
	sprintf(lock_rec.path,"cltest/test/bk1/lockobj.%ld", (long) seconds);
	strcpy(lock_rec.nhid, "nhid-1");
	strcpy(lock_rec.vmchid, "vmchid-1");
	lock_rec.genid = 1;
	lock_rec.uvid = 1;
	lock_rec.segid = 1;
	lock_rec.serverid = 3;
	lock_rec.size = 100;

	err = geolock_insert_lock_rec(&client, &lock_rec);
	if (err != 0) {
		fprintf(stderr, "Failed to insert lock record\n");
		exit(1);
	}
	printf("Inserted lock record\n");

	/* Check if record is locked */
	err = geolock_is_locked(&client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}
	printf("Lock status : %s\n", is_locked ? "locked" : "unlocked");

	/* Lock the object */
	err = geolock_lock(&client, lock_rec.path, lock_rec.genid);
	if (err != 0) {
		fprintf(stderr, "Failed to SET lock record\n");
		exit(1);
	}
	printf("Changed lock record state to locked ...\n");

	memset(&db_rec, 0, sizeof db_rec);
	err = geolock_get_lock_rec(&client, lock_rec.path, &db_rec, &rec_count);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}

	if (rec_count != 0) {
		printf("Fetched lock record\n");
		print_lock_rec(&db_rec);
	}

	/* Check if record is locked */
	is_locked = 0;
	err = geolock_is_locked(&client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}
	printf("Lock status : %s\n", is_locked ? "locked" : "unlocked");

	/* Unlock the object */
	err = geolock_unlock(&client, lock_rec.path, lock_rec.genid);
	if (err != 0) {
		fprintf(stderr, "Failed to UNSET lock record\n");
		exit(1);
	}
	printf("Changed lock record state to unlocked ...\n");

	/* Check if record is locked */
	err = geolock_is_locked(&client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}
	printf("Lock status : %s\n", is_locked ? "locked" : "unlocked");

	memset(&db_rec, 0, sizeof db_rec);
	err = geolock_get_lock_rec(&client, lock_rec.path, &db_rec, &rec_count);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}

	if (rec_count != 0) {
		printf("Fetched lock record\n");
		print_lock_rec(&db_rec);
	}

	/* Acquire lock for an object */
	printf("\nAcquire lock:\n");
	memset(&lock_rec, 0, sizeof lock_rec);
	sprintf(lock_rec.path,"cltest/test/bk1/testobj.%ld", (long) seconds);
	strcpy(lock_rec.nhid, "nhid-2");
	strcpy(lock_rec.vmchid, "vmchid-2");
	lock_rec.genid = 2;
	lock_rec.uvid = 2;
	lock_rec.segid = 22;
	lock_rec.serverid = 33;
	lock_rec.size = 100;

	err = geolock_acquire_lock_rec(&client, &lock_rec);
	if (err) {
		fprintf(stderr, "Failed to acquire lock err: %d\n", err);
		exit(1);
	}

	/* Check if record is locked */
	err = geolock_is_locked(&client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}
	printf("Lock status : %s\n", is_locked ? "locked" : "unlocked");

	memset(&db_rec, 0, sizeof db_rec);
	err = geolock_get_lock_rec(&client, lock_rec.path, &db_rec, &rec_count);
	if (err) {
		fprintf(stderr, "Failed to fetch lock record err: %d\n", err);
		exit(1);
	}

	if (rec_count != 0) {
		printf("Fetched acquired lock record\n");
		print_lock_rec(&db_rec);
	}

	// try get lock again
	printf("\nAcquire lock again:\n");
	err = geolock_acquire_lock_rec(&client, &lock_rec);
	printf("Second acquire err: %d\n", err);
	if (err != -EBUSY) {
		fprintf(stderr, "Wrong error code: %d\n", err);
		exit(1);
	}

	// Release lock record
	printf("\nRelease lock:\n");
	lock_rec.genid++;
	strcpy(lock_rec.vmchid, "vmchid-2_next");
	err = geolock_release_lock_rec(&client, &lock_rec);
	if (err != 0) {
		fprintf(stderr, "Failed to relese lock\n");
		exit(1);
	}

	printf("\nRelease lock again:\n");
	lock_rec.genid++;
	strcpy(lock_rec.vmchid, "vmchid-2_next");
	err = geolock_release_lock_rec(&client, &lock_rec);
	if (err != 0) {
		fprintf(stderr, "Failed to relese lock\n");
		exit(1);
	}

	/* Check if record is locked */
	err = geolock_is_locked(&client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}
	printf("Lock status : %s\n", is_locked ? "locked" : "unlocked");

	memset(&db_rec, 0, sizeof db_rec);
	err = geolock_get_lock_rec(&client, lock_rec.path, &db_rec, &rec_count);
	if (err) {
		fprintf(stderr, "Failed to fetch lock record after release err: %d\n", err);
		exit(1);
	}

	if (rec_count != 0) {
		printf("Fetched lock record after release\n");
		print_lock_rec(&db_rec);
	}

	// Acquire lock again after relese
	printf("\nAcquire lock after release:\n");
	err = geolock_acquire_lock_rec(&client, &lock_rec);
	printf("After release acquire err: %d\n", err);

	if (!err) {
		printf("Acquired lock record:\n");
		print_lock_rec(&lock_rec);
	}

	/* Check if record is locked */
	err = geolock_is_locked(&client, lock_rec.path, &is_locked);
	if (err != 0) {
		fprintf(stderr, "Failed to fetch lock record\n");
		exit(1);
	}
	printf("Lock status : %s\n", is_locked ? "locked" : "unlocked");

	if (argc == 4 && strcmp(argv[3],"-t") == 0) {
		printf("\nThread test\n");
		test_thread(client, &lock_rec);
	} else {
		printf("\nSequence test\n");
		test_sequence(client, &lock_rec);
	}


	printf("Deleting lock table\n");
	test_geolock_delete_tbl(&client);

	cdq_stop(&client);
}
