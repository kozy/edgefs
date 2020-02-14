#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#include "geolock.h"

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
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <server_id> <server_ip>\n", argv[0]);
		exit(1);
	}
	err = sscanf(argv[1], "%d", &client.srv_id);
	if (err == 0) {
		fprintf(stderr, "Expecting server id to be an integer\n");
		exit(1);
	}
	snprintf(client.srv_ipaddr, INET_ADDRSTRLEN, "%s", argv[2]);

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
		fprintf(stderr, "Failed to create global lock table\n");
		exit(1);
	}

	/* Insert lock record for an object */
	memset(&lock_rec, 0, sizeof lock_rec);
	strcpy(lock_rec.path, "cltest/test/bk1/lockobj");
	strcpy(lock_rec.nhid, "nhid-1");
	strcpy(lock_rec.vmchid, "vmchid-1");
	lock_rec.genid = 1;
	lock_rec.uvid = 1;
	lock_rec.segid = 1;
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
		printf("Record\n====\n");
		printf("%s,\n", db_rec.path);
		printf("%lu,\n", db_rec.genid);
		printf("%lu,\n", db_rec.uvid);
		printf("%u,\n", db_rec.deleted);
		printf("%s,\n", db_rec.nhid);
		printf("%s,\n", db_rec.vmchid);
		printf("%lu,\n", db_rec.segid);
		printf("%u,\n", db_rec.size);
		printf("%lu,\n", db_rec.lock_time);
		printf("%u\n", db_rec.lock_state);
		printf("=====\n");
	}

	/* Check if record is locked */
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

	printf("Deleting lock table\n");
	test_geolock_delete_tbl(&client);

	cdq_stop(&client);
}
