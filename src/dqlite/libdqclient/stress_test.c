#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <sys/time.h>
#include <pthread.h>

#include "dqclient.h"

#define MAX_ITER	10000
#define NUM_THREADS	4

enum sql_op {
	INSERT_OP = 1,
	QUERY_OP,
	DELETE_OP,
	UPDATE_OP
};

char sql_op_str[4][16] = \
{
	"INSERT",
	"QUERY",
	"DELETE",
	"UPDATE"
};

static char *srv_ipaddr_port;
static int srv_id;

static pthread_t stress_threads[NUM_THREADS];

static char *create_tbl_stmt =
		"CREATE TABLE IF NOT EXISTS stress_db (key text, value text, PRIMARY KEY (key))";
static char *insert_tbl_stmt =
		"INSERT INTO stress_db (key, value) VALUES('%s', '%s')";
static char *update_tbl_stmt =
		"UDPATE stress_db SET value = '%s' WHERE key = '%s'";
static char *delete_tbl_stmt =
		"DELETE FROM stress_db WHERE key = '%s'";
static char *query_tbl_stmt =
		"SELECT value FROM stress_db WHERE key = '%s'";
static char *delete_all_tbl_stmt =
		"DELETE FROM stress_db";

static int
test_setup(struct cdq_client *client, int thr_id)
{
	int err;
	unsigned stmt_id, last_insert_id, rows_affected;
	struct timeval tv;
	char dbname[64];

	err = cdq_start(client);
	if (err !=0) {
		fprintf(stderr, "Failed to start client\n");
		exit(1);
	}
	printf("Started client ...\n");

	sprintf(dbname, "stress_test_%d.db", thr_id);
	err = cdq_db_open(client, dbname);
	if (err != 0) {
		fprintf(stderr, "Failed to open db - %s\n", dbname);
		exit(1);
	}
	printf("Open DB - %s successful ...\n", dbname);

	printf("Creating table ...\n");
	err = cdq_prepare_stmt(client, create_tbl_stmt, &stmt_id);
	if (err != 0) {
		fprintf(stderr, "Failed to prepate create table statement\n");
		goto err_;
	}

	err = cdq_exec_stmt(client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Failed to create table, err: %d\n", err);
		goto err_;
	}
	printf("Table created ...\n");
	gettimeofday(&tv, NULL);
	srand((unsigned int)tv.tv_usec);
	return 0;

err_:
	cdq_stop(client);
	return -1;
}

static void
test_term(struct cdq_client *client)
{
	int err;
	unsigned stmt_id, last_insert_id, rows_affected;

	printf("Dropping table ...\n");
	err = cdq_prepare_stmt(client, delete_all_tbl_stmt, &stmt_id);
	if (err != 0) {
		fprintf(stderr, "Failed to prepate delete table statement\n");
	}

	err = err ? err : cdq_exec_stmt(client, stmt_id,
					&last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Failed to delete table, err: %d\n", err);
	} else {
		printf("Table deleted ...\n");
	}
	cdq_stop(client);
}

static void *
db_test(void *arg)
{
	int op;
	int thr_id = (intptr_t) arg;
	unsigned stmt_id, last_insert_id, rows_affected;
	struct cdq_client client;
	char stmt[1024], key[32], value[32];
	int err, i;


	if (MAX_ITER < 1)
		return NULL;

	client.srv_id = srv_id;
	sprintf(client.srv_ipaddr, "%s", srv_ipaddr_port);

	err = test_setup(&client, thr_id);
	if (err != 0) {
		printf("Unable to create client ...\n");
		return NULL;
	}

	/* Wait for all other threads to start */
	sleep(1);

	/* Choose operation - INSERT, QUERY or DELETE */
	for (i = 0; i < MAX_ITER; i++) {

		op = 1 + rand() % 3;

		sprintf(key, "key-%d", i);
		sprintf(value, "value-%d", i);

		switch(op) {
			case INSERT_OP:
				sprintf(stmt, insert_tbl_stmt, key, value);
				break;
			case UPDATE_OP:
				sprintf(stmt, update_tbl_stmt, value, key);
				break;
			case QUERY_OP:
				sprintf(stmt, query_tbl_stmt, key);
				break;
			case DELETE_OP:
				sprintf(stmt, delete_tbl_stmt, key);
				break;
			default:
				printf("Unknown SQL op. Quitting\n");
				return NULL;
				break; /* Not reached */
		}

		err = cdq_prepare_stmt(&client, "BEGIN", &stmt_id);
		err = err ? err : cdq_exec_stmt(&client, stmt_id,
					&last_insert_id, &rows_affected);
		err = cdq_prepare_stmt(&client, stmt, &stmt_id);
		if (err != 0) {
			cdq_prepare_stmt(&client, "ROLLBACK", &stmt_id);
			cdq_exec_stmt(&client, stmt_id,
					&last_insert_id, &rows_affected);
			fprintf(stderr, "Unable to prepare %s statement\n",
					stmt);
			continue;
		}
		err = cdq_exec_stmt(&client, stmt_id,
				&last_insert_id, &rows_affected);
		if (err != 0) {
			cdq_prepare_stmt(&client, "ROLLBACK", &stmt_id);
			cdq_exec_stmt(&client, stmt_id,
					&last_insert_id, &rows_affected);
			fprintf(stderr, "Failed to to execute SQL\n");
			continue;
		}
		err = cdq_prepare_stmt(&client, "COMMIT", &stmt_id);
		err = err ? err : cdq_exec_stmt(&client, stmt_id,
				&last_insert_id, &rows_affected);
		if (err != 0) {
			cdq_prepare_stmt(&client, "ROLLBACK", &stmt_id);
			cdq_exec_stmt(&client, stmt_id,
					&last_insert_id, &rows_affected);
			fprintf(stderr, "Failed to commit transaction\n");
			continue;
		}
		printf("%s successful...\n", stmt);
	}
	test_term(&client);
	return NULL;
}

static void
stress_test()
{
	int i;

	for (i = 0; i < NUM_THREADS; i++) {
		printf("Starting thread %d\n", i);
		pthread_create(&stress_threads[i], NULL,
				db_test, (void *)(intptr_t)i);
	}

	for (i = 0; i < NUM_THREADS; i++) {
		pthread_join(stress_threads[i], NULL);
	}
}

int
main(int argc, char **argv)
{
	int err;

	if (argc != 3) {
		fprintf(stderr, "Usage: %s <server_id> <server_ip>\n", argv[0]);
		exit(1);
	}
	err = sscanf(argv[1], "%d", &srv_id);
	if (err == 0) {
		fprintf(stderr, "Expecting server id to be an integer\n");
		exit(1);
	}
	srv_ipaddr_port = argv[2];

	stress_test();
}
