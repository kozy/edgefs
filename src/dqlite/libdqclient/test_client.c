#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#include "dqclient.h"

static char *create_tbl_stmt =
		"CREATE TABLE IF NOT EXISTS test (n INT)";
static char *update_tbl_stmt =
		"INSERT INTO test (n) VALUES(123)";
static char *query_tbl_stmt =
		"SELECT n FROM test";

static char stmt[4096];

int
main(int argc, char **argv)
{
	int err;
	unsigned stmt_id, last_insert_id = 0, rows_affected = 0;
	struct cdq_client client;
	struct rows rows;

	char *dbname = "test.db";

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
	client.srv_ipaddr = strdup(argv[2]);

	err = cdq_start(&client);
	if (err !=0) {
		fprintf(stderr, "Failed to start client\n");
		free(client.srv_ipaddr);
		exit(1);
	}
	printf("Started client ...\n");

	err = cdq_db_open(&client, dbname);
	if (err != 0) {
		fprintf(stderr, "Failed to open db - %s\n", dbname);
		free(client.srv_ipaddr);
		exit(1);
	}
	printf("Open DB successful ...\n");

	printf("Creating table ...\n");
	err = cdq_prepare_stmt(&client, create_tbl_stmt, &stmt_id);
	if (err != 0) {
		fprintf(stderr, "Failed to prepate create table statement\n");
		goto err_;
	}
	printf("Prepare statement successful ...\n");

	err = cdq_exec_stmt(&client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Failed to create table, err: %d\n", err);
		goto err_;
	}
	printf("Table created ...\n");

	err = cdq_prepare_stmt(&client, "BEGIN", &stmt_id);
	err = err ? err : cdq_exec_stmt(&client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Failed to start transaction\n");
		goto err_;
	}
	err = cdq_prepare_stmt(&client, update_tbl_stmt, &stmt_id);
	if (err != 0) {
		fprintf(stderr, "Unable to prepare INSERT statement\n");
		goto err_;
	}

	err = cdq_exec_stmt(&client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Failed to insert into table\n");
		goto err_;
	}

	err = cdq_prepare_stmt(&client, "COMMIT", &stmt_id);
	err = err ? err : cdq_exec_stmt(&client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Failed to commit transaction\n");
		goto err_;
	}
	printf("Updated table with a value ...\n");

	err = cdq_prepare_stmt(&client, query_tbl_stmt, &stmt_id);
	if (err != 0) {
		fprintf(stderr, "Unable to prepare Query statement\n");
		goto err_;
	}

	err = cdq_query_stmt(&client, stmt_id, &rows);
	if (err != 0) {
		fprintf(stderr, "Query failed\n");
	} else {
		fprintf(stdout, "Query successful ...\n");
	}
	cdq_rows_close(&rows);

err_:
	cdq_stop(&client);
	free(client.srv_ipaddr);
}
