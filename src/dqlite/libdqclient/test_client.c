#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "dqclient.h"

#if 0
static char *create_tbl_stmt =
		"CREATE TABLE IF NOT EXISTS model (key TEXT, value TEXT)";
#else
static char *create_tbl_stmt =
		"CREATE TABLE test (n INT)";
#endif
static char *update_tbl_stmt =
		"INSERT OR REPLACE INTO model(key, value) VALUES(%s, %s)";
static char *query_tbl_stmt =
		"SELECT value FROM model WHERE key = %s";

static char stmt[4096];

int
main(int argc, char **argv)
{
	int err;
	unsigned stmt_id, last_insert_id = 0, rows_affected = 0;
	struct cdq_client client;
	char *dbname = "test";

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
	printf("Prepare statement successful id: %u ...\n", stmt_id);

	err = cdq_exec_stmt(&client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Failed to create table, err: %d\n", err);
		goto err_;
	}
	printf("Table created ...\n");

#if 0
	sprintf(stmt, update_tbl_stmt, "my-key", "my_value");
	printf("Statement : %s\n", stmt);
	err = cdq_prepare_stmt(&client, stmt, &stmt_id);
	if (err != 0) {
		fprintf(stderr, "Unable to prepare update statement\n");
		goto err_;
	}

	err = cdq_exec_stmt(&client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Failed to update table\n");
		goto err_;
	}
	printf("Table updated <my-key, my-value> ...\n");
#endif

err_:
	cdq_stop(&client);
	free(client.srv_ipaddr);
}
