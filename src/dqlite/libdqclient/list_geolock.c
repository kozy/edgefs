#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>

#include "geolock.h"

int
main(int argc, char **argv)
{
	int err, is_locked = 0, rec_count = 0;
	unsigned stmt_id, last_insert_id = 0, rows_affected = 0;
	struct cdq_client client;
	struct rows rows;
	geo_lock_t lock_rec, db_rec;

	memset(&client, 0, sizeof client);
	if (argc < 6) {
		fprintf(stderr, "Usage: %s <server_id> <server_ip> <dbname> <from> <lock_state>\n", argv[0]);
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

	err = cdq_db_open(&client, argv[3]);
	if (err != 0) {
		fprintf(stderr, "Failed to open db - %s\n", argv[3]);
		exit(1);
	}
	printf("Open DB successful ...\n");

	/* List lock objects */
    err = geolock_list_lock_rec(&client, argv[4], argv[5]);
    if (err) {
        fprintf(stderr, "List error: %d\n", err);
    }

	cdq_stop(&client);
}
