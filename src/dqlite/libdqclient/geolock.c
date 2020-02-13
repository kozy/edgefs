#include <stdio.h>
#include "geolock.h"

static char *global_lock_tbl_stmt = 
	"CREATE TABLE IF NOT EXISTS GVMR ( " 
	"path TEXT PRIMARY KEY, "
	"genid NUMERIC NOT NULL, "
	"uvid NUMERIC NOT NULL, "
	"deleted INTEGER NOT NULL, "
	"nhid TEXT NOT NULL, "
	"vmchid TEXT NOT NULL, "
	"segid NUMERIC NOT NULL, "
	"size NUMERIC NOT NULL, "
	"locktime NUMERIC NOT NULL, "
	"lockstate INTEGER NOT NULL"
	")";

static char *delete_lock_tbl_stmt =
	"DELETE from GVMR";

static char *update_lock_rec_stmt =
	"UPDATE GVMR SET lockstate = %d, locktime = datetime('now') "
        " WHERE path = '%s' and genid = %lu and lockstate = %d and deleted = 0";

static char *insert_lock_rec_stmt =
	"INSERT INTO GVMR (path, genid, uvid, deleted, nhid, "
	" vmchid, segid, size, locktime, lockstate) "
	" VALUES ('%s', 1, %lu, 0, '%s', '%s', %lu, %lu, "
	" datetime('now'), 0)";

static char *select_lock_stmt =
	"SELECT * from GVMR WHERE path = '%s'";

static char *mark_for_deletion_stmt =
	"UPDATE GVMR SET deleted = 1 WHERE path = '%s'";

static char *delete_lock_rec_stmt =
	"DELETE from GVMR WHERE path = '%s'";

static int
sql_exec_stmt(struct cdq_client *client, char *sql_stmt)
{
	int err;
	unsigned stmt_id, last_insert_id, rows_affected;

	err = cdq_prepare_stmt(client, sql_stmt, &stmt_id);
	if (err != 0) {
		fprintf(stderr, "Prepare failed\n");
		return -1;
	}
	err = cdq_exec_stmt(client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Execute failed\n");
		return -1;
	}
	return 0;
}

int
geolock_create_lock_tbl(struct cdq_client *client)
{
	return sql_exec_stmt(client, global_lock_tbl_stmt);
}

int
geolock_get_lock_rec(struct cdq_client *client, const char *path_key,
			geo_lock_t *lock_rec)
{
	char stmt[4096];
	int err, recs_nr = 0;
	unsigned stmt_id;
	struct rows rows;
	struct row *rec, *first;

	sprintf(stmt, select_lock_stmt, path_key);
	err = cdq_prepare_stmt(client, stmt, &stmt_id);
	if (err != 0) {
		return -1;
	}
	err = cdq_query_stmt(client, stmt_id, &rows);
	if (err == 0) {
#if 0
		first = rec = rows.next;
		printf("REC pointer: %p\n", rec);
		while(rec) {
			recs_nr++;
			rec = rec->next++;
			printf("ROW: %p\n", rec);
		}
		printf("No. of lock records selected - %d\n", recs_nr);
		printf("ROW: %p\n", rows.next);
#endif
		cdq_rows_close(&rows);
	}
	
	return err ? -1 : 0;
	
}

int
geolock_mark_for_delete(struct cdq_client *client, const char *path_key)
{
	char stmt[4096];

	sprintf(stmt, mark_for_deletion_stmt, path_key);
	return sql_exec_stmt(client, stmt);
}

int
geolock_delete_lock_rec(struct cdq_client *client, const char *path_key)
{
	char stmt[4096];

	sprintf(stmt, delete_lock_rec_stmt, path_key);
	return sql_exec_stmt(client, stmt);
}

int
geolock_insert_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec)
{
	char stmt[4096];

	sprintf(stmt, insert_lock_rec_stmt,
			lock_rec->path, lock_rec->uvid,
			lock_rec->nhid, lock_rec->vmchid,
			lock_rec->segid, lock_rec->size);
	return sql_exec_stmt(client, stmt);
}

int
geolock_lock(struct cdq_client *client, const char *path_key, uint64_t genid)
{
	char stmt[4096];
	int lock_new_state = 1;

	sprintf(stmt, update_lock_rec_stmt, lock_new_state, path_key,
			genid, !lock_new_state);
	return sql_exec_stmt(client, stmt);
}

int
geolock_unlock(struct cdq_client *client, const char *path_key, uint64_t genid)
{
	char stmt[4096];
	int lock_new_state = 0;

	sprintf(stmt, update_lock_rec_stmt, lock_new_state, path_key,
			genid, !lock_new_state);
	return sql_exec_stmt(client, stmt);
}

int
test_geolock_delete_tbl(struct cdq_client *client)
{
	return sql_exec_stmt(client, delete_lock_tbl_stmt);
}
