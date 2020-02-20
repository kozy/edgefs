#include <stdio.h>
#include <errno.h>
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

static char *check_lock_stmt =
	"SELECT * from GVMR WHERE path = '%s' and lockstate = 1";

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
		return -EINVAL;
	}
	err = cdq_exec_stmt(client, stmt_id, &last_insert_id, &rows_affected);
	if (err != 0) {
		fprintf(stderr, "Execute failed\n");
		return -EIO;
	}
	return 0;
}

int
geolock_create_lock_tbl(struct cdq_client *client)
{
	return sql_exec_stmt(client, global_lock_tbl_stmt);
}

int
geolock_is_locked(struct cdq_client *client, const char *path_key, int *locked)
{
	char stmt[4096];
	int err, recs_nr = 0;
	unsigned stmt_id;
	struct rows rows;
	struct row *rec;

	if (locked == NULL)
		return -EINVAL;

	*locked = 0;

	sprintf(stmt, select_lock_stmt, path_key);
	err = cdq_prepare_stmt(client, stmt, &stmt_id);
	if (err != 0) {
		return -EINVAL;
	}

	memset(&rows, 0, sizeof rows);
	err = cdq_query_stmt(client, stmt_id, &rows);
	if (err == 0) {
		if (rows.next != NULL) {
			*locked = rows.next->values[9].integer;
			cdq_rows_close(&rows);
		} else {
			err = -ENOENT;
		}
	} else {
		err = -EIO;
	}
	return err;
}

static void
copy_lock_rec(struct row *rec, geo_lock_t *lock_rec)
{
	strcpy(lock_rec->path, rec->values[0].text);
	lock_rec->genid = rec->values[1].integer;
	lock_rec->uvid = rec->values[2].integer;
	lock_rec->deleted = rec->values[3].integer;
	strcpy(lock_rec->nhid, rec->values[4].text);
	strcpy(lock_rec->vmchid, rec->values[5].text);
	lock_rec->segid = rec->values[6].integer;
	lock_rec->size = rec->values[7].integer;
	lock_rec->lock_time = rec->values[8].integer;
	lock_rec->lock_state = rec->values[9].integer;
}

int
geolock_get_lock_rec(struct cdq_client *client, const char *path_key,
			geo_lock_t *lock_rec, int *rec_count)
{
	char stmt[4096];
	int err, recs_nr = 0;
	unsigned stmt_id;
	struct rows rows;
	struct row *rec;

	sprintf(stmt, check_lock_stmt, path_key);
	err = cdq_prepare_stmt(client, stmt, &stmt_id);
	if (err != 0) {
		return -EINVAL;
	}

	memset(&rows, 0, sizeof rows);
	err = cdq_query_stmt(client, stmt_id, &rows);
	if (err == 0) {
		rec = rows.next;
		if (rec != NULL && lock_rec != NULL) {
			copy_lock_rec(rec, lock_rec);
		}

		while (rec) {
			recs_nr++;
			rec = rec->next;
		}
		if (recs_nr == 0)
			err = -ENOENT;
		*rec_count = recs_nr;
		cdq_rows_close(&rows);
	} else {
		err = -EIO;
	}
	
	return err;
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
