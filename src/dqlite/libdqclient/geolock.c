#include <stdio.h>
#include <errno.h>
#include "geolock.h"

static char *global_lock_tbl_stmt =
	"CREATE TABLE IF NOT EXISTS GVMR ( "
	"path TEXT PRIMARY KEY, "
	"genid NUMERIC, "
	"uvid NUMERIC, "
	"deleted INTEGER, "
	"nhid TEXT, "
	"vmchid TEXT, "
	"segid NUMERIC, "
	"serverid NUMERIC, "
	"size NUMERIC, "
	"locktime NUMERIC, "
	"lockstate INTEGER"
	")";

static char *delete_lock_tbl_stmt =
	"DELETE from GVMR";

static char *update_lock_rec_stmt =
	"UPDATE GVMR SET lockstate = %d, locktime = datetime('now') "
        " WHERE path = '%s' and genid = %lu and lockstate = %d";

static char *insert_lock_rec_stmt =
	"INSERT INTO GVMR (path, genid, uvid, deleted, nhid, "
	" vmchid, segid, serverid, size, locktime, lockstate) "
	" VALUES ('%s', %lu, %lu, %lu, '%s', '%s', %lu, %lu, %lu, "
	" datetime('now'), 0)";

static char *acquire_lock_rec_insert_stmt =
	"INSERT OR IGNORE INTO GVMR (path, genid, uvid, deleted, nhid, "
	" vmchid, segid, serverid, size, locktime, lockstate) "
	" VALUES ('%s', %lu, %lu, %lu, '%s', '%s', %lu, %lu, %lu, "
	" datetime('now'), 1)";

static char *acquire_lock_rec_update_stmt =
	" UPDATE GVMR SET locktime=datetime('now'), lockstate=1, segid=%lu, serverid=%lu "
	" where path='%s' and lockstate=0";

static char *release_lock_rec_stmt =
	" UPDATE GVMR SET "
	" genid=%lu, uvid=%lu, deleted=%lu, nhid='%s', vmchid='%s', "
	" size=%lu, locktime=datetime('now'), lockstate=0 "
	" where path='%s' and segid=%lu and serverid=%lu";

static char *clean_lock_rec_stmt =
	" UPDATE GVMR SET "
	" locktime=datetime('now'), lockstate=0 "
	" where segid=%lu and serverid=%lu and lockstate=0";

static char *select_lock_stmt =
	"SELECT * from GVMR WHERE path = '%s'";

static char *check_lock_stmt =
	"SELECT * from GVMR WHERE path = '%s' and lockstate = 1";

static char *list_lock_stmt =
	"SELECT * from GVMR where path > '%s' and lockstate >= %d";

static char *mark_for_deletion_stmt =
	"UPDATE GVMR SET deleted = 1 WHERE path = '%s'";

static char *delete_lock_rec_stmt =
	"DELETE from GVMR WHERE path = '%s'";

static int
sql_exec_stmt(struct cdq_client *client, char *sql_stmt, unsigned *rows_affected)
{
	int err;
	unsigned stmt_id, last_insert_id;

	err = cdq_prepare_stmt(client, sql_stmt, &stmt_id);
	if (err != 0) {
		return -EINVAL;
	}
	err = cdq_exec_stmt(client, stmt_id, &last_insert_id, rows_affected);
	if (err != 0) {
		return -EIO;
	}
	return 0;
}

int
geolock_create_lock_tbl(struct cdq_client *client)
{
	unsigned rows_affected;
	return sql_exec_stmt(client, global_lock_tbl_stmt, &rows_affected);
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
		rec = rows.next;
		if (rec != NULL) {
			*locked = rec->values[10].integer;
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
	lock_rec->serverid = rec->values[7].integer;
	lock_rec->size = rec->values[8].integer;
	lock_rec->lock_time = rec->values[9].integer;
	lock_rec->lock_state = rec->values[10].integer;
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

	sprintf(stmt, select_lock_stmt, path_key);
	err = cdq_prepare_stmt(client, stmt, &stmt_id);
	if (err) {
		return -EINVAL;
	}

	*rec_count = 0;
	memset(&rows, 0, sizeof rows);
	err = cdq_query_stmt(client, stmt_id, &rows);
	if (err == 0) {
		rec = rows.next;
		if (rec != NULL && lock_rec != NULL) {
			copy_lock_rec(rec, lock_rec);
			*rec_count = 1;
		} else {
			err = -ENOENT;
		}
		cdq_rows_close(&rows);
	} else {
		err = -EIO;
	}
	return err;
}

int
geolock_list_lock_rec(struct cdq_client *client, char *from, char *lock)
{
	char stmt[4096];
	int err, rec_count = 0;
	unsigned stmt_id;
	struct rows rows;
	struct row *rec = NULL;
	geo_lock_t db_rec;
	int ilock = 0;

	if (lock && lock[0] == '1') {
		ilock = 1;
	}

	sprintf(stmt, list_lock_stmt, from, ilock);
	fprintf(stderr, "List lock stmt: %s\n", stmt);
	err = cdq_prepare_stmt(client, stmt, &stmt_id);
	if (err) {
		fprintf(stderr, "Prepre failed stmt: %s err: %d\n", stmt, err);
		return -EINVAL;
	}

	memset(&rows, 0, sizeof rows);
	err = cdq_query_stmt(client, stmt_id, &rows);
	if (!err) {
		rec = rows.next;
	}
	int read_count = 0;
	while (rec != NULL && !err) {
		memset(&db_rec, 0, sizeof db_rec);
		copy_lock_rec(rec, &db_rec);
		rec_count++;
		read_count++;
		printf("%d: %s|", rec_count, db_rec.path);
		printf("%lu|", db_rec.genid);
		printf("%lu|", db_rec.uvid);
		printf("%u|", db_rec.deleted);
		printf("%s|", db_rec.nhid);
		printf("%s|", db_rec.vmchid);
		printf("%lu|", db_rec.segid);
		printf("%lu|", db_rec.serverid);
		printf("%lu|", db_rec.size);
		printf("%lu|", db_rec.lock_time);
		printf("%u\n", db_rec.lock_state);
		rec = rec->next;
		if (rec == NULL) {
			cdq_rows_close(&rows);
			read_count = 0;
			memset(&rows, 0, sizeof rows);
			err = cdq_query_stmt_more(client, &rows);
			if (err) {
				break;
			}
			rec = rows.next;
		}
	}
	if (!err && read_count > 0) {
		cdq_rows_close(&rows);
	}
	return err;
}

int
geolock_delete_lock_rec(struct cdq_client *client, const char *path_key)
{
	char stmt[4096];
	unsigned rows_affected;

	sprintf(stmt, delete_lock_rec_stmt, path_key);
	return sql_exec_stmt(client, stmt, &rows_affected);
}

int
geolock_insert_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec)
{
	char stmt[4096];
	unsigned rows_affected;

	sprintf(stmt, insert_lock_rec_stmt,
			lock_rec->path, lock_rec->genid, lock_rec->uvid, lock_rec->deleted,
			lock_rec->nhid, lock_rec->vmchid,
			lock_rec->segid, lock_rec->serverid, lock_rec->size);
	return sql_exec_stmt(client, stmt, &rows_affected);
}

int
geolock_acquire_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec)
{
	char stmt[4096];
	unsigned rows_affected = 0;
	int err = 0;

	lock_rec->lock_state = 0;
	sprintf(stmt, acquire_lock_rec_update_stmt, lock_rec->segid, lock_rec->serverid, lock_rec->path);
	err = sql_exec_stmt(client, stmt, &rows_affected);
	if (!err && rows_affected > 0) { // Get previous record
		int rec_count = 0;
		err = geolock_get_lock_rec(client, lock_rec->path,	lock_rec, &rec_count);
		lock_rec->lock_state = 1;
		return err;
	}

	// create a new record
	rows_affected = 0;
	sprintf(stmt, acquire_lock_rec_insert_stmt,
			lock_rec->path, lock_rec->genid, lock_rec->uvid, lock_rec->deleted,
			lock_rec->nhid, lock_rec->vmchid,
			lock_rec->segid, lock_rec->serverid, lock_rec->size);
	err = sql_exec_stmt(client, stmt, &rows_affected);
	if (err) {
		return err;
	}
	if (rows_affected == 0) {
		return -EBUSY;
	}
	lock_rec->lock_state = 1;
	return 0;
}

int
geolock_release_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec)
{
	char stmt[4096];
	unsigned rows_affected = 0;

	sprintf(stmt, release_lock_rec_stmt,
			lock_rec->genid, lock_rec->uvid, lock_rec->deleted,
			lock_rec->nhid, lock_rec->vmchid, lock_rec->size,
			lock_rec->path, lock_rec->segid, lock_rec->serverid);
	int err = sql_exec_stmt(client, stmt, &rows_affected);
	if (err) {
		return err;
	}
	if (rows_affected == 0) {
		return -EBUSY;
	}
	return 0;
}
int
geolock_clean_lock_rec(struct cdq_client *client, geo_lock_t *lock_rec)
{
	char stmt[4096];
	unsigned rows_affected = 0;

	sprintf(stmt, clean_lock_rec_stmt,
			lock_rec->segid, lock_rec->serverid);
	int err = sql_exec_stmt(client, stmt, &rows_affected);
	return err;
}


int
geolock_lock(struct cdq_client *client, const char *path_key, uint64_t genid)
{
	char stmt[4096];
	int lock_new_state = 1;
	unsigned rows_affected;

	sprintf(stmt, update_lock_rec_stmt, lock_new_state, path_key,
			genid, !lock_new_state);
	return sql_exec_stmt(client, stmt, &rows_affected);
}

int
geolock_unlock(struct cdq_client *client, const char *path_key, uint64_t genid)
{
	char stmt[4096];
	int lock_new_state = 0;
	unsigned rows_affected;

	sprintf(stmt, update_lock_rec_stmt, lock_new_state, path_key,
			genid, !lock_new_state);
	return sql_exec_stmt(client, stmt, &rows_affected);
}

int
test_geolock_delete_tbl(struct cdq_client *client)
{
	unsigned rows_affected;
	return sql_exec_stmt(client, delete_lock_tbl_stmt, &rows_affected);
}
