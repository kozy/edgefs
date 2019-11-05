#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

#include "ccowutil.h"
#include "fsio_system.h"

#include "fsio_s3cache.h"

#define MAX_KEY_LEN 2048
#define MIN_CLEAN_KEY_COUNT 1024
#define MIN_CLEAN_AGE 100  // 100 seconds
#define FSIO_S3_CACHE_HT_SIZE 32768
#define FSIO_S3_CACHE_HT_LOAD_FACTOR 0.085

char *build_s3_key(inode_t parent, char *name, char *buf)
{
	sprintf(buf, "%lu_%s", parent, name);
	return buf;
}

int
fsio_s3_cache_entry_init(fsio_s3_cache_entry_t *fsio_s3_cache_entry, inode_t parent, inode_t child, char *name, uint512_t *vmchid) {
	fsio_s3_cache_entry->parent = parent;
	fsio_s3_cache_entry->child = child;
	fsio_s3_cache_entry->name = je_strndup(name, MAX_KEY_LEN);
	memcpy(&fsio_s3_cache_entry->vmchid, vmchid, sizeof(uint512_t));
	fsio_s3_cache_entry->created = get_timestamp_us() / 1000;
	return 0;
}

void
fsio_s3_cache_entry_destroy(fsio_s3_cache_entry_t *fsio_s3_cache_entry) {
	if (fsio_s3_cache_entry->name) {
		je_free(fsio_s3_cache_entry->name);
		fsio_s3_cache_entry->name = NULL;
	}
}

int
fsio_s3_cache_entry_age(fsio_s3_cache_entry_t *fsio_s3_cache_entry) {
	uint64_t age = (get_timestamp_us() / 1000 - fsio_s3_cache_entry->created)/1000;
	return (int) age;
}


int
fsio_s3_cache_entry_expired(fsio_s3_cache_entry_t *fsio_s3_cache_entry) {
	return (fsio_s3_cache_entry_age(fsio_s3_cache_entry) > FSIO_S3_CACHE_TTL);
}


/* fsio_s3_cache_entry_t hash tables */
int
fsio_s3_cache_create(fsio_s3_cache_t *fsio_s3_cache) {
	if (fsio_s3_cache->fsio_s3_cache_entry_ht != NULL)
		return 0;

	int err = pthread_mutex_init(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock, NULL);

	if (err != 0)
		return err;

	fsio_s3_cache->fsio_s3_cache_entry_ht = hashtable_create(FSIO_S3_CACHE_HT_SIZE, 0, FSIO_S3_CACHE_HT_LOAD_FACTOR);
	fsio_s3_cache->last_clean = get_timestamp_us() / 1000;
	return 0;
}


int
fsio_s3_cache_destroy(fsio_s3_cache_t *fsio_s3_cache) {
	if (fsio_s3_cache == NULL)
		return 0;
	if (fsio_s3_cache->fsio_s3_cache_entry_ht == NULL)
		return 0;

	void **keys;
	char *key;
	size_t ent_size;
	unsigned int key_count;
	keys = hashtable_keys(fsio_s3_cache->fsio_s3_cache_entry_ht, &key_count);

	for (unsigned int i = 0; i < key_count; i++) {
		key = (char *) keys[i];
		fsio_s3_cache_entry_t *fsio_s3_cache_entry = hashtable_get(fsio_s3_cache->fsio_s3_cache_entry_ht,
			(void *)key, strlen(key) + 1, &ent_size);
		if (fsio_s3_cache_entry != NULL && ent_size == sizeof(fsio_s3_cache_entry_t)) {
			fsio_s3_cache_entry_destroy(fsio_s3_cache_entry);
		}
	}

	if (key_count)
		je_free(keys);

	hashtable_destroy(fsio_s3_cache->fsio_s3_cache_entry_ht);
	fsio_s3_cache->fsio_s3_cache_entry_ht = NULL;
	pthread_mutex_destroy(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
	return 0;
}

int
fsio_s3_cache_clean(fsio_s3_cache_t *fsio_s3_cache) {
	if (fsio_s3_cache == NULL)
		return 0;
	if (fsio_s3_cache->fsio_s3_cache_entry_ht == NULL)
		return 0;

	void **keys;
	char *key;
	size_t ent_size;
	unsigned int key_count;
	unsigned int num = 0;

	pthread_mutex_lock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);

	if (fsio_s3_cache->fsio_s3_cache_entry_ht->key_count < MIN_CLEAN_KEY_COUNT) {
		pthread_mutex_unlock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
		return 0;
	}

	uint64_t clean_age = (get_timestamp_us() / 1000 - fsio_s3_cache->last_clean)/1000;
	if (clean_age < MIN_CLEAN_AGE) {
		pthread_mutex_unlock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
		return 0;
	}

	log_trace(fsio_lg, "fsio_s3_cache clean started on age %lu, count: %u",
			clean_age, fsio_s3_cache->fsio_s3_cache_entry_ht->key_count);


	fsio_s3_cache->last_clean = get_timestamp_us() / 1000;

	keys = hashtable_keys(fsio_s3_cache->fsio_s3_cache_entry_ht, &key_count);

	for (unsigned int i = 0; i < key_count; i++) {
		key = (char *) keys[i];

		fsio_s3_cache_entry_t *fsio_s3_cache_entry = hashtable_get(fsio_s3_cache->fsio_s3_cache_entry_ht,
			(void *)key, strlen(key) + 1, &ent_size);
		if (fsio_s3_cache_entry != NULL && ent_size == sizeof(fsio_s3_cache_entry_t)) {
			if (fsio_s3_cache_entry_expired(fsio_s3_cache_entry)) {
				fsio_s3_cache_entry_destroy(fsio_s3_cache_entry);
				num++;
				hashtable_remove(fsio_s3_cache->fsio_s3_cache_entry_ht, (void *)key, strlen(key) + 1);
			}
		}
	}

	if (key_count)
		je_free(keys);

	log_trace(fsio_lg, "fsio_s3_cache cleaned %u records from %u", num, key_count);

	pthread_mutex_unlock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);

	return 0;
}


int
fsio_s3_cache_put(fsio_s3_cache_t *fsio_s3_cache, fsio_s3_cache_entry_t *fsio_s3_cache_entry)
{
	int err = 0;

	char buf[2048];
	char  *key = build_s3_key(fsio_s3_cache_entry->parent, fsio_s3_cache_entry->name, buf);

	log_trace(fsio_lg, "ht add fsio_s3_cache_entry: %s", key);

	pthread_mutex_lock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);

	// Delete old entry
	size_t ent_size;
	fsio_s3_cache_entry_t *entry = hashtable_get(fsio_s3_cache->fsio_s3_cache_entry_ht,
		(void *)key, strlen(key) + 1, &ent_size);

	if (entry != NULL && ent_size == sizeof(fsio_s3_cache_entry_t)) {
		fsio_s3_cache_entry_destroy(entry);
		hashtable_remove(fsio_s3_cache->fsio_s3_cache_entry_ht, (void *)key, strlen(key) + 1);
	}

	err = hashtable_put(fsio_s3_cache->fsio_s3_cache_entry_ht, (void *)key, strlen(key) + 1,
	    fsio_s3_cache_entry, sizeof(fsio_s3_cache_entry_t));

	pthread_mutex_unlock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
	return err;
}

int
fsio_s3_cache_get(fsio_s3_cache_t *fsio_s3_cache, char *key, inode_t *res_ino)
{
	size_t ent_size;
	pthread_mutex_lock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);

	fsio_s3_cache_entry_t *ent = hashtable_get(fsio_s3_cache->fsio_s3_cache_entry_ht, (void *)key, strlen(key) + 1, &ent_size);

	log_trace(fsio_lg,"ht get by fsio_s3_cache_entry key: %s, size: %d", key, (int) ent_size);

	if (ent != NULL && ent_size == sizeof(fsio_s3_cache_entry_t)) {
		if (fsio_s3_cache_entry_expired(ent)) {
			pthread_mutex_unlock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
			fsio_s3_cache_delete(fsio_s3_cache, key);
			return ENOENT;
		}
		*res_ino = ent->child;
		pthread_mutex_unlock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
		return 0;
	} else {
		pthread_mutex_unlock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
		return ENOENT;
	}
}


void
fsio_s3_cache_delete(fsio_s3_cache_t *fsio_s3_cache, char *key)
{
	pthread_mutex_lock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
	size_t ent_size;
	fsio_s3_cache_entry_t *fsio_s3_cache_entry = hashtable_get(fsio_s3_cache->fsio_s3_cache_entry_ht, (void *)key, strlen(key) + 1, &ent_size);

	if (fsio_s3_cache_entry != NULL && ent_size == sizeof(fsio_s3_cache_entry_t)) {
		fsio_s3_cache_entry_destroy(fsio_s3_cache_entry);
		hashtable_remove(fsio_s3_cache->fsio_s3_cache_entry_ht, (void *)key, strlen(key) + 1);
	}
	pthread_mutex_unlock(&fsio_s3_cache->fsio_s3_cache_entry_ht_lock);
}
