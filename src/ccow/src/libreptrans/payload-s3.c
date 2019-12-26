/*
 * Copyright (c) 2015-2018 Nexenta Systems, inc.
 *
 * This file is part of EdgeFS Project
 * (see https://github.com/Nexenta/edgefs).
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <uthash.h>
#include <errno.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/crypto.h>
#include "ccowutil.h"
#include "payload-s3.h"

#define BLOCK_LENGTH 64U
#define INNER_PAD '\x36'
#define OUTER_PAD '\x5c'

#define SHA256_DIGEST_LENGTH 32
#define SHA256_DIGEST_HEX_LENGTH (SHA256_DIGEST_LENGTH *2)+4

#define AWS_KEY_PREFIX "AWS4"
#define AWS_ALGORITHM "AWS4-HMAC-SHA256"
#define AWS_SIGNED_HEADERS "host;x-amz-date"
#define AWS_REQUEST_TYPE "aws4_request"
#define AWS_DATE_LABEL "x-amz-date"

static volatile int curl_global = 0;
static int curl_verb = 1;

struct get_cache_entry {
	char key[130];
	uv_buf_t val;
	UT_hash_handle hh;
};

struct get_cache {
	struct get_cache_entry *head;
	struct get_cache_entry* last;
	pthread_rwlock_t lock;
	size_t cap;
};

static int
get_cache_create(size_t cap, struct get_cache** out) {
	if (!cap)
		return -EINVAL;
	*out = je_malloc(sizeof(struct get_cache));
	if (NULL == *out)
		return -ENOMEM;
	(*out)->cap = cap;
	(*out)->head = NULL;
	(*out)->last = NULL;
	pthread_rwlock_init(&(*out)->lock, NULL);
	return 0;
}

static void
get_cache_destroy(struct get_cache* p) {
	struct get_cache_entry* e = NULL, *tmp = NULL;
	e = NULL, tmp = NULL;
	HASH_ITER(hh, p->head, e, tmp) {
		assert(e);
		HASH_DEL(p->head, e);
		je_free(e->val.base);
		je_free(e);
	}
	pthread_rwlock_destroy(&p->lock);
	je_free(p);
}

static int
get_cache_insert(struct get_cache* p, char* key, uv_buf_t value) {
	struct get_cache_entry* res = NULL;
	pthread_rwlock_wrlock(&p->lock);
	HASH_FIND_STR(p->head, key, res);
	if (res) {
		log_debug(lg, "Key %s already exists in S3 cache", key);
		pthread_rwlock_unlock(&p->lock);
		return -EEXIST;
	}

	struct get_cache_entry* e= je_malloc(sizeof(*e));
	if (!e) {
		pthread_rwlock_unlock(&p->lock);
		return -ENOMEM;
	}
	strncpy(e->key, key, sizeof(e->key));
	e->val = value;
	e->val.base = je_memdup(value.base, value.len);
	HASH_ADD_STR(p->head, key, e);
	if (!p->last)
		p->last = e;
	if (HASH_COUNT(p->head) > p->cap) {
		/* Removing oldest entry from the tail */
		res = p->last;
		assert(res);
		p->last = p->last->hh.next;
		assert(p->last);
		if (!strcmp(key, res->key))
			log_error(lg, "Removing just inserted S3 key");
		HASH_DEL(p->head, res);
		je_free(res->val.base);
		je_free(res);
	}
	pthread_rwlock_unlock(&p->lock);
	return 0;
}

static int
get_cache_find(struct get_cache* p, char* key, uv_buf_t* out) {
	struct get_cache_entry* res = NULL;
	pthread_rwlock_rdlock(&p->lock);
	HASH_FIND_STR(p->head, key, res);
	pthread_rwlock_unlock(&p->lock);
	if (res) {
		*out = res->val;
		return 0;
	}
	return -ENOENT;
}

struct perf_cap {
	char hdr[128];
	uint64_t ts; /* Timestamp of a measurement interval */
	size_t size; /* Size of data transfered during a measurement interval */
	size_t par_ops; /* Max. number of parallel operations */
	size_t par_ops_max; /* Max. number of parallel operations */
	size_t ops; /* Total number of operations */
	uint64_t dur; /* Average duration of a single operation */
	uint64_t chits; /* Number of GET cache hits */
	pthread_mutex_t lock;
};

static struct perf_cap g_get_perf = {.hdr = "GET perf"};
static struct perf_cap g_put_perf = {.hdr = "PUT perf"};
static struct perf_cap g_del_perf = {.hdr = "DEL perf"};

#define atomic_set(ptr, val, tmp) \
	do { \
		(tmp) = __sync_fetch_and_add((ptr), 0); \
	} while (!__sync_bool_compare_and_swap((ptr), (tmp), (val)))

static void
perf_cache_hit(struct perf_cap* cap) {
	__sync_fetch_and_add(&cap->chits, 1);
}

static void
perf_cap_update(struct perf_cap* cap, size_t size, uint64_t dur) {
	uint64_t tmp64;
	size_t tmpst;
	char buff[128];

	if (__sync_bool_compare_and_swap(&cap->ts, 0, get_timestamp_us())) {
		pthread_mutex_init(&cap->lock, NULL);
	}

	if (!dur) {
		/* Operation start */
		uint64_t pops = __sync_add_and_fetch(&cap->par_ops, 1);
		if (pops > cap->par_ops_max)
			atomic_set(&cap->par_ops_max, pops, tmpst);
	} else {
		/* Operation end */
		tmpst = __sync_fetch_and_sub(&cap->par_ops, 1);
		__sync_fetch_and_add(&cap->ops, 1);
		__sync_fetch_and_add(&cap->size, size);
		/* Update duration */
		__sync_fetch_and_add(&cap->dur, dur);
	}
	if (get_timestamp_us() - cap->ts > 1000000UL) {
		if (pthread_mutex_trylock(&cap->lock))
			return;
		size_t len = 0;
		uint64_t ts_begin;
		atomic_set(&cap->ts, get_timestamp_us(), ts_begin);
		buff[0] = 0;
		atomic_set(&cap->size, 0, len);
		sprintf(buff, "size %lu, ", len);

		atomic_set(&cap->par_ops_max, 0, tmpst);
		sprintf(buff+strlen(buff), "#par_ops_max %lu, ", tmpst);

		atomic_set(&cap->ops, 0, tmpst);
		sprintf(buff+strlen(buff), "#ops %lu, ", tmpst);

		atomic_set(&cap->dur, 0, tmp64);
		sprintf(buff+strlen(buff), "avg %lu mS", tmp64/((tmpst ? tmpst : 1)*1000UL));

		log_debug(lg, "%s: %s, perf %.3f MB/s, cache hits %lu", cap->hdr, buff,
			(double)len/(double)(get_timestamp_us() - ts_begin), cap->chits);
		pthread_mutex_unlock(&cap->lock);
	}
}

//#define ERR_INJ 1

#if ERR_INJ
static long err_inj() {
	long resp = 0;
	int rc = rand() % 10000;
	if (!rc) {
		rc = rand() % 2;
		if (rc == 0)
			resp = 403;
		else
			resp = 503;
	}
	if (resp)
		log_notice(lg, "Injecting error %ld", resp);
	return resp;
}
#endif

static int
payload_s3_trace(CURL *handle, curl_infotype type, char *data, size_t size,
    void *userp)
{
	return 0;
}

static void
payload_s3_hmac_gen(uint8_t *input_key, uint8_t key_len, uint8_t *msg,
    uint8_t hmac_out[SHA256_DIGEST_LENGTH])
{
	uint8_t key[BLOCK_LENGTH];
	uint8_t inner_key[BLOCK_LENGTH];
	uint8_t outer_key[BLOCK_LENGTH];
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
	EVP_MD_CTX *inner_s, *outer_s;
#else
	EVP_MD_CTX inner_s[1];
	EVP_MD_CTX outer_s[1];
#endif
	uint8_t inner_hash[SHA256_DIGEST_LENGTH];

	memcpy(key, input_key, key_len);
	memset(key + key_len, '\0', BLOCK_LENGTH - key_len);

	for (size_t i = 0; i < BLOCK_LENGTH; i++) {
		inner_key[i] = key[i] ^ INNER_PAD;
		outer_key[i] = key[i] ^ OUTER_PAD;
	}

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
	inner_s = EVP_MD_CTX_new();
	assert(inner_s != NULL);
	outer_s = EVP_MD_CTX_new();
	assert(outer_s != NULL);
#endif
	EVP_MD_CTX_init(inner_s);
	if (EVP_DigestInit_ex(inner_s, EVP_sha256(), NULL) == 0)
		assert(0);
	if (EVP_DigestUpdate(inner_s, inner_key, BLOCK_LENGTH) == 0)
		assert(0);
	if (EVP_DigestUpdate(inner_s, msg, strlen((char *)msg)) == 0)
		assert(0);
	memset(inner_hash, 0, SHA256_DIGEST_LENGTH);
	if (EVP_DigestFinal_ex(inner_s, inner_hash, NULL) == 0)
		assert(0);

	EVP_MD_CTX_init(outer_s);
	if (EVP_DigestInit_ex(outer_s, EVP_sha256(), NULL) == 0)
		assert(0);
	if (EVP_DigestUpdate(outer_s, outer_key, BLOCK_LENGTH) == 0)
		assert(0);
	if (EVP_DigestUpdate(outer_s, inner_hash, SHA256_DIGEST_LENGTH) == 0)
		assert(0);
	memset(hmac_out, 0, SHA256_DIGEST_LENGTH);
	if (EVP_DigestFinal_ex(outer_s, hmac_out, NULL) == 0)
		assert(0);
#if OPENSSL_VERSION_NUMBER < 0x10100000L
	EVP_MD_CTX_cleanup(inner_s);
	EVP_MD_CTX_cleanup(outer_s);
#else
	EVP_MD_CTX_free(inner_s);
	EVP_MD_CTX_free(outer_s);
#endif
}

static void
payload_s3_hash_sha256_hex_gen(char *in, size_t in_len, char *out, size_t out_size)
{
	int ret;
	uint8_t digest[SHA256_DIGEST_LENGTH];
	unsigned out_idx = 0;

	if (EVP_Digest(in, in_len, digest, NULL, EVP_sha256(), NULL) == 0)
		assert(0);

	out[0] = '\0';
	for (size_t i = 0; i < SHA256_DIGEST_LENGTH; i++)
		out_idx += snprintf(out + out_idx, out_size - out_idx,
				"%02x", digest[i]);
}

static int
payload_s3_sign_request(struct payload_s3 *ctx, const char *objname, char *method,
    char *date_header, char *auth_header)
{
	char datetime[32];
	time_t sig_time = time(0);
	struct tm  *tstruct = gmtime(&sig_time);
	strftime(datetime, sizeof(datetime), "%Y%m%dT%H%M%SZ", tstruct);

	char date[12];
	memset(date, '\0', 12);
	strncpy(date, datetime, 8);

	char aws_host[1024];
	if (ctx->port == 80 || ctx->port == 443)
		sprintf(aws_host, "%s", ctx->host);
	else
		sprintf(aws_host, "%s:%d", ctx->host, ctx->port);

	// = 'host:' + host + '\n' + 'x-amz-date:' + amzdate + '\n'
	char canonical_headers[1024];
	sprintf(canonical_headers, "host:%s\n%s:%s\n", aws_host,
	    AWS_DATE_LABEL, datetime);

	char hmac_payload_hex[SHA256_DIGEST_HEX_LENGTH];
	strcpy(hmac_payload_hex, "UNSIGNED-PAYLOAD");

	char canonical_request[4096];
	sprintf(canonical_request, "%s\n%s/%s\n%s\n%s\n%s\n%s", method,
	    ctx->path, objname, "", canonical_headers, AWS_SIGNED_HEADERS, hmac_payload_hex);

	char hmac_canonical_request_hex[SHA256_DIGEST_HEX_LENGTH];
	payload_s3_hash_sha256_hex_gen(canonical_request, strlen(canonical_request),
	    hmac_canonical_request_hex, sizeof(hmac_canonical_request_hex));

	char credential_scope[64];
	sprintf(credential_scope, "%s/%s/%s/%s", date, ctx->aws_region,
	    "s3", AWS_REQUEST_TYPE);

	char string_to_sign[4096 + 256];
	sprintf(string_to_sign, "%s\n%s\n%s\n%s", AWS_ALGORITHM, datetime,
	    credential_scope, hmac_canonical_request_hex);

	uint8_t hmac_signing_interim[SHA256_DIGEST_LENGTH];
	uint8_t hmac_signing_key[SHA256_DIGEST_LENGTH];
	char prefixed_secret_key[64] = AWS_KEY_PREFIX;
	strcat(prefixed_secret_key,ctx->secret_key);
	payload_s3_hmac_gen((uint8_t *)prefixed_secret_key,
	    (uint8_t)strlen(prefixed_secret_key), (uint8_t *)date, hmac_signing_interim);
	payload_s3_hmac_gen((uint8_t *)&hmac_signing_interim, SHA256_DIGEST_LENGTH,
	    (uint8_t *)ctx->aws_region, hmac_signing_interim);
	payload_s3_hmac_gen((uint8_t *)&hmac_signing_interim, SHA256_DIGEST_LENGTH,
	    (uint8_t *)"s3", hmac_signing_interim);
	payload_s3_hmac_gen((uint8_t *)&hmac_signing_interim, SHA256_DIGEST_LENGTH,
	    (uint8_t *)AWS_REQUEST_TYPE, hmac_signing_key);

	uint8_t hmac_signature[SHA256_DIGEST_LENGTH];
	payload_s3_hmac_gen((uint8_t *)&hmac_signing_key, SHA256_DIGEST_LENGTH,
	    (uint8_t *)string_to_sign, hmac_signature);
	char hmac_signature_hex[SHA256_DIGEST_HEX_LENGTH];
	unsigned hmac_signature_hex_idx = 0;
	memset(hmac_signature_hex, '\0', 4);
	for (size_t i = 0; i < SHA256_DIGEST_LENGTH; i++)
		hmac_signature_hex_idx += snprintf(hmac_signature_hex + hmac_signature_hex_idx,
				sizeof(hmac_signature_hex) - hmac_signature_hex_idx,
				"%02x", hmac_signature[i] );

	sprintf(auth_header, "Authorization: %s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
	    AWS_ALGORITHM, ctx->access_key, credential_scope, AWS_SIGNED_HEADERS, hmac_signature_hex);
	sprintf(date_header, "%s: %s", AWS_DATE_LABEL, datetime);
	return 0;
}

static void
payload_s3_lock(CURL *handle, curl_lock_data data, curl_lock_access laccess,
    void *useptr)
{
	(void)handle;
	(void)data;
	(void)laccess;
	struct payload_s3 *ctx = useptr;
	pthread_mutex_lock(&ctx->conn_lock);
}

static void
payload_s3_unlock(CURL *handle, curl_lock_data data, void *useptr)
{
	(void)handle;
	(void)data;
	struct payload_s3 *ctx = useptr;
	pthread_mutex_unlock(&ctx->conn_lock);
}

struct payload_s3_write_ctx {
	const char *readptr;
	size_t sizeleft;
	struct curl_slist* headers;
	CURL *req;
	FILE *ferr;
	uv_buf_t data;
	char errbuf[CURL_ERROR_SIZE];
	int retry;
	int err;
};

static size_t
payload_s3_memread_callback(void *ptr, size_t size, size_t nmemb, void *userp)
{
	struct payload_s3_write_ctx *ctx = (struct payload_s3_write_ctx *)userp;
	size_t max = size*nmemb;

	if(max < 1)
		return 0;

	if(ctx->sizeleft) {
		size_t copylen = max;
		if(copylen > ctx->sizeleft)
			copylen = ctx->sizeleft;
		memcpy(ptr, ctx->readptr, copylen);
		ctx->readptr += copylen;
		ctx->sizeleft -= copylen;
		return copylen;
	}

	/* no more data left to deliver */
	return 0;
}

static int
payloas_s3_put_create(struct payload_s3 *ctx, const char* key, uv_buf_t *data,
	struct payload_s3_write_ctx** h) {
	CURLcode res;
	struct payload_s3_write_ctx* p = je_calloc(1, sizeof(struct payload_s3_write_ctx));
	char url[2048];
	char date_header[32];
	char auth_header[256];
	char errbuf[CURL_ERROR_SIZE];
	int err = 0;

	sprintf(url, "%s/%s", ctx->bucket_url, key);

	payload_s3_sign_request(ctx, key, "PUT", date_header, auth_header);
	p->headers = curl_slist_append(p->headers, "content-type: application/octet-stream");
	p->headers = curl_slist_append(p->headers, auth_header);
	p->headers = curl_slist_append(p->headers, date_header);
	p->headers = curl_slist_append(p->headers, "x-amz-content-sha256: UNSIGNED-PAYLOAD");

	p->req = curl_easy_init();
	curl_easy_setopt(p->req, CURLOPT_HTTPHEADER, p->headers);
	curl_easy_setopt(p->req, CURLOPT_URL, url);
	curl_easy_setopt(p->req, CURLOPT_SHARE, ctx->share);
	curl_easy_setopt(p->req, CURLOPT_UPLOAD, 1L);

	p->errbuf[0] = 0;
	curl_easy_setopt(p->req, CURLOPT_ERRORBUFFER, p->errbuf);
	if (curl_verb) {
		curl_easy_setopt(p->req, CURLOPT_DEBUGFUNCTION, payload_s3_trace);
		curl_easy_setopt(p->req, CURLOPT_VERBOSE, 1L);
	}

	p->ferr = fopen("/dev/null", "w");
	if (p->ferr)
		curl_easy_setopt(p->req, CURLOPT_STDERR, p->ferr);

	p->readptr = data->base;
	p->sizeleft = data->len;
	p->data = *data;
	p->retry = 0;
	p->err = 0;
	curl_easy_setopt(p->req, CURLOPT_READDATA, p);
	curl_easy_setopt(p->req, CURLOPT_READFUNCTION, payload_s3_memread_callback);
	curl_easy_setopt(p->req, CURLOPT_INFILESIZE_LARGE, (curl_off_t)p->sizeleft);
	curl_easy_setopt(p->req, CURLOPT_USERAGENT, EDGE_USER_AGENT);
	*h = p;
	return 0;
}

static void
payload_s3_put_reset(struct payload_s3_write_ctx* p) {
	p->readptr = p->data.base;
	p->sizeleft = p->data.len;
}

static void
payload_s3_put_cleanup(struct payload_s3_write_ctx* h) {
	assert(h);
	if (h->req)
		curl_easy_cleanup(h->req);
	if (h->headers)
		curl_slist_free_all(h->headers);
	if (h->ferr)
		fclose(h->ferr);
	je_free(h);
}

int
payload_s3_put(struct payload_s3 *ctx, const char* key, uv_buf_t *data)
{
	CURLcode res;
	uint64_t begin = get_timestamp_us();
	int retry = 1;

	perf_cap_update(&g_put_perf, 0, 0);
	struct payload_s3_write_ctx* h = NULL;
	int err = payloas_s3_put_create(ctx, key, data, &h);
	if (err) {
		log_error(lg, "Couldn't create S3 PUT context: %d", err);
		return err;
	}

_retry:
	res = curl_easy_perform(h->req);
	if (res != CURLE_OK) {
		log_error(lg, "curl_easy_perform() failed: %s", curl_easy_strerror(res));
		err = -EIO;
	} else {
		long response_code;
		res = curl_easy_getinfo(h->req, CURLINFO_RESPONSE_CODE, &response_code);
#if ERR_INJ
		int resp = err_inj();
		if (resp)
			response_code = resp;
#endif
		if (res != CURLE_OK) {
			log_error(lg, "curl_easy_getinfo() returned a code %d (%s) after PUT",
				res, curl_easy_strerror(res));
			err = -EIO;
		} else if (response_code == 503) {
			/*
			 *  it might receive HTTP 503 slowdown responses
			 *  Retry request after a few seconds
			 */
			if (retry <= 80) {
				usleep(retry*100000);
				retry <<= 1;
				payload_s3_put_reset(h);
				goto _retry;
			} else {
				log_error(lg, "S3 PUT 503 response code after several attempts, giving up");
				err = -EIO;
			}
		} else if ((response_code / 100) != 2)
			err = -EIO;
	}
	payload_s3_put_cleanup(h);
	if (!err)
		perf_cap_update(&g_put_perf, data->len, get_timestamp_us() - begin);
	return err;
}

static int
payload_s3_put_multi_perform(struct payload_s3 *ctx, struct payload_s3_write_ctx** reqs,
	size_t n_reqs, struct perf_cap* perf) {
	assert(ctx);
	CURLMcode res = 0;
	size_t i = 0;
	int err = 0;
	uint64_t ts = get_timestamp_us();
	uint64_t delay = 1;

	CURLM* cm = curl_multi_init();
	if (!cm) {
		log_error(lg, "Couldn't create curl_multi handle");
		return -ENOMEM;
	}

	for (; i < n_reqs; i++) {
		res = curl_multi_add_handle(cm, reqs[i]->req);
		if (CURLM_OK != res) {
			log_error(lg, "Couldn't add S3 handle to curl_multi: %d",
				res);
			goto _exit;
		}
		perf_cap_update(perf, 0, 0);
	}

_retry:;
	int still_running = 0, repeats = 0;
	do {
		int numfds;
		res = curl_multi_perform(cm, &still_running);
		if(res == CURLM_OK ) {
			/* wait for activity, timeout or "nothing" */
			res = curl_multi_wait(cm, NULL, 0, 1000, &numfds);
		}

		if(res != CURLM_OK) {
			log_error(lg, "curl_multi failed, code %d.\n", res);
			err = -EIO;
			goto _exit;
		}

		/* 'numfds' being zero means either a timeout or no file descriptors to
		 * wait for. Try timeout on first occurrence, then assume no file
		 * descriptors and no file descriptors to wait for means wait for 100
		 * milliseconds.
		*/

		if(!numfds) {
			repeats++; /* count number of repeated zero numfds */
			if(repeats > 1) {
				usleep(100*1000); /* sleep 100 milliseconds */
			}
		} else
			repeats = 0;
	} while(still_running);

	/**
	 * Check connection status. Possibly repeat some of them
	 */
	struct CURLMsg *m;
	int n_retry = 0;
	do {
		int msgq = 0, will_retry = 0;
		m = curl_multi_info_read(cm, &msgq);
		if(m && (m->msg == CURLMSG_DONE)) {
			CURL *e = m->easy_handle;
			long response_code = 0;
			struct payload_s3_write_ctx* h = NULL;
			for (size_t i = 0; i < n_reqs; i++) {
				if (reqs[i] && reqs[i]->req == e) {
					h = reqs[i];
					break;
				}
			}
			assert(h);
			if (m->data.result != CURLE_OK)
				/* Something went wrong. Give it more tries */
				will_retry = 1;
			else {
				res = curl_easy_getinfo(e, CURLINFO_RESPONSE_CODE, &response_code);
				if (err != CURLM_OK) {
					log_error(lg, "curl_easy_getinfo() failed with code %d(%s)",
						res, curl_easy_strerror(res));
					h->err = -EIO;
				} else {
#if ERR_INJ
					int resp = err_inj();
					if (resp)
						response_code = resp;
#endif
					if (response_code == 503) {
						/* AWS slowdown response */
						will_retry = 1;
					} else if ((response_code / 100) != 2)
						h->err = response_code;
					else {
						h->err = 0;
						perf_cap_update(perf, h->data.len,
							get_timestamp_us() - ts);
					}
				}
			}
			if (will_retry) {
				if(++h->retry > 5) {
					h->err = -EIO;
					log_warn(lg, "S3 PUT execution error, response %ld",
						response_code);
					curl_multi_remove_handle(cm, e);
				} else {
					n_retry++;
					if (h->data.len)
						payload_s3_put_reset(h);
				}
			} else
				curl_multi_remove_handle(cm, e);
		}
	} while(m);

	if (n_retry && delay < 80) {
		usleep(delay*100);
		delay <<= 1;
		goto _retry;
	}

_exit:
	for (size_t j = 0; j < i; j++) {
		if (reqs[j]) {
			if (!err && reqs[j]->err)
				err = reqs[j]->err;
			payload_s3_put_cleanup(reqs[j]);
		}
	}
	if (cm)
		curl_multi_cleanup(cm);
	return err;
}

int
payload_s3_put_multi(struct payload_s3 *ctx, uv_buf_t* keys, uv_buf_t* data, size_t n) {
	assert(ctx);
	assert(keys);
	assert(data);
	struct payload_s3_write_ctx* hdls[n];
	CURLMcode res = 0;
	size_t i = 0;
	int err = 0;

	for (; i < n; i++) {
		char* key = keys[i].base;
		uv_buf_t val = data[i];
		int err= payloas_s3_put_create(ctx, key, &val, hdls + i);
		if (err) {
			log_error(lg, "Couldn't create S3 PUT request: %d", err);
			for (size_t j = 0; j <= i; j++)
				payload_s3_put_cleanup(hdls[j]);
			return err;
		}
	}
	return payload_s3_put_multi_perform(ctx,hdls, n, &g_put_perf);
}


struct payload_s3_read_ctx {
	uv_buf_t *chunk;
	size_t netlen;
};

static size_t
payload_s3_memwrite_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t realsize = size * nmemb;
	struct payload_s3_read_ctx *read_ctx = (struct payload_s3_read_ctx *)userp;
	uv_buf_t *chunk = read_ctx->chunk;

	if (read_ctx->netlen + realsize > chunk->len) {
		chunk->base = je_realloc(chunk->base, read_ctx->netlen + realsize);
		chunk->len = read_ctx->netlen + realsize;
	}

	memcpy(&(chunk->base[read_ctx->netlen]), contents, realsize);
	read_ctx->netlen += realsize;

	return realsize;
}

int
payload_s3_get(struct payload_s3 *ctx, const char* key, uv_buf_t *outbuf)
{
	CURL *curl;
	CURLcode res;
	char url[2048];
	char date_header[32];
	char auth_header[256];
	char errbuf[CURL_ERROR_SIZE];
	struct curl_slist *headers = NULL;
	int delay = 1;
	int err = 0;
	uv_buf_t ub;

	/*
	 * Key format string is <vdev_str>/<chid_str>
	 * Cut off the vdev string
	 */
	char* chidstr = strchr(key, '/');
	assert(chidstr);
	chidstr++;

	uint64_t ts = get_timestamp_us();
	if (ctx->cache) {
		err = get_cache_find(ctx->cache, chidstr, &ub);
		if (!err) {
			assert(ub.base);
			memcpy(outbuf->base, ub.base, ub.len);
			perf_cache_hit(&g_get_perf);
			perf_cap_update(&g_get_perf, 0, 0);
			perf_cap_update(&g_get_perf, outbuf->len,
				get_timestamp_us() - ts);
			return 0;
		}
		err = 0;
	}

	pthread_mutex_lock(&ctx->get_lock);
	while (ctx->parallel_gets >= ctx->parallel_gets_max)
		pthread_cond_wait(&ctx->get_cond, &ctx->get_lock);
	ctx->parallel_gets++;
	pthread_mutex_unlock(&ctx->get_lock);

	ts = get_timestamp_us();

	perf_cap_update(&g_get_perf, 0, 0);
	sprintf(url, "%s/%s", ctx->bucket_url, key);

	payload_s3_sign_request(ctx, key, "GET", date_header, auth_header);
	headers = curl_slist_append(headers, auth_header);
	headers = curl_slist_append(headers, date_header);
	headers = curl_slist_append(headers, "x-amz-content-sha256: UNSIGNED-PAYLOAD");

	struct payload_s3_read_ctx read_ctx = { .chunk = outbuf, .netlen = 0 };

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_SHARE, ctx->share);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, payload_s3_memwrite_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&read_ctx);
	curl_easy_setopt(curl, CURLOPT_USERAGENT, EDGE_USER_AGENT);

	errbuf[0] = 0;
	curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
	if (curl_verb) {
		curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, payload_s3_trace);
		curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
	}

	FILE *pfd = fopen("/dev/null", "w");
	if (pfd)
		curl_easy_setopt(curl, CURLOPT_STDERR, pfd);

_retry:
	res = curl_easy_perform(curl);
	if (res != CURLE_OK) {
		log_error(lg, "curl_easy_perform() failed: %s", curl_easy_strerror(res));
		err = -EIO;
	} else {
		long response_code;
		res = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
#if ERR_INJ
		int resp = err_inj();
		if (resp)
			response_code = resp;
#endif
		if (res == CURLE_OK) {
			if (response_code == 503) {
				if (delay < 80) {
					usleep(delay*100000);
					delay <<= 1;
					read_ctx.chunk = outbuf;
					read_ctx.netlen = 0;
					goto _retry;
				} else {
					log_error(lg, "S3 GET giving up on the 503 slowdown after several attempts");
					err = -EIO;
				}
			} else if ((response_code / 100) != 2) {
				log_error(lg, "S3 GET response code %ld", response_code);
				err = -ENOENT;
			}
		} else {
			log_error(lg, "curl_easy_getinfo() on GET error %d (%s),"
				" response code %ld", res, curl_easy_strerror(res),
				response_code);
		}
	}
	curl_easy_cleanup(curl);
	curl_slist_free_all(headers);
	if (pfd)
		fclose(pfd);
	if (!err) {
		perf_cap_update(&g_get_perf, outbuf->len,
			get_timestamp_us() - ts);
	}
	pthread_mutex_lock(&ctx->get_lock);
	ctx->parallel_gets--;
	pthread_cond_signal(&ctx->get_cond);
	pthread_mutex_unlock(&ctx->get_lock);
	if (ctx->cache)
		get_cache_insert(ctx->cache, chidstr, *outbuf);
	return err;
}


static int
payloas_s3_delete_create(struct payload_s3 *ctx, const char* key,
	struct payload_s3_write_ctx** h) {
	struct payload_s3_write_ctx* p = je_calloc(1, sizeof(struct payload_s3_write_ctx));

	CURLcode res;
	char url[2048];
	char date_header[32];
	char auth_header[256];
	int err = 0;

	sprintf(url, "%s/%s", ctx->bucket_url, key);

	payload_s3_sign_request(ctx, key, "DELETE", date_header, auth_header);
	p->headers = curl_slist_append(p->headers, auth_header);
	p->headers = curl_slist_append(p->headers, date_header);
	p->headers = curl_slist_append(p->headers, "x-amz-content-sha256: UNSIGNED-PAYLOAD");

	p->req = curl_easy_init();
	curl_easy_setopt(p->req, CURLOPT_HTTPHEADER,p->headers);
	curl_easy_setopt(p->req, CURLOPT_URL, url);
	curl_easy_setopt(p->req, CURLOPT_SHARE, ctx->share);
	curl_easy_setopt(p->req, CURLOPT_CUSTOMREQUEST, "DELETE");
	curl_easy_setopt(p->req, CURLOPT_USERAGENT, EDGE_USER_AGENT);
	p->data.base = NULL;
	p->data.len = 0;

	p->errbuf[0] = 0;
	curl_easy_setopt(p->req, CURLOPT_ERRORBUFFER, p->errbuf);
	if (curl_verb) {
		curl_easy_setopt(p->req, CURLOPT_DEBUGFUNCTION, payload_s3_trace);
		curl_easy_setopt(p->req, CURLOPT_VERBOSE, 1L);
	}

	p->ferr = fopen("/dev/null", "w");
	if (p->ferr)
		curl_easy_setopt(p->req, CURLOPT_STDERR, p->ferr);
	*h = p;
	return 0;
}

int
payload_s3_delete(struct payload_s3 *ctx, const char *key)
{
	struct payload_s3_write_ctx* dc = NULL;
	CURLcode res;
	uint64_t delay = 1;

	int err = payloas_s3_delete_create(ctx, key, &dc);
	if (err) {
		log_error(lg, "payloas_s3_delete_create() error %d, key %s", err, key);
		return err;
	}
	assert(dc);

_retry:
	res = curl_easy_perform(dc->req);
	if (res != CURLE_OK) {
		log_error(lg, "curl_easy_perform() failed: %s", curl_easy_strerror(res));
		err = -EIO;
	} else {
		long response_code;
		res = curl_easy_getinfo(dc->req, CURLINFO_RESPONSE_CODE, &response_code);
#if ERR_INJ
		int resp = err_inj();
		if (resp)
			response_code = resp;
#endif

		if (res == CURLE_OK) {
			if (response_code == 503) {
				if (delay < 80) {
					usleep(delay*100000);
					delay <<= 1;
					goto _retry;
				} else {
					log_error(lg, "S3 DELETE giving up on the 503 slowdown after several attempts");
					err = -EIO;
				}
			} else if ((response_code / 100) == 4)
				err = -ENOENT;
			else if ((response_code / 100) != 2)
				err = -EIO;
		}
	}
	payload_s3_put_cleanup(dc);
	return err;
}

int
payload_s3_delete_multi(struct payload_s3 *ctx, uv_buf_t* keys, size_t n) {
	assert(ctx);
	assert(keys);
	struct payload_s3_write_ctx* hdls[n];
	CURLMcode res = 0;
	size_t i = 0;
	int err = 0;

	for (; i < n; i++) {
		char* key = keys[i].base;
		int err= payloas_s3_delete_create(ctx, key, hdls + i);
		if (err) {
			log_error(lg, "Couldn't create S3 DEL request: %d", err);
			for (size_t j = 0; j <= i; j++)
				payload_s3_put_cleanup(hdls[j]);
			return err;
		}
	}
	return payload_s3_put_multi_perform(ctx,hdls, n, &g_del_perf);
}

int
payload_s3_init(char *url, char *region, char *keyfile, uint64_t cache_entries_max,
	struct payload_s3 **ctx_out)
{
	char key[256] = { 0 };
	struct payload_s3 *ctx = je_calloc(1, sizeof(struct payload_s3));
	if (!ctx)
		return -ENOMEM;

	ctx->port = 80;
	ctx->path[0] = '/';

	int ok = 0;
	if (sscanf(url, "http://%1023[^:]:%i/%2047[^\n]", ctx->host, &ctx->port, &ctx->path[1]) == 3) { ok = 1;}
	else if (sscanf(url, "http://%1023[^/]/%2047[^\n]", ctx->host, &ctx->path[1]) == 2) { ok = 1;}
	else if (sscanf(url, "http://%1023[^:]:%i[^\n]", ctx->host, &ctx->port) == 2) { ok = 1;}
	else if (sscanf(url, "http://%1023[^\n]", ctx->host) == 1) { ok = 1;}
	else if (sscanf(url, "https://%1023[^/]/%2047[^\n]", ctx->host, &ctx->path[1]) == 2) { ok = 1; ctx->port=443; }
	else if (sscanf(url, "https://%1023[^\n]", ctx->host) == 1) { ok = 1; ctx->port = 443; }

	if (!ok) {
		je_free(ctx);
		log_error(lg, "Cannot parse payload_s3_bucket_url");
		return -EBADF;
	}
	ctx->bucket_url = je_strdup(url);
	ctx->aws_region = je_strdup(region);

	int fd = open(keyfile, 0);
	if (fd == -1) {
		je_free(ctx);
		log_error(lg, "Cannot open the key file %s", keyfile);
		return -errno;
	}
	if (read(fd, &key[0], 256) == -1) {
		close(fd);
		je_free(ctx);
		log_error(lg, "Cannot read the key file %s", keyfile);
		return -errno;
	}
	close(fd);

	char *saveptr = NULL;
	char *access = strtok_r(key, ",", &saveptr);
	char *secret = strtok_r(NULL, ",", &saveptr);
	if (!access || !secret) {
		je_free(ctx);
		log_error(lg, "Key file format not recognized");
		return -EBADF;
	}
	char secret_trimmed[256];
	sscanf(secret, "%s", secret_trimmed);

	pthread_mutex_init(&ctx->conn_lock, NULL);
	pthread_mutex_init(&ctx->get_lock, NULL);
	ctx->parallel_gets = 0;
	ctx->parallel_gets_max = 256;

	if (__sync_bool_compare_and_swap(&curl_global, 0, 1))
		curl_global_init(CURL_GLOBAL_ALL);

	ctx->access_key = je_strdup(access);
	ctx->secret_key = je_strdup(secret_trimmed);
	ctx->share = curl_share_init();
	curl_share_setopt(ctx->share, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);
	curl_share_setopt(ctx->share, CURLSHOPT_LOCKFUNC, payload_s3_lock);
	curl_share_setopt(ctx->share, CURLSHOPT_UNLOCKFUNC, payload_s3_unlock);
	curl_share_setopt(ctx->share, CURLSHOPT_USERDATA, ctx);

	if (cache_entries_max)
		get_cache_create(cache_entries_max, &ctx->cache);
	else
		ctx->cache = NULL;

	*ctx_out = ctx;
	return 0;
}

void
payload_s3_destroy(struct payload_s3 *ctx)
{
	while(ctx->parallel_gets)
		usleep(10000);
	curl_share_cleanup(ctx->share);
	pthread_mutex_destroy(&ctx->conn_lock);
	pthread_mutex_destroy(&ctx->get_lock);
	pthread_cond_destroy(&ctx->get_cond);
	je_free(ctx->aws_region);
	je_free(ctx->access_key);
	je_free(ctx->secret_key);
	je_free(ctx->bucket_url);
	if (ctx->cache)
		get_cache_destroy(ctx->cache);
	je_free(ctx);
}
