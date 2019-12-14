/*
 * s3_payload_bench.c
 *
 *  Created on: Dec 3, 2019
 *      Author: root
 */

#ifndef TOOLS_S3_PAYLOAD_BENCH_C_
#define TOOLS_S3_PAYLOAD_BENCH_C_

#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

#include "ccowutil.h"
#include "../libreptrans/payload-s3.h"

#define CHIDS_RANDOM (1 << 0)
#define CHIDS_STORE (1 << 1)
#define CHIDS_FROM_CACHE (1 << 2)

Logger lg; // without a logger libreptrans crashes

static pthread_mutex_t g_get_lock;
static int64_t g_get_index = 0;

struct get_arg {
	struct payload_s3 *ctx;
	const char* key;
	uv_buf_t outbuf;
};

void*
get_thread(void* arg) {
	struct get_arg* g_list = arg;
	do {
		uv_mutex_lock(&g_get_lock);
		if (g_get_index < 0) {
			uv_mutex_unlock(&g_get_lock);
			break;
		}
		struct get_arg* p = g_list + g_get_index--;
		uv_mutex_unlock(&g_get_lock);
		int err = payload_s3_get(p->ctx, p->key, &p->outbuf);
		if (err) {
			log_error(lg, "Payload GET returned an error %d for key %s",
				err, p->key);
		}
	} while (g_get_index >= 0);
	return NULL;
}

int main(int argc, char *argv[]) {

	lg = Logger_create("s3bench");
	char* key_file = NULL;
	char* secret = NULL;
	char* url = NULL;
	char* region = "us-west-1";
	size_t conn = 1;
	size_t chunk_size = 512*1024;
	size_t n_chunks = 10000;
	size_t burst = 16;
	struct payload_s3* ctx = NULL;
	int get_only = 0;

	while (1) {
		int opt_index;
		int c;
		static struct option long_options[] = {
			{ "help", 0, 0, 'h' },
			{"url", 0, 0, 'u' },
			{"key-file", 0, 0, 'f' },
			{"region", 0, 0, 'r' },
			{ "chunk-size", 0, 0, 'c' },
			{ "burst-size", 0, 0, 'b'},
			{ "upload_size", 0, 0, 's' },
			{ "get-only", 0, 0, 'g' },
			{ NULL }};

		c = getopt_long(argc, argv, "hu:f:b:r:c:s:g", long_options,
		                &opt_index);

		if (c == -1)
			break;

		switch (c) {

		case 'u':
			url = je_strdup(optarg);
			break;
		case 'r':
			region = je_strdup(optarg);
			break;
		case 'f':
			key_file = je_strdup(optarg);
			break;
		case 'c':
			chunk_size = atol(optarg);
			break;

		case 's':
			n_chunks = atol(optarg);
			break;

		case 'h':
			printf("\n\tUsage: s3_payload_bench [-u <S3 URL>] "
				"[-f <S3-secret-file>][-r <AWS-region>] "
				"[-b <#conn>] [-c <chunk-size-bytes>] "
				"[-s <number-of-chunks]\n");
			exit(0);
			break;
		case 'b':
			burst = atol(optarg);
			break;

		case 'g':
			get_only = 1;
			break;

		default:
			fprintf(stderr, "invalid options. Use -h for help.");
			exit(1);
		}
	}

	if (!url)
		url = getenv("AWS_URL");

	if (!key_file || !url) {
		fprintf(stderr, "ERROR: an AWS secret file or S3 bucket URL isn't specified.\n");
		exit(1);
	}

	int err = payload_s3_init(url, region, key_file, is_embedded() ? 20 : 400, &ctx);
	if (err) {
		fprintf(stderr, "Coudln't initialize S3 payload context: %d.\n", err);
		exit(1);
	}
	ctx->parallel_gets_max = burst;

	char* ptr = je_malloc(chunk_size + n_chunks);
	if (!ptr) {
		fprintf(stderr, "Coudln't allocate payload memory %lu bytes.\n",
				chunk_size + n_chunks);
		exit(1);
	}

	if (!get_only) {
		for (size_t i = 0; i < (chunk_size + n_chunks)/sizeof(uint64_t); i++)
			*(uint64_t*)ptr = rand();

		uv_buf_t keys[burst];
		uv_buf_t vals[burst];
		char buff[burst][PATH_MAX];

		for (size_t i = 0; i < burst; i++) {
			keys[i].base = buff[i];
			keys[i].len = PATH_MAX;
		}

		size_t pos = 0;
		size_t sent = 0;
		uint64_t ts = get_timestamp_us();
		do {
			size_t len = pos + burst > n_chunks ? n_chunks - pos : burst;
			for (size_t i = 0; i < len; i++) {
				sprintf(keys[i].base, "s3bench%016lu", pos + i);
				vals[i].base = ptr + pos + i;
				vals[i].len = chunk_size;
			}
			err = payload_s3_put_multi(ctx, keys, vals, len);
			if (err) {
				fprintf(stderr, "Cannot put chunks: %d.\n", err);
				goto _exit;
			}
			sent += len*chunk_size;
			if (get_timestamp_us() - ts > 1000UL*1000UL) {
				printf("PUT perf %.3f MB/s\n", (double)sent/(double)(get_timestamp_us() - ts));
				sent = 0;
				ts = get_timestamp_us();
			}
			pos += len;
		} while (pos < n_chunks);
	}

	struct get_arg* get_args = je_calloc(n_chunks, sizeof(struct get_arg));
	pthread_t* pthr = je_calloc(burst, sizeof(pthread_t));
	pthread_mutex_init(&g_get_lock, NULL);
	g_get_index = n_chunks-1;

	for (size_t i = 0; i < n_chunks; i++) {
		char buf[256];
		sprintf(buf, "s3bench%016lu", i);
		get_args[i].ctx = ctx;
		get_args[i].key = je_strdup(buf);
		get_args[i].outbuf.base = ptr;
		get_args[i].outbuf.len = chunk_size;
	}

	for (size_t i = 0; i < burst; i++) {
		err = pthread_create(pthr + i, NULL, get_thread, get_args);
		if (err) {
			log_error(lg, "Couldn't create a GET thread: %d", err);
			goto _get_exit;
		}
	}
	for (size_t i = 0; i < burst; i++) {
		pthread_join(pthr[i], NULL);
	}

_get_exit:
	for (size_t i = 0; i < n_chunks; i++) {
		je_free((void*)get_args[i].key);
	}
	je_free(get_args);
	je_free(pthr);

_exit:
	if (ctx)
		payload_s3_destroy(ctx);
	if (ptr)
		je_free(ptr);

}

#endif /* TOOLS_S3_PAYLOAD_BENCH_C_ */
