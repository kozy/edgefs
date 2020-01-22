#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include "ccowutil.h"
#include "ccow.h"
#include "ccow-impl.h"
#include "ccowd.h"
#include "opp-status.h"
#include "replicast.h"

#define CID_DEFAULT	"cltest"
#define TID_DEFAULT	"test"
#define BID_DEFAULT	"put-file-bucket-test"
#define OID_DEFAULT	"file-put"
#define ROOT_INODE_STR	".nexenta_nedge_nfs_root_2"
#define STR_MAXLEN	512
#define	PARALLEL_OPS_N	50


static uint64_t vdev_usage_summ = 0;
static uint32_t vdev_usage_number = 0;
static uint128_t* known_hosts = NULL;
static uint32_t n_known_hosts = 0;

struct vminfo {
	uint512_t vmchid;
	uint512_t nhid;
	char name[2048];
};

typedef int (*nfs_iter_cb) (ccow_t tc, const char* bucket, const char* inode,
	const char* oid, uint64_t ts, void* arg);

struct ostat_arg {
	time_t firstTS;
	int flags;
	int verbose;
	opp_status_t ostat;
	FILE* f;
	size_t count;
};

static void
usage(const char *argv0)
{
	printf(	"\n"
		"USAGE:\n"
		"	%s [-h] [-s] [-b bucket_name] [-o obj_id]\n"
		"		[-t tenent_name] [-c cluster_name] [-l log_str]\n"
		"\n"
		"	-h	Display this message and exit\n"
		"\n"
		"	-o	Specify object name. NFS object has to begin with a '/' symbol\n"
		"\n"
		"	-b	Specify bucket name\n"
		"\n"
		"	-c	Specify cluster name\n"
		"\n"
		"	-t	Specify tenant name\n"
		"\n"
		"	-V	Get verification progress\n"
		"\n"
		"	-s	Short output\n"
		"\n"
		"	-v	Verbose object(s) VM/NHID\n"
		"\n"
		"	-T	Server wait timeout, sec (0- disabled, default)\n"
		"\n"
		"	-x	eXtended info (valid only with -V)\n"
		"\n"
		"	-p	add Parity chunk info (valid only with -x)\n"
		"\n"
		"	-B	Start bucket verification. Object ID ins't required\n"
		"\n"
		"	-T	Check only objects newer than specified date. Only for bucket mode.\n"
		"		Format: MM-DD-YYYY H:M:S\n"
		"\n"
		"	-l <str> log requested info to server's log file\n"
		"		<str> can be made of:\n"
		"		- 'L' log lost CHIDs,\n"
		"		- 'N' log CHIDs if #VBRs = 0\n"
		"		- 'O' log CHIDs if #VBRs < #Replicas\n"
		"\n"
		"	-j	output in JSON format\n"
		"\n"
		"\n", argv0);

	exit(EXIT_SUCCESS);
}

static int
vdev_usage_avg_update(const uint128_t* host, uint64_t* vdev_usage,
	uint32_t n_vdevs) {
	static uint32_t hosts_max = 0;
	if (!known_hosts) {
		known_hosts = je_calloc(10, sizeof(uint128_t));
		if (!known_hosts) {
			fprintf(stderr, "Memory allocation error\n");
			return -ENOMEM;
		}
		hosts_max = 10;
	}
	/* Don't add the same host twice */
	for (uint32_t i = 0; i < n_known_hosts; i++) {
		if (!uint128_cmp(known_hosts + i, host))
			return 0;
	}
	if (n_known_hosts >= hosts_max) {
		known_hosts = je_realloc(known_hosts, hosts_max*2*sizeof(uint128_t));
		if (!known_hosts) {
			fprintf(stderr, "Memory allocation error\n");
			return -ENOMEM;
		}
		hosts_max *= 2;
	}
	known_hosts[n_known_hosts++] = *host;
	for (uint32_t i = 0; i < n_vdevs; i++)
		vdev_usage_summ += vdev_usage[i];
	vdev_usage_number += n_vdevs;
	return 0;
}

static uint64_t
vdev_usage_avg_get() {
	return vdev_usage_number ? vdev_usage_summ/vdev_usage_number : 0;
}

static void dump_json(const opp_status_t* ostat) {
	printf("{\"n_cpar\":%lu, \"n_cp\":%lu, \"n_cm_zl\":%lu, \"n_cm_tl\":%lu, "
		"\"n_cm_zl_pp\":%lu, \"n_cm_zl_verified\":%lu, \"n_cm_tl_verified\":%lu, "
		"\"n_cp_verified\":%lu, \"n_cpar_verified\":%lu, \"n_cm_zl_1vbr\":%lu, "
		"\"n_cm_tl_1vbr\":%lu, \"n_cp_1vbr\":%lu, \"n_cm_zl_lost\":%lu, "
		"\"n_cm_tl_lost\":%lu, \"n_cp_lost\":%lu, \"n_cpar_lost\":%lu, "
		"\"n_cm_zl_erc_err\":%lu, \"n_cm_tl_erc_err\":%lu,\"n_cm_zl_1rep\":%lu,"
		"\"n_cm_tl_1rep\":%lu,\"n_cp_erc_err\":%lu,\"n_cp_1rep\":%lu,", ostat->n_cpar,
		ostat->n_cp, ostat->n_cm_zl, ostat->n_cm_tl, ostat->n_cm_zl_pp,
		ostat->n_cm_zl_verified, ostat->n_cm_tl_verified, ostat->n_cp_verified,
		ostat->n_cpar_verified, ostat->n_cm_zl_1vbr, ostat->n_cm_tl_1vbr,
		ostat->n_cp_1vbr, ostat->n_cm_zl_lost, ostat->n_cm_tl_lost,
		ostat->n_cp_lost, ostat->n_cpar_lost, ostat->n_cm_zl_erc_err,
		ostat->n_cm_tl_erc_err, ostat->n_cm_zl_1repl, ostat->n_cm_tl_1repl,
		ostat->n_cp_erc_err, ostat->n_cp_1rep);
	char hoststr[UINT128_BYTES*2+1];
	uint128_dump(&ostat->hostid, hoststr, UINT128_BYTES*2+1);
	printf("\"hostid\":\"%s\", \"pp_algo\":%d, \"pp_data_number\":%d, "
		"\"pp_parity_number\":%d, \"pp_domain\":%d, \"vdevs_usage\":%lu,"
		" \"n_hosts\":%u}", hoststr, ostat->pp_algo,
		ostat->pp_data_number, ostat->pp_parity_number,
		ostat->pp_domain, vdev_usage_avg_get(), n_known_hosts);
}

static int
ecstat_calc_nhid(const char* cid, const char* tid, const char* bid,
	const char* oid, uint512_t* nhid) {
	int err = 0;
	crypto_state_t S;
	err = crypto_init_with_type(&S, HASH_TYPE_DEFAULT);
	if (err) {
		log_error(lg, "crypto_init: object hash id %d", err);
		return err;
	}

	err = crypto_update(&S, (uint8_t *)cid, strlen(cid) + 1);
	if (err) {
		log_error(lg, "crypto_update: object hash id %d", err);
		return err;
	}

	err = crypto_update(&S, (uint8_t *)tid, strlen(tid) + 1);
	if (err) {
		log_error(lg, "crypto_update: object hash id %d", err);
		return err;
	}

	err = crypto_update(&S, (uint8_t *)bid, strlen(bid) + 1);
	if (err) {
		log_error(lg, "crypto_update: object hash id %d", err);
		return err;
	}

	err = crypto_update(&S, (uint8_t *)oid, strlen(oid) + 1);
	if (err) {
		log_error(lg, "crypto_update: object hash id %d", err);
		return err;
	}

	crypto_final(&S, (uint8_t *)nhid);
	if (err)
		log_error(lg, "crypto_final: object hash id %d", err);
	return err;
}

/*
 * the @ts parameter. On input, it keeps min. timestamp (unless it's 0)
 *			Upon exit, it contains object timestamp fetched
 *			from object's metadata
 */
static int
object_stats_get(ccow_t tc, const char* bid, const char* oid, int flags,
	int verbose, uint64_t* ts_in_out, opp_status_t* ostat) {
	ccow_completion_t c;
	int multipart = 0;
	uint512_t vmchid, nhid;
	uint64_t size = 0, chunk_size = 0, gen = 0, creation_time = 0;
	struct vminfo* vms = NULL;
	int n_vms = 0;

	int err = ccow_create_completion(tc, NULL, NULL, 1, &c);
	if (err) {
		fprintf(stderr, "\nccow_create_completion error: %d\n", err);
		return err;
	}

	ccow_lookup_t iter;
	oid = oid ? oid : "";
	bid = bid ? bid : "";
	err = ccow_get(bid, strlen(bid) + 1, oid, strlen(oid) + 1, c, NULL, 0,
		0, &iter);
	if (err) {
		fprintf(stderr, "\nObject information retrieval error: %d\n", err);
		ccow_release(c);
		return err;
	}
	err = ccow_wait(c, -1);
	if (err) {
		return err;
	}
	struct ccow_metadata_kv *kv = NULL;
	while ((kv = ccow_lookup_iter(iter, CCOW_MDTYPE_METADATA | CCOW_MDTYPE_CUSTOM, -1))) {
		if (strcmp(kv->key, RT_SYSKEY_VM_CONTENT_HASH_ID) == 0) {
			memcpy(&vmchid, kv->value, sizeof(uint512_t));
		} else if (strcmp(kv->key, RT_SYSKEY_NAME_HASH_ID) == 0) {
			memcpy(&nhid, kv->value, sizeof(uint512_t));
		} else if (strcmp(kv->key, "multipart") == 0) {
			char* cptr = kv->value;
			multipart = *cptr == '2';
		} else if (strcmp(kv->key, RT_SYSKEY_LOGICAL_SIZE) == 0) {
			ccow_iterator_kvcast(CCOW_KVTYPE_UINT64, kv, &size);
		} else if (strcmp(kv->key, RT_SYSKEY_CHUNKMAP_CHUNK_SIZE) == 0) {
			ccow_iterator_kvcast(CCOW_KVTYPE_UINT32, kv, &chunk_size);
		} else if (strcmp(kv->key, RT_SYSKEY_TX_GENERATION_ID) == 0) {
			ccow_iterator_kvcast(CCOW_KVTYPE_UINT64, kv, &gen);
		} else if (strcmp(kv->key, RT_SYSKEY_CREATION_TIME) == 0) {
			ccow_iterator_kvcast(CCOW_KVTYPE_UINT64, kv, &creation_time);
		}
	}
	ccow_lookup_release(iter);
	if (ts_in_out && *ts_in_out && creation_time && *ts_in_out > creation_time) {
		/* Skip this object. It's too old */
		return -EBADF;
	}
	if (ts_in_out)
		*ts_in_out = creation_time;
	if (multipart) {
		/* The multipart object has a JSON string as a content.
		 * The JSON provides detailed info on parts.
		 * Collect all the part VMs, calculate their NHIDs
		 */
		int iovcnt = size / chunk_size + !!(size % chunk_size);
		char* iob = je_calloc(1, iovcnt*chunk_size);
		if (!iob) {
			fprintf(stderr, "Memory allcoation error");
			return -ENOMEM;
		}
		struct iovec *iov = je_malloc(iovcnt * sizeof(struct iovec));
		if (!iob) {
			je_free(iob);
			fprintf(stderr, "Memory allcoation error");
			return -ENOMEM;
		}
		for (int i = 0; i < iovcnt; i++) {
			iov[i].iov_len = chunk_size;
			iov[i].iov_base = iob + i*chunk_size;
		}
		err = ccow_create_completion(tc, NULL, NULL, 1, &c);
		if (err) {
			je_free(iob);
			je_free(iov);
			fprintf(stderr, "ccow_create_completion error: %d\n", err);
			return -EIO;
		}
		err = ccow_get(bid, strlen(bid) + 1, oid, strlen(oid) + 1, c, iov, iovcnt,
			0, &iter);
		if (err) {
			je_free(iob);
			je_free(iov);
			ccow_release(c);
			fprintf(stderr, "ccow_get error: %d\n", err);
			return -EIO;
		}
		err = ccow_wait(c, -1);
		if (err) {
			je_free(iob);
			je_free(iov);
			fprintf(stderr, "ccow wait error: %d\n", err);
			return -EIO;
		}
		json_value* opts = json_parse(iob, size+1);
		je_free(iob);
		je_free(iov);
		if (!opts) {
			fprintf(stderr, "Error parsing multipart VM's content");
			return -EINVAL;
		}
		if (opts->type != json_array) {
			json_value_free(opts);
			printf("VM's JSON isn't an array: %d\n", opts->type);
			return -EINVAL;
		}
		vms = je_calloc(opts->u.array.length + 1, sizeof(*vms));
		for (uint32_t i = 0; i < opts->u.array.length; i++) {
			json_value* item = opts->u.array.values[i];
			assert(item->type == json_object);
			struct vminfo* cur = vms + i;
			char* name = NULL;
			for (uint32_t j = 0; j < item->u.object.length; j++) {
				char *namekey = item->u.object.values[j].name;
				json_value *v = item->u.object.values[j].value;
				if (strcmp(namekey, "vm_content_hash_id") == 0) {
					uint512_fromhex(v->u.string.ptr,
						UINT512_BYTES*2+1, &cur->vmchid);
				} else if (strcmp(namekey, "name") == 0) {
					name = v->u.string.ptr;
					strcpy(cur->name, name);
					err = ecstat_calc_nhid(tc->cid, tc->tid, bid,
						name,&cur->nhid);
					if (err) {
						json_value_free(opts);
						je_free(vms);
						fprintf(stderr, "Error calculating NHID");
						return err;
					}
				}
			}
			if (verbose) {
				char chidstr[UINT512_BYTES*2+1];
				char nhidstr[UINT512_BYTES*2+1];
				uint512_dump(&cur->vmchid, chidstr, UINT512_BYTES*2+1);
				uint512_dump(&cur->nhid, nhidstr, UINT512_BYTES*2+1);
				printf("Part %d: obj %s, VMCHID %s, NHID %s\n",
					i+1, name, chidstr, nhidstr);
			}
			n_vms++;
		}
		json_value_free(opts);
	}
	if (!vms) {
		vms = je_calloc(1, sizeof(*vms));
		vms[n_vms].vmchid = vmchid;
		vms[n_vms++].nhid = nhid;
		if (verbose) {
			char chidstr[UINT512_BYTES*2+1];
			uint512_dump(&vmchid, chidstr, UINT512_BYTES*2+1);
			printf("VMCHID:\t%s\n", chidstr);
			uint512_dump(&nhid, chidstr, UINT512_BYTES*2+1);
			printf("NHID:\t%s\n", chidstr);
			printf("GEN:\t%lu\n", gen);
		}

	}
	memset(ostat, 0, sizeof(*ostat));
	if (multipart) {
		/* Lookup for VM of each part to make sure they are exist */
		int cnt = 0, lost = 0;
		c = NULL;
		for (int i = 0; i < n_vms; i++) {
			if (!c) {
				err = ccow_create_completion(tc, NULL, NULL, PARALLEL_OPS_N, &c);
				if (err) {
					je_free(vms);
					fprintf(stderr, "\nccow_create_completion error: %d\n", err);
					return err;
				}
			}
			err = ccow_chunk_lookup(c, &vms[i].vmchid, &vms[i].nhid, HASH_TYPE_DEFAULT, RD_ATTR_VERSION_MANIFEST, 1);
			if (err) {
				char chidstr[UINT512_STR_BYTES];
				char nhidstr[UINT512_STR_BYTES];
				uint512_dump(&vms[i].vmchid, chidstr, UINT512_STR_BYTES);
				uint512_dump(&vms[i].nhid, nhidstr, UINT512_STR_BYTES);
				fprintf(stderr, "\nVM lookup init error VMCHID %s NHID %s OBJ %s: %d\n",
					chidstr, nhidstr, vms[i].name, err);
				je_free(vms);
				return err;
			}
			cnt++;
			if ((cnt == PARALLEL_OPS_N) || (i == n_vms - 1)) {
				err = ccow_wait(c, -1);
				ccow_drop(c);
				if (err != 0) {
					ostat->n_cm_tl_lost++;
					ostat->n_cm_tl++;
					lost++;
				}
				ccow_release(c);
				c = NULL;
				cnt = 0;
			}
		}
		if (lost) {
			je_free(vms);
			return -EIO;
		}
	}

	int vdev_usage_count = 0;
	uint64_t vdev_usage_summ = 0;
	opp_status_t* req_stat = je_calloc(PARALLEL_OPS_N, sizeof(opp_status_t));
	if (!req_stat) {
		fprintf(stderr, "Memory allocation error");
		return -ENOMEM;
	}

	int cnt = 0;
	err = ccow_create_completion(tc, NULL, NULL, PARALLEL_OPS_N, &c);
	if (err) {
		fprintf(stderr, "\nccow_create_completion error: %d\n", err);
		je_free(req_stat);
		je_free(vms);
		return err;
	}
	for (int i = 0; i < n_vms; i++) {
		err = ccow_opp_satus_request(tc, &vms[i].vmchid, &vms[i].nhid, c,
			flags, req_stat + cnt);
		if (err) {
			fprintf(stderr, "\nError getting parity protection status: %d\n",
				err);
			je_free(req_stat);
			je_free(vms);
			return err;
		}
		cnt++;
		if ((cnt == PARALLEL_OPS_N) || (i == n_vms - 1)) {
			err = ccow_timed_wait(c, -1, 10000);
			ccow_drop(c);
			if (err) {
				if (verbose) {
					char chidstr[UINT512_BYTES*2+1];
					char nhidstr[UINT512_BYTES*2+1];
					uint512_dump(&vms[i].vmchid, chidstr, UINT512_BYTES*2+1);
					uint512_dump(&vms[i].nhid, nhidstr, UINT512_BYTES*2+1);
					if (err != -EBUSY)
						fprintf(stderr, "\nPart %d corrupted, VMCHID %s, NHID %s, name %s\n",
							i+1, chidstr, nhidstr, vms[i].name);
					else
						fprintf(stderr, "\nPart %d corrupted or timeout has expired, VMCHID %s, NHID %s, name %s\n",
							i+1, chidstr, nhidstr, vms[i].name);
				}
				ostat->n_cm_tl++;
				ostat->n_cm_tl_lost++;
			} else {
				for (int j = 0; j < cnt; j++) {
					ostat->n_cm_tl += req_stat[j].n_cm_tl;
					ostat->n_cm_zl += req_stat[j].n_cm_zl;
					ostat->n_cp += req_stat[j].n_cp;
					ostat->n_cpar += req_stat[j].n_cpar;
					ostat->n_cm_zl_verified += req_stat[j].n_cm_zl_verified;
					ostat->n_cm_tl_verified += req_stat[j].n_cm_tl_verified;
					ostat->n_cp_verified += req_stat[j].n_cp_verified;
					ostat->n_cpar_verified += req_stat[j].n_cpar_verified;
					ostat->n_cm_zl_1vbr += req_stat[j].n_cm_zl_1vbr;
					ostat->n_cm_tl_1vbr += req_stat[j].n_cm_tl_1vbr;
					ostat->n_cp_1vbr += req_stat[j].n_cp_1vbr;
					ostat->n_cp_1rep += req_stat[j].n_cp_1rep;
					ostat->n_cp_erc_err += req_stat[j].n_cp_erc_err;
					ostat->n_cm_zl_1repl += req_stat[j].n_cm_zl_1repl;
					ostat->n_cm_tl_1repl += req_stat[j].n_cm_tl_1repl;
					ostat->n_cm_zl_lost += req_stat[j].n_cm_zl_lost;
					ostat->n_cm_tl_lost += req_stat[j].n_cm_tl_lost;
					ostat->n_cp_lost += req_stat[j].n_cp_lost;
					ostat->n_cpar_lost += req_stat[j].n_cpar_lost;
					ostat->n_cm_zl_pp += req_stat[j].n_cm_zl_pp;
					ostat->n_cm_zl_erc_err += req_stat[j].n_cm_zl_erc_err;
					ostat->n_cm_tl_erc_err += req_stat[j].n_cm_tl_erc_err;
					if (ostat->n_cm_zl_pp && !ostat->pp_data_number) {
						ostat->pp_algo = req_stat[j].pp_algo;
						ostat->pp_data_number = req_stat[j].pp_data_number;
						ostat->pp_parity_number = req_stat[j].pp_parity_number;
						ostat->pp_domain = req_stat[j].pp_domain;
					}
					ostat->hostid = req_stat[j].hostid;
					err = vdev_usage_avg_update(&req_stat[j].hostid,
						req_stat[j].vdevs_usage,
						req_stat[j].n_vdevs);
					je_free(req_stat[j].vdevs_usage);
					if (err) {
						je_free(vms);
						je_free(req_stat);
						return err;
					}
				}
			}
			cnt = 0;
			memset(req_stat, 0, sizeof(opp_status_t)*PARALLEL_OPS_N);
			if (i != n_vms - 1) {
				err = ccow_create_completion(tc, NULL, NULL, PARALLEL_OPS_N, &c);
				if (err) {
					fprintf(stderr, "\nccow_create_completion error: %d\n", err);
					je_free(req_stat);
					return err;
				}
			}
		}
	}
	je_free(req_stat);
	return 0;
}

static int
unpackValue(void *value, size_t value_size, char *buf, size_t buf_size) {
	if (value_size == 0 || !value) {
		return 0;
	}
	int err = 0;
	uint8_t ver=0;
	uint64_t generation = 0;
	uint64_t size = 0;
	uint64_t inode = 0;
	uint64_t ts = 0;
	uint512_t vmchid;
	char etag[STR_MAXLEN] = "";
	char content_type[STR_MAXLEN] = "";
	char owner[STR_MAXLEN] = "";
	char srcip[STR_MAXLEN] = "";
	uint8_t object_deleted = 0;
	msgpack_u *u = msgpack_unpack_init(value, value_size, 0);
	err = msgpack_unpack_uint8(u, &ver);
	if (err) {
		goto _exit;
	}
	if (ver == 1) {
		err = msgpack_unpack_uint8(u, &object_deleted);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_uint64(u, &ts);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_uint64(u, &generation);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_uint512(u, &vmchid);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_str(u, etag, STR_MAXLEN);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_str(u, content_type, STR_MAXLEN);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_uint64(u, &size);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_uint64(u, &inode);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_str(u, owner, STR_MAXLEN);
		if (err) {
			goto _exit;
		}
		err = msgpack_unpack_str(u, srcip, STR_MAXLEN);
		if (err) {
			goto _exit;
		}
		char vmchid_buf[UINT512_BYTES * 2 + 1] = "";
		uint512_dump(&vmchid, vmchid_buf, UINT512_BYTES * 2 + 1);
		sprintf(buf,"%lu;%lu;%s;%s;%s;%lu;%u;%lu;%s;%s", ts, generation,
			vmchid_buf, etag, content_type, size, object_deleted, inode, owner, srcip);
		goto _exit;
	}
	if (ver == 2) {
		err = msgpack_unpack_str(u, buf, buf_size);
		goto _exit;
	}
	if (ver == 3) {
		err = msgpack_unpack_uint64(u, &inode);
		if (err)
			goto _exit;
		uint8_t type = (inode >> 60) & 3;
		sprintf(buf,"%lu;%u", inode, type);
		goto _exit;
	}
	if (ver == 4) {
		uint32_t st_mode, st_uid, st_gid;
		uint64_t st_dev, st_rdev, tv_sec;
		uint64_t st_atim_tv_sec, st_atim_tv_nsec;
		uint64_t st_mtim_tv_sec, st_mtim_tv_nsec;
		uint64_t st_ctim_tv_sec, st_ctim_tv_nsec;
		err = msgpack_unpack_uint32(u, &st_mode);
		if (err)
			goto _exit;
		err = msgpack_unpack_uint32(u, &st_uid);
		if (err)
			goto _exit;
		err = msgpack_unpack_uint32(u, &st_gid);
		if (err)
			goto _exit;
		err = msgpack_unpack_uint64(u, &st_dev);
		if (err)
			goto _exit;
		err = msgpack_unpack_uint64(u, &st_rdev);
		if (err)
			goto _exit;
		err =
		    msgpack_unpack_uint64(u,
		    (uint64_t *) &st_atim_tv_sec);
		if (err)
			goto _exit;
		err =
		    msgpack_unpack_uint64(u,
		    (uint64_t *) & st_atim_tv_nsec);
		if (err)
			goto _exit;
		err =
		    msgpack_unpack_uint64(u,
		    (uint64_t *) & st_mtim_tv_sec);
		if (err)
			goto _exit;
		err =
		    msgpack_unpack_uint64(u,
		    (uint64_t *) & st_mtim_tv_nsec);
		if (err)
			goto _exit;
		err =
		    msgpack_unpack_uint64(u,
		    (uint64_t *) & st_ctim_tv_sec);
		if (err)
			goto _exit;
		err =
		    msgpack_unpack_uint64(u,
		    (uint64_t *) & st_ctim_tv_nsec);
		if (err)
			goto _exit;
		sprintf(buf,"%u;%u;%u;%lu;%lu;%lu;%lu;%lu;%lu",
				st_mode, st_uid, st_gid,
				st_atim_tv_sec, st_atim_tv_nsec,
				st_mtim_tv_sec, st_mtim_tv_nsec,
				st_ctim_tv_sec, st_ctim_tv_nsec);
		goto _exit;
	}
	if (ver == 5) {
		const uint8_t *data;
		uint32_t nout;
		err = msgpack_unpack_raw(u, &data, &nout);
		if (!err)
		    memcpy(buf, data, nout);
		goto _exit;
	}

_exit:
		msgpack_unpack_free(u);
		return err;
}

static int
interate_nfs_bucket(ccow_t tc, const char* bid, size_t bid_size, const char* oid,
	size_t oid_size, const char* prefix, nfs_iter_cb cb, void* arg) {
	int err = 0;
	int res = 0;

	ccow_shard_context_t list_shard_context;
	ccow_lookup_t iter = NULL;
	msgpack_u *u = NULL;
	char marker[256] = "";

	do {
		err = ccow_shard_context_create((char*)oid, oid_size, 4, &list_shard_context);
		if (err) {
			log_error(lg, "ccow_shard_context_create error %d", err);
			return err;
		}

		err = ccow_sharded_get_list(tc, bid, bid_size, list_shard_context, marker,
			strlen(marker)+1, NULL, 10, &iter);

		ccow_shard_context_destroy(&list_shard_context);
		if (err) {
			if (iter)
				ccow_lookup_release(iter);
			log_error(lg, "ccow_shard_context_destroy error %d", err);
			return err;
		}

		struct ccow_metadata_kv *kv;
		void *t;
		size_t count = 0;
		do {
			t = ccow_lookup_iter(iter, CCOW_MDTYPE_NAME_INDEX, -1);
			kv = (struct ccow_metadata_kv *)t;
			if (kv == NULL)
				break;
			assert (kv->type == CCOW_KVTYPE_RAW);
			char buf[PATH_MAX] = "";
			err = unpackValue(kv->value, kv->value_size, buf, PATH_MAX);
			if (err) {
				if (iter)
					ccow_lookup_release(iter);
				log_error(lg,"Shard unpack error %d", err);
				return err;
			}
			count++;
			if (!strcmp(kv->key, ".") || !strcmp(kv->key, "..") || !strlen(kv->key))
				continue;
			char* p = strchr(buf, ';');
			if (p) {
				*p = 0;
				if (*(p+1) == '1') {
					/**
					 * This is an NFS directory, entering
					 */
					char pbuff[PATH_MAX];
					sprintf(pbuff, "%s/%s", prefix, kv->key);
					interate_nfs_bucket(tc, bid, bid_size, buf, sizeof(buf) + 1,
						pbuff, cb, arg);
				} else {
					if (cb) {
						char lbuf[PATH_MAX];
						sprintf(lbuf, "%s/%s", prefix, kv->key);
						err = cb(tc, bid, buf, lbuf, 0, arg);
						if (err)
							goto _exit;
					}
				}
			}
			strcpy(marker, kv->key);
		} while (kv != NULL);
		if (!count)
			break;
	} while (1);

_exit:
	if (iter)
		ccow_lookup_release(iter);

	return err;
}

static int
interate_bucket(ccow_t cl, const char* bid, size_t bid_size, nfs_iter_cb cb, void* arg) {
	int err = 0;
	char last_obj[PATH_MAX] = {0};
	ccow_lookup_t iter = NULL;
	struct iovec iov;
	iov.iov_base = last_obj;
	iov.iov_len = strlen(last_obj) + 1;
	ccow_completion_t c1;

	while (1) {
		err = ccow_create_completion(cl, NULL, NULL, 1, &c1);
		if (err) {
			log_error(lg, "Bucket clone: completion create error %d", err);
			goto _release;
		}
		iov.iov_len = strlen(last_obj) + 1;
		iov.iov_base = last_obj;
		err = ccow_tenant_get(cl->cid, cl->cid_size, cl->tid,
			cl->tid_size, bid, strlen(bid) + 1, "", 1, c1, &iov, 1,
			1000, CCOW_GET_LIST, &iter);
		if (err) {
			log_error(lg, "Source bucket list error %s/%s/%s: %d",
				cl->cid, cl->tid, bid, err);
			goto _release;
		}

		err = ccow_wait(c1, 0);
		if (err) {
			log_error(lg, "Source bucket list error (wait) %s/%s/%s: %d",
				cl->cid, cl->tid, bid, err);
			goto _release;
		}
		struct ccow_metadata_kv *kv = NULL;
		int idx = 0, processed = 0;
		while ((kv = ccow_lookup_iter(iter, CCOW_MDTYPE_NAME_INDEX, idx++)) != NULL) {
			/* Skip the last entry from previous iteration */
			if (!strcmp(kv->key, last_obj))
				continue;
			/* Skip multipart */
			if (strstr(kv->key, "\xEF\xBF\xBF{") == kv->key)
				continue;
			strcpy(last_obj, kv->key);

			msgpack_u u;
			msgpack_unpack_init_b(&u, kv->value, kv->value_size, 0);
			uint8_t ver = 0, deleted = 0;
			uint64_t ts = 0, gen = 0;
			uint512_t vmchid;
			err = msgpack_unpack_uint8(&u, &ver);
			if (err) {
				log_error(lg, "Bucket entry unpack error %d", err);
				goto _release;
			}

			err = msgpack_unpack_uint8(&u, &deleted);
			if (err) {
				log_error(lg, "Bucket entry unpack error %d", err);
				goto _release;
			}

			err = msgpack_unpack_uint64(&u, &ts);
			if (err) {
				log_error(lg, "Bucket entry unpack error %d", err);
				goto _release;
			}

			err = msgpack_unpack_uint64(&u, &gen);
			if (err) {
				log_error(lg, "Bucket entry unpack error %d", err);
				goto _release;
			}

			err = replicast_unpack_uint512(&u, &vmchid);
			if (err) {
				log_error(lg, "Bucket entry unpack error %d", err);
				goto _release;
			}
			processed++;
			if (cb) {
				err = cb(cl, bid, last_obj, last_obj, ts, arg);
				if (err) {
					err = 0;
					goto _release;
				}
			}
		}
		ccow_lookup_release(iter);
		iter = NULL;
		if (!processed)
			break;
	}

_release:
	ccow_release(c1);
	if (iter)
		ccow_lookup_release(iter);

	return err;
}

static int oid_to_inode_cb (ccow_t cl, const char* bucket, const char* oid,
	const char* path, uint64_t ts, void* arg) {
	char* buff = arg;
	if (!strcmp(path, buff)) {
		strcpy(buff, oid);
		return 100;
	}
	return 0;
}

static int object_stat_cb (ccow_t cl, const char* bucket, const char* oid,
	const char* path, uint64_t ts, void* arg) {
	struct ostat_arg* p = arg;

	if (ts && p->firstTS && p->firstTS < (time_t)ts) {
		/*
		 * If object creation time is provided, then compare
		 * filter out old objects
		 * If creation time is unknown, then postpone this task
		 */
		return 0;
	}
	ts = p->firstTS;
	int err = object_stats_get(cl, bucket, oid, p->flags, p->verbose, &ts, &p->ostat);
	if (err) {
		if (err == -EBADF) {
			/* Object is too old */
			return 0;
		}
		if (err == -ENOENT) {
			char buf[1024];
			if (ts) {
				time_t t = ts/1000000;
				struct tm tm;
				gmtime_r(&t, &tm);
				size_t n = strftime(buf, sizeof(buf), "%m-%d-%Y %H:%M:%S", &tm);
			} else
				strcpy(buf, "- -");
			if (p->f) {
				fprintf(p->f, "%s/%s/%s/%s;%s;;;;LOST;\n", cl->cid,
					cl->tid, bucket, path, buf);
			}
			printf("ERROR: object not found %s/%s/%s/%s, date %s\n",
				cl->cid, cl->tid, bucket, path, buf);
		} else {
			fprintf(stderr, "%s/%s/%s/%s object stat error %d\n",
				cl->cid, cl->tid, bucket, path, err);
		}
	}

	size_t total_chunks = p->ostat.n_cp + p->ostat.n_cm_zl + p->ostat.n_cm_tl;
	size_t total_1vbr = p->ostat.n_cp_1vbr + p->ostat.n_cm_zl_1vbr + p->ostat.n_cm_tl_1vbr;
	size_t lost = p->ostat.n_cp_lost + p->ostat.n_cm_tl_lost + p->ostat.n_cm_zl_lost;
	if (lost) {
		time_t t = ts/1000000;
		struct tm tm;
		gmtime_r(&t, &tm);
		char buf[1024];
		size_t n = strftime(buf, sizeof(buf), "%m-%d-%Y %H:%M:%S", &tm);
		if (p->f) {
			fprintf(p->f, "%s/%s/%s/%s;%s;%lu;%lu;%lu;DATALOSS;\n", cl->cid,
				cl->tid, bucket, oid, buf, lost, total_chunks, total_1vbr);
		}
		printf("ERROR: object %s, date %s, #chunks %lu, #lost %lu, #1vbr %lu\n",
				path, buf, total_chunks, lost, total_1vbr);
	} else if (total_chunks != total_1vbr) {
		printf("WARN: object %s is a data loss candidate: %lu chunks/manifest without VBR",
			path, total_chunks - total_1vbr);
	} else if (p->ostat.n_cm_tl_erc_err || p->ostat.n_cm_zl_erc_err || p->ostat.n_cp_erc_err) {
		printf("INFO: %s under-replication of %lu manifests and %lu payloads\n",
			path, p->ostat.n_cm_tl_erc_err + p->ostat.n_cm_zl_erc_err,
			p->ostat.n_cp_erc_err);
	} else {
		printf("%lu\r", ++p->count);
	}
	return 0;
}

int
main(int argc, char** argv) {

	int opt;
	int o_short = 0;
	char* cid = NULL;
	char* tid = NULL;
	char* bid = NULL;
	char* oid = NULL;
	int verify = 0;
	int ext = 0;
	int p_info = 0;
	char* lerr = 0;
	int json = 0;
	struct vminfo* vms = NULL;
	int n_vms = 0;
	uint512_t vmchid, nhid;
	int multipart = 0;
	uint64_t size = 0;
	uint32_t chunk_size = 0;
	uint64_t gen = 0;
	int batch = 0;
	char* log_file = NULL;
	struct ostat_arg oarg;

	while ((opt = getopt(argc, argv, "ho:b:c:t:sVxpl:jvBL:T:")) != -1) {
		switch(opt) {

			case 'o':
				oid = strdup(optarg);
				break;

			case 'b':
				bid = strdup(optarg);
				break;

			case 'c':
				cid = strdup(optarg);
				break;

			case 't':
				tid = strdup(optarg);
				break;

			case 'V':
				verify = 1;
				break;

			case 's':
				o_short = 1;
				break;

			case 'x':
				ext = 1;
				break;

			case 'p':
				p_info = 1;
				break;

			case 'l':
				lerr = strdup(optarg);
				break;

			case 'j':
				json = 1;
				break;

			case 'v':
				oarg.verbose = 1;
				break;

			case 'B':
				batch = 1;
				break;

			case 'L':
				log_file = strdup(optarg);
				break;

			case 'T':
			{
				struct tm ltm;
				char* p = strptime(optarg, "%m-%d-%Y %H:%M:%S", &ltm);
				if (!p) {
					fprintf(stderr, "The time stamp format is MM-DD-YYYY H:M:S\n");
					return -1;

				}
				oarg.firstTS = mktime(&ltm)*1000000UL;
				break;
			}

			case 'h':
			default:
				usage(argv[0]);
				break;
		}
	}

	if (!tid)
		tid = strdup(TID_DEFAULT);
	if (!cid)
		cid = strdup(CID_DEFAULT);
	if (!bid)
		bid = strdup(BID_DEFAULT);


	ccow_t cl;
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/etc/ccow/ccow.json", nedge_path());

	int ccow_fd = open(path, O_RDONLY);
	if (ccow_fd < 0) {
		fprintf(stderr, "ccow.json open error %d: %s\n",
			-errno, strerror(errno));
		return -errno;
	}

	char buf[16384];
	int err = read(ccow_fd, buf, 16383);
	if (err < 0) {
		fprintf(stderr, "\nccow.json read error %d: %s\n",
			-errno, strerror(errno));
		close(ccow_fd);
		return -EIO;
	}
	close(ccow_fd);
	buf[err] = 0;
	err = ccow_tenant_init(buf, cid, strlen(cid) + 1, tid, strlen(tid)+1,
		&cl);
	if (err) {
		fprintf(stderr, "\nccow init error: cluster or tenant ID is wrong\n");
		return -EINVAL;
	}

	oarg.flags = 0; /* EC-information only */
	oarg.ostat.n_cp = 0;
	if (verify)
		oarg.flags |= OPP_STATUS_FLAG_VERIFY;
	if (ext)
		oarg.flags |= OPP_STATUS_FLAG_ERC;
	if (p_info)
		oarg.flags |= OPP_STATUS_FLAG_CPAR;
	if (lerr) {
		if (strchr(lerr, 'L'))
			oarg.flags |= OPP_STATUS_FLAG_LERR;
		if (strchr(lerr, 'N'))
			oarg.flags |= OPP_STATUS_FLAG_LACKVBR;
		if (strchr(lerr, 'O'))
			oarg.flags |= OPP_STATUS_FLAG_MISSVBR;
	}

	if (batch) {
		/* Iterate bucket and check each object */
		char last_obj[PATH_MAX] = {0};
		ccow_lookup_t iter = NULL;
		ccow_completion_t c1;
		int ci = 0, total = 0;
		if (log_file) {
			oarg.f = fopen(log_file, "w");
			if (!oarg.f) {
				fprintf(stderr, "ERROR: Couldn't open log file %s for writing\n", log_file);
				return -1;
			}
		}

		interate_nfs_bucket(cl, bid, strlen(bid) + 1, ROOT_INODE_STR,
			strlen(ROOT_INODE_STR) + 1, "", object_stat_cb, &oarg);

		interate_bucket(cl, bid, strlen(bid) + 1, object_stat_cb, &oarg);

		if (oarg.f)
			fclose(oarg.f);
		printf("\n");
		return 0;
	}

	err = object_stats_get(cl, bid, oid, oarg.flags, oarg.verbose, NULL,
		&oarg.ostat);
	if (err == -ENOENT) {
		/* Possibly an NFS object. Trying to find inode*/
		char ibuf[PATH_MAX];
		strcpy(ibuf, oid);
		printf("Regular object not found, looking for NFS inode...\n");
		err = interate_nfs_bucket(cl, bid, strlen(bid) + 1, ROOT_INODE_STR,
			strlen(ROOT_INODE_STR) + 1, "", oid_to_inode_cb, ibuf);
		if (err == 100) {
			/* The inode found, doing stat */
			printf("%s found inode %s\n", oid, ibuf);
			err = object_stats_get(cl, bid, ibuf, oarg.flags, oarg.verbose, NULL,
				&oarg.ostat);
		} else
			err = -ENOENT;
	}
	if (err) {
		if (err == -ENOENT)
			fprintf(stderr, "Object not found\n");
		else
			fprintf(stderr, "Error %d\n", err);
		return err;
	}

	double ep = oarg.ostat.n_cm_zl ? (oarg.ostat.n_cm_zl_pp*100.0f/oarg.ostat.n_cm_zl) : 0.0f;
	size_t total_chunks = oarg.ostat.n_cp + oarg.ostat.n_cm_zl + oarg.ostat.n_cm_tl;
	size_t total_verified = oarg.ostat.n_cp_verified + oarg.ostat.n_cm_zl_verified + oarg.ostat.n_cm_tl_verified;
	size_t total_1vbr = oarg.ostat.n_cp_1vbr + oarg.ostat.n_cm_zl_1vbr + oarg.ostat.n_cm_tl_1vbr;
	double vp = total_chunks ? (total_verified*100.0f/total_chunks) : 0.0f;
	double vp1vbr = total_chunks ? (total_1vbr*100.0f/total_chunks) : 0.0f;

	if (o_short) {
		printf("%.2f %.2f %d:%d:%d:%d\n", vp, ep, oarg.ostat.pp_data_number,
			oarg.ostat.pp_parity_number, oarg.ostat.pp_algo, oarg.ostat.pp_domain);
	} else if (json) {
		dump_json(&oarg.ostat);
	} else {
		printf("EC encoding progress:\t\t%.2f%% (%lu/%lu)\n",
			ep, oarg.ostat.n_cm_zl_pp, oarg.ostat.n_cm_zl);
		if (oarg.ostat.n_cm_zl_pp) {
			printf("EC format:\t\t\t%d(D):%d(P):%d(A):%d(FD)\n",
				oarg.ostat.pp_data_number, oarg.ostat.pp_parity_number,
				oarg.ostat.pp_algo, oarg.ostat.pp_domain);
		}
		if (verify) {
			printf("Verification progress:\t\t%.2f%% (%lu/%lu)\n",
				vp, total_verified, total_chunks);
			if (ext) {
				printf("1VBR verify progress:\t\t%.2f%% (%lu/%lu)\n",
					vp1vbr, total_1vbr, total_chunks);
				uint64_t usage = vdev_usage_avg_get();
				printf("VDEVs usage (%u):\t\t%.5f%%\t\n",
					n_known_hosts, ((double)usage)/10000.0);
				printf("    \tTotal\t\tVerified\tLost\tERC err\n");
				printf("CM TL\t%lu\t\t%lu\t\t%lu\t\t%lu\n", oarg.ostat.n_cm_tl,
					oarg.ostat.n_cm_tl_verified, oarg.ostat.n_cm_tl_lost,
					oarg.ostat.n_cm_tl_erc_err);
				printf("CM ZL\t%lu\t\t%lu\t\t%lu\t\t%lu\n", oarg.ostat.n_cm_zl,
					oarg.ostat.n_cm_zl_verified, oarg.ostat.n_cm_zl_lost,
					oarg.ostat.n_cm_zl_erc_err);
				printf("CP  \t%lu\t\t%lu\t\t%lu\t\t%lu\n", oarg.ostat.n_cp,
					oarg.ostat.n_cp_verified, oarg.ostat.n_cp_lost,
					oarg.ostat.n_cp_erc_err);
				if (p_info) {
					printf("PARITY \t%lu\t\t%lu\t\t%lu\n", oarg.ostat.n_cpar,
						oarg.ostat.n_cpar_verified, oarg.ostat.n_cpar_lost);
				}
			}
		}
	}
	if (known_hosts)
		je_free(known_hosts);
	ccow_tenant_term(cl);
	return 0;
}

