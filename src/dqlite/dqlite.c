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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <getopt.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <ctype.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "ccowutil.h"
#include "ccowfsio.h"
#include "logger.h"
#include "dqlite.h"

#define DQLITE_CLUSTER_CONFIG	"%s/etc/dqlite/cluster.json"
#define CONFIG_BUF_SZ		8 * 1024
#define JOIN_RETRY		3
#define JOIN_TIME		100 * 1000
#define SLEEP_TIME		100 * 1000

struct ccow_dqlite_server {
	uint64_t	id;
	char		addr[INET_ADDRSTRLEN + 6 + 1];
	char		dir[PATH_MAX];
	dqlite_node	*node;
};

static struct ccow_dqlite_server cdq_server;
static int daemonize = 0, config_ready = 0, verbose = 0, init = 0;
static char pidfile[PATH_MAX];
static char config_buf[CONFIG_BUF_SZ];

/*
 * Not including header file due to conflict of type double_t between
 * CCOW and dqlite.
 */
extern int cdq_node_leave_cluster(uint64_t leader_id, char *leader_addr,
		uint64_t candidate_id);
extern int cdq_node_join_cluster(uint64_t leader_id, char *leader_addr,
		uint64_t candidate_id, char *candidate_addr);
extern int cdq_get_leader(char *json_buf, uint64_t *id, char **addr);
extern int cdq_get_cluster_nodes(uint64_t id, char *addr);
extern int cdq_is_leader(uint64_t id, char *addr);

Logger lg;

static int
ccow_dq_get_leader(uint64_t *leader_id, char **leader_ip)
{
	char config_file[PATH_MAX];
	int config_fd,
	    err;

	if (config_ready == 0) {
		/* Cluster info not coming from CLI. Look for config file */
		snprintf(config_file, PATH_MAX - 1,
			DQLITE_CLUSTER_CONFIG, nedge_path());
		config_fd = open(config_file, O_RDONLY);
		if (config_fd < 0) {
			log_error(lg, "%s open error: %d", config_file,
							   config_fd);
			return -EBADF;
		}
		if ((read(config_fd, config_buf, CONFIG_BUF_SZ)) < 0) {
			log_error(lg, "%s read error: %d", config_file, errno);
			close(config_fd);
			return -EIO;
		}
	}
	err = cdq_get_leader(config_buf, leader_id, leader_ip);
	return err;
}

static int
ccow_dq_leave_cluster()
{
	uint64_t leader_id;
	int err;
	char *leader_ip = NULL;

	err = ccow_dq_get_leader(&leader_id, &leader_ip);
	if (err != 0) {
		log_error(lg, "Failed to get leader node: %d\n", err);
		return -ENOENT;
	}

	log_notice(lg, "DQLite node (%lu) attempting to leave "
			"the cluster\n", cdq_server.id);
	err = cdq_node_leave_cluster(leader_id, leader_ip, cdq_server.id);

	if (err) {
		log_error(lg, "Failed to leave the cluster. error - %d\n", err);
	} else {
		log_notice(lg, "DQLite node left the cluster.\n");
	}
	return err;
}

static int
ccow_dq_join_cluster()
{
	uint64_t leader_id;
	int err;
	char *leader_ip = NULL;

	err = ccow_dq_get_leader(&leader_id, &leader_ip);
	if (err != 0) {
		log_error(lg, "Failed to get leader node: %d\n", err);
		return -ENOENT;
	}

	log_notice(lg, "DQLite node (%lu) attempting to join the cluster\n",
			cdq_server.id);
	/* Leader need not add itself */
	if (leader_id != cdq_server.id) {
		err = cdq_node_join_cluster(leader_id, leader_ip,
						cdq_server.id, cdq_server.addr);
	}
	if (err) {
		log_error(lg, "Failed to join the cluster. error - %d\n", err);
	} else {
		log_notice(lg, "DQLite node joined the cluster.\n");
	}
	return err;
}

static int
ccow_dq_stop()
{
	int err;

	/*
	 * If the DQLite node has not started then don't try to stop it.
	 * It will happen if ccow has not started and this process
	 * gets terminated.
	 */
	if (!init)
		return 0;

	err = dqlite_node_stop(cdq_server.node);
	if (err) {
		log_error(lg, "Failed to stop dqlite node, err: %d\n", err);
		return err;
	}

	ccow_fsio_term();
	dqlite_node_destroy(cdq_server.node);
	log_notice(lg, "Stopped DQLite node addr: %s, dir: %s\n",
			cdq_server.addr, cdq_server.dir);
	return 0;
}

static void
signal_handler(int signum)
{
	int err;
	static int terminating = 0;

	if (terminating) {
		fprintf(stderr, "Ignoring signal during termination ...\n");
		return;
	}

	if (signum == SIGHUP) {
		Logger_hup(lg);
		return;
	}

	terminating = 1;
	log_flush(lg);

	/* Ignore error and stop */
	err = ccow_dq_leave_cluster();

	if (daemonize) {
		unlink(pidfile);
	}

	log_notice(lg, "Stopping the DQLite node (%lu)\n", cdq_server.id);
	err = ccow_dq_stop();
	if (err) {
		log_error(lg, "Failed to stop ccow-sql\n");
	}

	log_flush(lg);
	signal(signum, SIG_DFL);
	raise(signum);
}

static int
ccow_dq_start()
{
	int err;

	err = dqlite_node_create(cdq_server.id, cdq_server.addr,
			cdq_server.dir, &cdq_server.node);
	if (err) {
		log_error(lg, "Failed to create dqlite node, err: %d\n", err);
		goto out_;
	}

	err = dqlite_node_set_bind_address(cdq_server.node, cdq_server.addr);
	if (err) {
		log_error(lg, "Failed to bind dqlite node address %s, "
				"err: %d\n", cdq_server.addr, err);
		goto out_;
	}

	err = dqlite_node_start(cdq_server.node);
	if (err) {
		log_error(lg, "Failed to start dqlite node, err: %d\n", err);
	} else {
		log_notice(lg, "Successfully started dqlite addr: %s, "
				"dir: %s\n", cdq_server.addr, cdq_server.dir);
		init = 1;
	}

out_:
	return err;
}

static void
ccow_dq_usage()
{
	printf("\n"
		"USAGE:\n"
		"       ccow_sql [-h] [-v] <[-i | --id] id>\n"
		"		<[-a | --addr] xx.xx.xx.xx:1234>\n"
	        "		<[-d | --dir] object-path>\n"
	        "		[-o | --opts] \"id1,ipaddr1;id2,ipaddr2;...\"\n"
		"\n"
		"       -h Display this help message and exit.\n"
		"\n"
		"       -i | --id id server id number"
		"\n"
		"       -a | --adddr xx.xx.xx.xx:1234 IPv4 address and port number"
		"\n"
		"       -d | --dir object-path Object that stores the database"
		"\n"
		"       -o | --opt list of cluster nodes"
		"\n"
		"       -v | --verbose dump cluster and leader information"
		"\n");
}

static void
node_str_to_json(char *nodes)
{
	char *list;
	char *node, *id_str, *ip;
	int id, err;

	if (nodes == NULL)
		return;

	config_ready = 1;
	list = strdup(nodes);

	sprintf(config_buf, "{\n\t\"nodes\": [\n");
	node = strtok(list, ";");
	while (node != NULL) {
		id_str = strchr(node, ',');
		ip = id_str + 1;
		*id_str = '\0';

		sprintf(config_buf, "%s\t\t{\n\t\t\t\"id\": %s,\n",
				config_buf, node);
		sprintf(config_buf, "%s\t\t\t\"ipaddr_port\": \"%s\"\n",
				config_buf, ip);
		node = strtok(NULL, ";");
		if (node != NULL)
			sprintf(config_buf, "%s\t\t},\n", config_buf);
		else
			sprintf(config_buf, "%s\t\t}\n", config_buf);
	}
	sprintf(config_buf, "%s\t]\n}", config_buf);
	free(list);
}

static int
is_ip_port_valid(char *ipport)
{
	char *ip, *port;
	struct in_addr in_addr;
	uint16_t port_num;
	int err;

	ip = strchr(ipport, ':');
	if (ip != NULL) {
		port = ip + 1;
		*ip = '\0';
	}
	err = inet_aton(ipport, &in_addr);
	if (err == 0) {
		fprintf(stderr, "Invalid IP-address: %s" "\n", ipport);
		return 0;
	}
	err = sscanf(port, "%hu", &port_num);
	if (err <= 0) {
		fprintf(stderr, "Invalid server port: " "%s\n", port);
		return 0;
	}
	while(*port != '\0') {
		if (!isdigit(*port++)) {
			port = ip + 1;
			fprintf(stderr, "Invalid server port: " "%s\n", port);
			return 0;
		}
	}
	return 1;
}

static int
is_nodes_str_valid(char *nodes)
{
	char *list;
	char *node, *id_str, *ip;
	int id, err;

	if (nodes == NULL)
		return 0;

	list = strdup(nodes);

	node = strtok(list, ";");
	while (node != NULL) {
		id_str = strchr(node, ',');
		if (id_str == NULL) {
			fprintf(stderr, "Expecting nodeid and IP address\n");
			return 0;
		}
		ip = id_str + 1;
		*id_str = '\0';
		err = sscanf(node, "%d", &id);
		if (err <= 0 || !is_ip_port_valid(ip)) {
			fprintf(stderr, "Invalid node id %d(%s) or IP-port %s\n",
					id, node, ip);
			free(list);
			return 0;
		}
		node = strtok(NULL, ";");
	}
	free(list);
	return 1;
}

static int
ccow_dq_parse_options(int argc, char *argv[])
{
	int option_index = 0, err = 0, c;
	char validation_buf[INET_ADDRSTRLEN + 6];
	char *cluster_nodes;

	static struct option long_options[] = {
		{"help",        0,              0,  'h' },
		{"id", required_argument, 0,  'i' },
		{"addr", required_argument, 0,  'a' },
		{"dir", required_argument, 0,  'd' },
		{"opts", required_argument, 0,  'o' },
		{"verbose", required_argument, 0,  'v' },
		{0,         0,                   0,  0 }
	};

	memset(&cdq_server, 0, sizeof cdq_server);
	while (1) {
		c = getopt_long(argc, argv, "hvo:i:a:d:",
				long_options, &option_index);
		if (c == -1)
			break;

		switch (c) {
			case 'i':
				err = sscanf(optarg, "%"PRIu64, &cdq_server.id);
				if (err <= 0) {
					fprintf(stderr, "Invalid server id: %s\n",
							optarg);
					return -EINVAL;
				}
				break;
			case 'a':
				if (optarg == NULL)
					return -EINVAL;
				/*
				 * We assume program calling this program has
				 * validated address and port
				 */
				strncpy(cdq_server.addr, optarg,
					INET_ADDRSTRLEN + 6 );
				strncpy(validation_buf, optarg,
					INET_ADDRSTRLEN + 6 );
				if (!is_ip_port_valid(validation_buf))
					return -EINVAL;
				break;
			case 'd':
				/*
				 * We assume program calling this program has
				 * validated cluster, tenant and bucket in
				 * this path
				 */
				strncpy(cdq_server.dir, optarg, PATH_MAX - 1);
				break;
			case 'o':
				if (!is_nodes_str_valid(optarg))
					return EINVAL;
				cluster_nodes = optarg;
				break;
			case 'v':
				verbose = 1;
				break;
			case '?':
			case 'h':
				ccow_dq_usage();
				break;
			default:
				ccow_dq_usage();
				break;
		}

	}/* while */

	if (cdq_server.id == 0 || cdq_server.dir[0] == '\0' ||
		cdq_server.addr[0] == '\0') {
			ccow_dq_usage();
			return -1;
	}
	/* Convert cluster-nodes list to json */
	node_str_to_json(cluster_nodes);
	return 0;
}

static int
write_pidfile(char *pid_file, pid_t pid)
{
	FILE *file = fopen(pid_file, "w");
	if (!file) {
		fprintf(stderr, "Failed to open pid file! (%s)", pid_file);
		return 1;
	}

	fprintf(file, "%d", pid);
	fclose(file);

	return 0;
}

static void
ccow_dq_check_cluster_info()
{
	int err;

	log_info(lg, "Obtaining cluster information.\n");
	err = cdq_get_cluster_nodes(cdq_server.id, cdq_server.addr);
	if (err) {
		printf("Failed to get cluster server list\n");
	}

	log_info(lg, "Checking if this node (%lu) is a leader.\n",
			cdq_server.id);
	err = cdq_is_leader(cdq_server.id, cdq_server.addr);
	if (err)
		printf("It's a leader\n");
	else
		printf("NO LEADER!!\n");
}

int
main(int argc, char *argv[])
{
	int err;
	char *ccowlog = getenv("CCOW_LOG_STDOUT");
	char logname[PATH_MAX];

	daemonize = (ccowlog && *ccowlog == '1') ? 0 : 1;

	if (argc < 7 && argc > 10) {
		ccow_dq_usage();
		return -1;
	}

	err = ccow_dq_parse_options(argc, argv);
	if (err != 0) {
		return -1;
	}

	char *nedge_home = getenv("NEDGE_HOME");
	if (nedge_home)
		snprintf(pidfile, PATH_MAX, "%s/var/run/dqlite.%ld.pid",
		    nedge_home, cdq_server.id);
	else
		snprintf(pidfile, PATH_MAX, "%s/var/run/dqlite.%ld.pid",
		    QUOTE(INSTALL_PREFIX), cdq_server.id);

	struct stat st;
	if (stat(pidfile, &st) == 0) {
		FILE *fp = fopen(pidfile, "r");
		if (fp == NULL) {
			fprintf(stderr, "Daemon already running!\n");
			return 1;
		} else {
			int pid;
			int nread;
			char buf[PATH_MAX];

			nread = fscanf(fp, "%d", &pid);
			fclose(fp);
			sprintf(buf, "/proc/%d", pid);
			if (nread == 1 && stat(buf, &st) == 0) {
				fprintf(stderr, "Daemon already running!\n");
				return 1;
			}
		}
	}

	if (daemonize && daemon(1, 1)) {
		fprintf(stderr, "Failed to daemonize\n");
		return 1;
	}

	sprintf(logname, "ccow-sql-%"PRIu64, cdq_server.id);
	lg = Logger_create(logname);

	int write_pidfile_res = write_pidfile(pidfile, getpid());
	if (write_pidfile_res) {
		fprintf(stderr, "Failed to write pidfile\n");
		return 1;
	}

	setpriority(PRIO_PROCESS, getpid(), -15);
	signal(SIGTERM, signal_handler);
	signal(SIGINT, signal_handler);

	log_info(lg, "Starting DQLite ...\n");
	err = ccow_dq_start();
	if (err) {
		log_error(lg, "Unable to start ccow-sql: error - %d\n", err);
		goto err_;
	}
	log_info(lg, "DQLite node started.\n");

	if (err == 0) {
		int retry = 0;
		do {
			/*
			 * Retries are useful when few servers are started
			 * in parallel. Wait before retry.
			 */
			err = ccow_dq_join_cluster();
			if (err != 0) {
				usleep(JOIN_TIME);
				retry++;
				log_error(lg, "Retrying (%d) cluster "
						"join ...\n", retry);
			}
		} while (err != 0 && retry < JOIN_RETRY);

		/* Keep node running even if it fails to join the cluster */
		if (verbose == 1) {
			ccow_dq_check_cluster_info();
		}
		while(1)
			usleep(SLEEP_TIME);
	}

err_:
	if (daemonize)
		unlink(pidfile);
	return err;
}
