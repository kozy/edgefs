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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "ccowutil.h"
#include "ccowfsio.h"
#include "logger.h"
#include "dqlite.h"

struct ccow_dqlite_server {
	uint64_t	id;
	char		addr[64];
	char		dir[PATH_MAX];
	dqlite_node	*node;
};

static struct ccow_dqlite_server cdq_server;
static int daemonize = 0;
static char pidfile[PATH_MAX];

Logger lg;

static int
ccow_dq_stop()
{
	int err;

	err = dqlite_node_stop(cdq_server.node);
	if (err) {
		log_error(lg, "Failed to stop dqlite node, err: %d\n", err);
		return err;
	}

	ccow_fsio_term();
	dqlite_node_destroy(cdq_server.node);
	log_notice(lg, "Stopped dqlite node addr: %s, dir: %s\n",
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
	err = ccow_dq_stop();
	if (daemonize)
		unlink(pidfile);
	if (err)
		fprintf(stderr, "Failed to stop ccow-sql\n");

	log_flush(lg);
	signal(signum, SIG_DFL);
	raise(signum);
}

int
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
	}

out_:
	return err;
}

static void
ccow_dq_usage()
{
	printf("\n"
		"USAGE:\n"
		"       ccow_sql [-h] <[-i | --id] id>\n"
		"		<[-a | --addr] xx.xx.xx.xx:1234>\n"
	        "		<[-d | --dir] object-path>\n"
		"\n"
		"       -h Display this help message and exit.\n"
		"\n"
		"       -i | --id id server id number"
		"\n"
		"       -a | --adddr xx.xx.xx.xx:1234 IPv4 address and port number"
		"\n"
		"       -d | --dir object-path Object that stores the database"
		"\n");
}

static int
ccow_dq_parse_options(int argc, char *argv[])
{
	int option_index = 0, err = 0, c;

	static struct option long_options[] = {
		{"help",        0,              0,  'h' },
		{"id", required_argument, 0,  0 },
		{"addr", required_argument, 0,  1 },
		{"dir", required_argument, 0,  2 },
		{0,         0,                   0,  0 }
	};

	while (1) {
		c = getopt_long(argc, argv, "hi:a:d:",
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
				/*
				 * We assume program calling this program has
				 * validated address and port
				 */
				strncpy(cdq_server.addr, optarg, 63);
				break;
			case 'd':
				/*
				 * We assume program calling this program has
				 * validated cluster, tenant and bucket in
				 * this path
				 */
				strncpy(cdq_server.dir, optarg, PATH_MAX - 1);
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

int
main(int argc, char *argv[])
{
	int err;
	char *ccowlog = getenv("CCOW_LOG_STDOUT");
	char logname[PATH_MAX];

	daemonize = (ccowlog && *ccowlog == '1') ? 0 : 1;

	if (argc != 7) {
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

	printf("Starting DQLite ...\n");
	err = ccow_dq_start();
	if (err)
		fprintf(stderr, "Unable to start ccow-sql error: %d\n", err);
	else {
		while(1)
			usleep(100000);
	}

	if (daemonize)
		unlink(pidfile);
	return err;
}
