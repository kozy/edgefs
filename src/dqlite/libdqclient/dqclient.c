#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "json.h"
#include "dqclient.h"

/*
 * IP Address format: IPv4Address:port
 */
int
cdq_connect_server(char *ipaddr)
{
	struct sockaddr_in inaddr;
	int fd, err = 0;
	char *ipv4addr, *port;
	in_port_t port_num;

	if (!ipaddr) {
		errno = EINVAL;
		return -1;
	}

	ipv4addr = ipaddr;
	port = strchr(ipaddr, ':');
	if (port) {
		*port = '\0';
		port++;
	} else {
		errno = EINVAL;
		return -1;
	}

	err = sscanf(port, "%hu", &port_num);
	if (err <= 0) {
		errno = EINVAL;
		return -1;
	}

	memset(&inaddr, 0, sizeof inaddr);
	inaddr.sin_family = AF_INET;
	inaddr.sin_addr.s_addr = inet_addr(ipv4addr);
	inaddr.sin_port = htons(port_num);

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		return fd;
	}

	err = connect(fd, &inaddr, sizeof inaddr);
	if (err < 0) {
		close(fd);
		return err;
	}
	return fd;
}

void
cdq_stop(struct cdq_client *client)
{
	clientClose(&client->cl);
	close(client->net_fd);
}

int
cdq_start(struct cdq_client *client)
{
	int err;

	/* Connect to the server */
	client->net_fd = cdq_connect_server(client->srv_ipaddr);
	if (client->net_fd < 0) {
		/* Log the error */
		fprintf(stderr, "Failed to connect to server: %s error: %s\n",
				client->srv_ipaddr, strerror(errno));
		return -1;
	}

	/* Initialize client */
	err = clientInit(&client->cl, client->net_fd);
	if (err) {
		close(client->net_fd);
		return -1;
	}

	/* Perform handshake */
	err = clientSendHandshake(&client->cl);
	if (err) {
		/* Log the error */
		fprintf(stderr, "Failed to send handshake: %s error: %s\n",
				client->srv_ipaddr, strerror(errno));
		cdq_stop(client);
		return -1;
	}

	return err;
}

int
cdq_db_open(struct cdq_client *client, const char *dbname)
{
	int err;

	err = clientSendOpen(&client->cl, dbname);
	if (err) {
		/* Log the error */
		fprintf(stderr, "Failed to open the database: %s error: %s\n",
				dbname, strerror(errno));
		cdq_stop(client);
		return err;
	}
	err = clientRecvDb(&client->cl);
	return err;
}

int
cdq_db_close(struct cdq_client *client)
{
	/* Probably nothing to do */
	return 0;
}

int
cdq_prepare_stmt(struct cdq_client *client, char *stmt, unsigned *stmt_id)
{
	int err;

	err = clientSendPrepare(&client->cl, stmt);
	if (err) {
		return err;
	}
	err = clientRecvStmt(&client->cl, stmt_id);
	return err;
}

int
cdq_exec_stmt(struct cdq_client *client, unsigned stmt_id,
		unsigned *last_insert_id, unsigned *rows_affected)
{
	int err;

	err = clientSendExec(&client->cl, stmt_id);
	if (err) {
		return err;
	}
	err = clientRecvResult(&client->cl, last_insert_id, rows_affected);
	return err;
}

void
cdq_rows_close(struct rows *rows)
{
	clientCloseRows(rows);
}

int
cdq_query_stmt(struct cdq_client *client, unsigned stmt_id, struct rows *rows)
{
	int err;

	err = clientSendQuery(&client->cl, stmt_id);
	if (err) {
		return err;
	}
	err = clientRecvRows(&client->cl, rows);
	return err;
}

int
cdq_join_node(struct cdq_client *client, uint64_t node_id, char *node_ip)
{
	int err;

	err = clientSendJoin(&client->cl, node_id, node_ip);
	if (err) {
		return err;
	}
	err = clientRecvEmpty(&client->cl);
	if (err) {
		return err;
	}
	err = clientSendPromote(&client->cl, node_id);
	if (err) {
		return err;
	}
	err = clientRecvEmpty(&client->cl);
	return err;
}

int
cdq_leave_node(struct cdq_client *client, uint64_t node_id)
{
	int err;

	err = clientSendRemove(&client->cl, node_id);
	if (err) {
		return err;
	}
	err = clientRecvEmpty(&client->cl);
	return err;
}

int
cdq_node_leave_cluster(uint64_t leader_id, char *leader_ip,
		uint64_t candidate_id)
{
	int err;
	struct cdq_client client;

	if (leader_ip == NULL)
		return -EINVAL;

	memset(&client, 0, sizeof client);
	client.srv_id =  leader_id;
	strncpy(client.srv_ipaddr, leader_ip, INET_ADDRSTRLEN + 6);

	err = cdq_start(&client);
	if (err != 0) {
		return ECONNREFUSED;
	}
	err = cdq_leave_node(&client, candidate_id);
	if (err != 0) {
		return EPROTO;
	}
	cdq_stop(&client);
	return 0;
}

int
cdq_node_join_cluster(uint64_t leader_id, char *leader_ip,
			uint64_t candidate_id, char *candidate_ip)
{
	int err;
	struct cdq_client client;

	if (leader_ip == NULL)
		return EINVAL;

	/* Connect to a leader */
	memset(&client, 0, sizeof client);
	client.srv_id =  leader_id;
	strncpy(client.srv_ipaddr, leader_ip, INET_ADDRSTRLEN + 6);

	err = cdq_start(&client);
	if (err != 0) {
		cdq_stop(&client);
		return ECONNREFUSED;
	}

	/* Send candidate node information to the leader */
	err = cdq_join_node(&client, candidate_id, candidate_ip);
	if (err != 0) {
		cdq_stop(&client);
		return EPROTO;
	}
	cdq_stop(&client);
	return 0;
}

int
cdq_get_cluster_nodes(uint64_t id, char *addr)
{
	int err;
	struct servers servers;
	struct cdq_client client;

	memset(&client, 0, sizeof client);
	client.srv_id =  id;
	strncpy(client.srv_ipaddr, addr, INET_ADDRSTRLEN + 6);

	err = cdq_start(&client);
	if (err != 0) {
		cdq_stop(&client);
		return ECONNREFUSED;
	}

	err = clientSendCluster(&client.cl);
	if (err) {
		cdq_stop(&client);
		return err;
	}
	err = clientRecvServers(&client.cl, &servers);
	if (err == 0) {
		printf("Number of servers: %lu\n", servers.servers_nr);
		for (uint64_t i = 0; i < servers.servers_nr; i++) {
			printf("%lu. Server Info\n", i + 1);
			printf("id - %d\n", servers.nodes[i].id);
			printf("IP Addr - %s\n", servers.nodes[i].addr);
		}
	}
	if (servers.servers_nr)
		clientCloseServers(&servers);
	cdq_stop(&client);
	return err;
}

int
cdq_is_leader(uint64_t id, char *addr)
{
	int err;
	struct server server;
	struct cdq_client client;

	memset(&client, 0, sizeof client);
	client.srv_id =  id;
	strncpy(client.srv_ipaddr, addr, INET_ADDRSTRLEN + 6);

	err = cdq_start(&client);
	if (err != 0) {
		cdq_stop(&client);
		return 0;
	}

	err = clientSendLeader(&client.cl);
	if (err) {
		cdq_stop(&client);
		return 0;
	}
	err = clientRecvServer(&client.cl, &server);
	if (err == 0) {
		/* Connected to the leader */
		if (server.addr[0] != '\0') {
			cdq_stop(&client);
			return 1;
		}
	}
	cdq_stop(&client);
	return 0;
}

/*
 * All node information passed a json string
 */
int
cdq_get_leader(char *jsonstr, uint64_t *leader_id, char **ipaddr)
{
	json_value *o, *nodes;
	char *srv_ip = NULL;
	int got_id = 0, got_ip = 0, srv_id = 0, got_leader = 0;

	if (leader_id == NULL || ipaddr == NULL) {
		return -EFAULT;
	}

	o = json_parse(jsonstr, strlen(jsonstr));
	if (o == NULL || o->type != json_object) {
		return -EINVAL;
	}

	if (strcmp(o->u.object.values[0].name, "nodes") != 0) {
		return -EINVAL;
	}

	nodes = o->u.object.values[0].value;
	if (nodes == NULL) {
		return -EINVAL;
	}
	if (nodes->type != json_array) {
		return -EINVAL;
	}

	for (size_t j = 0; j < nodes->u.array.length; j++) {
		json_value *n = nodes->u.array.values[j];
		for (size_t i = 0; i < n->u.object.length; i++) {
			char *key = n->u.object.values[i].name;
			json_value *v = n->u.object.values[i].value;

			if (strcmp(key, "id") == 0) {
				if (v->type != json_integer) {
					continue;
				}
				got_id = 1;
				srv_id = v->u.integer;
			}
			if (strcmp(key, "ipaddr_port") == 0) {
				if (v->type != json_string) {
					continue;
				}
				got_ip = 1;
				srv_ip = v->u.string.ptr;
			}
			if (got_id && got_ip) {
				if((cdq_is_leader(srv_id, srv_ip))) {
					got_leader = 1;
					*leader_id = srv_id;
					*ipaddr = strdup(srv_ip);
					break; /* stop search */
				}
			}
		}
		/* Found leader stop search */
		if (*leader_id > 0)
			break;
		got_id = got_ip = 0;
	}
	json_value_free(o);

	return got_leader ? 0 : -ENOENT;
}
