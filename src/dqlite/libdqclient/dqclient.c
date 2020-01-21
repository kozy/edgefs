#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>

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
