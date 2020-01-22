#ifndef __DQCLIENT_H__
#define __DQCLIENT_H__

#include "client.h"

/* Wrapper around a real dq client */
struct cdq_client
{
	struct	client cl;	/* dqclient */
	int	net_fd;		/* fd for connection to server */
	char	*srv_ipaddr;	/* server IPv4Addr:port */
	int	srv_id;		/* ID of the server */
};

/* Function declarations */
int cdq_start(struct cdq_client *client);
void cdq_stop(struct cdq_client *client);
int cdq_db_open(struct cdq_client *client, const char *dbname);
int cdq_prepare_stmt(struct cdq_client *client, char *stmt, unsigned *stmt_id);
int cdq_exec_stmt(struct cdq_client *client, unsigned stmt_id,
		unsigned *last_insert_id, unsigned *rows_affected);
int cdq_query_stmt(struct cdq_client *client, unsigned stmt_id,
		struct rows *rows);

#endif /* __DQCLIENT_H__ */
