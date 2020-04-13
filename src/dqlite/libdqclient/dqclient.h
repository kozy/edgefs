#ifndef __DQCLIENT_H__
#define __DQCLIENT_H__

#include "dqlite/client.h"

#define ADDRESS_MAX_LEN 32
#define CONNECTION_TIMEOUT 600

/* Wrapper around a real dq client */
struct cdq_client
{
	struct	client cl;	/* dqclient */
	int	net_fd;		/* fd for connection to server */
				/* server IPv4Addr:port */
	char	srv_ipaddr[ADDRESS_MAX_LEN];
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
int cdq_query_stmt_more(struct cdq_client *client, struct rows *rows);
void cdq_rows_close(struct rows *rows);
int cdq_get_cluster_nodes(uint64_t id, char *addr);
int cdq_is_leader(uint64_t id, char *addr);
int cdq_get_leader(char *jsonstr, uint64_t *leader_id, char **ipaddr);
int cdq_node_join_cluster(uint64_t leader_id, char *leader_ip,
			uint64_t candidate_id, char *candidate_ip);
int cdq_node_leave_cluster(uint64_t leader_id, char *leader_ip, uint64_t candidate_id);

#endif /* __DQCLIENT_H__ */
