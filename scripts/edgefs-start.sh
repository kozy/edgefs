#!/bin/bash

NEDGE_HOME=${NEDGE_HOME:-/opt/nedge}; export NEDGE_HOME

cd $NEDGE_HOME
source env.sh

UTIL="docker run -it edgefs/edgefs:$NEDGE_VERSION"
COROSYNC_CONF=$NEDGE_HOME/etc/corosync/corosync.conf
AUDITD_INI=$NEDGE_HOME/etc/ccow/auditd.ini
RUNNING_PODS_JSON=$NEDGE_HOME/etc/ccow/running-pods.json

cleanup() {
	echo "Stopping, performing cleanup..."
	if ! test "x$DAEMON_PID" = x; then
		kill -TERM $DAEMON_PID
		sleep 5
	fi
	edgefs-stop.sh
}
trap 'cleanup' SIGTERM

stats_cleanup() {
	rm -f $NEDGE_HOME/var/run/stats.db > /dev/null 2>&1
}

sysctl_apply() {
	local profile=$1
	SAVEIFS=$IFS
	IFS=$(echo -en "\n\b")
	for LINE in $(grep -v '^#' ${NEDGE_HOME}/etc/sysctl${profile}.conf | grep . ); do
		unset RESULT
		MY_VAR=${LINE%% *}
		RESULT=$(grep ${MY_VAR} /etc/sysctl.conf)
		if [ "${RESULT}" ]; then
			# exists in new sysctl.conf, skipping
			continue
		else
			echo "Adding ${MY_VAR} to new sysctl.conf"
			echo "${LINE}" >> /etc/sysctl.conf
		fi

	done
	IFS=$SAVEIFS
	sysctl --load $NEDGE_HOME/etc/sysctl${profile}.conf &>/dev/null
}

coro_wait() {
	sleep 1
	while ! test -e $NEDGE_HOME/var/run/corosync/cmap || ! corosync-cpgtool &>/dev/null; do
		echo "Waiting for corosync ..."
		sleep 5
	done
}

debug_wait() {
	while test -e $NEDGE_HOME/var/run/.edgefs-start-block-$1; do
		sleep 1
	done
}

start_msg() {
	rm -f $NEDGE_HOME/var/run/$1.pid
	if test "x$CCOW_LOG_STDOUT" = x -o "x$2" = x1; then
		echo "Starting $1 in background."
	else
		echo "Starting $1 in foreground."
	fi
}

check_container() {
	local IN_DOCKER="no"

	if test "x$1" = xtoolbox; then
		return
	fi

	# running in docker?
	if grep docker /proc/1/cgroup &>/dev/null; then
		IN_DOCKER="yes"
	fi

	if test "x$IN_DOCKER" = xyes -o "x$HOST_HOSTNAME" != x; then
		echo "Running in container"
		export CCOW_LOG_STDOUT=1

		# case when /opt/nedge/etc is bind-mounted, populate etc
		is_target_initial_population=0
		if ! test -e $NEDGE_HOME/etc/ccow; then
			echo "Populating $NEDGE_HOME/etc from defaults"
			cp -ar $NEDGE_HOME/etc.default/* $NEDGE_HOME/etc/
			is_target_initial_population=1
		fi

		# case when /opt/nedge/var is bind-mounted: populate etc
		if ! test -e $NEDGE_HOME/var/lock; then
			echo "Precreating $NEDGE_HOME/var directory structure"
			mkdir -p $NEDGE_HOME/var/lock/subsys
			mkdir -p $NEDGE_HOME/var/lib/nfs/sm
			mkdir -p $NEDGE_HOME/var/lib/auditd-sinks
		fi

		# case when /opt/nedge/etc.target is bind-mounted, populate etc
		# this is expected when container runs as initiator on the same
		# node as one of the targets
		if test -e $NEDGE_HOME/etc.target; then

			is_master=0
			if test "x$DAEMON_INDEX" = x -o "x$DAEMON_INDEX" = x0; then
				is_master=1
			fi

			if test "x$1" = xmgmt -o $is_master = 0; then
				while ! test -e $NEDGE_HOME/etc.target/ccow/ccow.json; do
					sleep 5
					echo "Waiting for target to initialize ccow.json file ..."
				done
			fi

			if test $is_target_initial_population = 1 -o "x$DAEMON_INDEX" = x; then
				echo "Populating $NEDGE_HOME/etc from $NEDGE_HOME/etc.target"
				cp -a $NEDGE_HOME/etc.target/ccow/* $NEDGE_HOME/etc/ccow/
				cp -a $NEDGE_HOME/etc.target/corosync/* $NEDGE_HOME/etc/corosync/
			fi

			if test "x$DAEMON_INDEX" = x; then
				efscli config broker || exit 1
				echo "Broker configuration adjusted."
			else
				efscli config server || exit 1
				echo "Server slave configuration adjusted."
			fi

		elif ! test "x$1" = xconfig; then
			# only if system is configured, run configurer to verify/adjust network
			if test -e $NEDGE_HOME/etc/ccow/ccow.json; then
				efscli config server || exit 1
				echo "Server configuration adjusted."
			fi
		fi
	fi

	# k8s daemonset?
	if test "x$KUBERNETES_DAEMONSET" != x -a "x$KUBERNETES_SERVICE_HOST" != x; then
		sleep 2
		k8s_list_pods > $RUNNING_PODS_JSON
		while ! grep "podIP" $RUNNING_PODS_JSON &>/dev/null; do
			sleep 2
			k8s_list_pods > $RUNNING_PODS_JSON
		done
		while grep "ready.*false" $RUNNING_PODS_JSON &>/dev/null; do
			sleep 2
			k8s_list_pods > $RUNNING_PODS_JSON
		done
		echo "Discovered Pod IPs:"
		grep podIP $RUNNING_PODS_JSON
	fi
}

idle() {
	while true; do
        	sleep 1
        done
}

is_gateway() {
	grep name $NEDGE_HOME/etc/ccow/rt-lfs.json &>/dev/null && return 1
	grep name $NEDGE_HOME/etc/ccow/rt-rd.json &>/dev/null && return 1
	return 0
}

is_leader() {
	grep leader.*1 $NEDGE_HOME/var/run/flexhash.json &>/dev/null && return 0
	return 1
}

corosync_pre() {
	mkdir -p $NEDGE_HOME/var/run/corosync
	mkdir -p $NEDGE_HOME/var/log/cluster

	if test -e $NEDGE_HOME/etc/config/nesetup.json; then
		# we are inside a corosync container
		efscli config file $NEDGE_HOME/etc/config/nesetup.json || exit 1
		if test x$K8S_NAMESPACE != x; then
			# We are inside a k8s container
			rm -f $NEDGE_HOME/etc/corosync/corosync.conf
                        # use corosync-pod-watcher for cofigurarion update
			corosync-pod-watcher &
			# the corosync pod watcher will create a config file for us. Waiting...
			while ! test -e $NEDGE_HOME/etc/corosync/corosync.conf; do
				sleep 1
			done
		fi
		echo "Configuration applied."
	fi

	cp $NEDGE_HOME/etc/corosync/corosync.conf $NEDGE_HOME/var/run/corosync/
}

usage() {
	cat << EOF

	Usage: $UTIL COMMAND

	Commands are:
	target	   - start full target mode
	solo	   - start all in one "Solo" mode
	config	   - configure node using "efscli config node" command
	daemon     - start ccow-daemon only
	auditd     - start ccow-auditd only
	corosync   - start corosync only
	toolbox	   - enter toolbox UNIX shell
	prepare    - apply host prepare settings (sysctl, etc)

	Services:
	mgmt	   - start gRPC management service
	isgw	   - start ISGW multi-site synchronization service
	smb        - start SMB service
	nfs        - start NFS service
	s3         - start AWS S3 compatbile service
	s3x        - start S3X service
	iscsi      - start ISCSI service
	sql        - start Embedded SQL service

	Options:

	-h, --help      - Show this help screen
EOF
}

if [ $# -eq 0 ]; then
	usage
	exit 0
fi

if test "x$1" = "xprepare"; then
	if test "x$CCOW_EMBEDDED" = x; then
		sysctl_apply
	else
		sysctl_apply "-embedded"
	fi
	exit 0
fi
check_container $1

case $1 in
	"target")
		shift

		corosync_pre
		$NEDGE_HOME/etc/init.d/corosync start
		coro_wait
		corosync-cfgtool -R
		corosync-quorumtool -i

		$NEDGE_HOME/etc/init.d/auditctl start

		if test "x$CCOW_EMBEDDED" = x; then
			sysctl_apply
		else
			sysctl_apply "-embedded"
		fi

		start_msg "ccowd"
		debug_wait "ccowd"
		ccow-daemon $1 $2 $3 $4 $5 $6 $7 $8 $9 &

		DAEMON_PID=$!

		# handle SIGTERM in the script
		wait $DAEMON_PID
		;;
	"solo")
		shift

		soloIf=${NEDGE_SOLOIF:-eth0}
		if ! test -e $NEDGE_HOME/etc/ccow/ccow.json; then
			efscli config node -f -i $soloIf
			echo "Default Solo configuration applied ($soloIf)."
		fi

		corosync_pre
		$NEDGE_HOME/etc/init.d/corosync start
		coro_wait
		corosync-cfgtool -R
		corosync-quorumtool -i

		start_msg "grpc-efsproxy" 1
                grpc-efsproxy -d

		$NEDGE_HOME/etc/init.d/auditctl start

		start_msg "ccowd"
		debug_wait "ccowd"
		ccow-daemon $1 $2 $3 $4 $5 $6 $7 $8 $9 &

		DAEMON_PID=$!

		# handle SIGTERM in the script
		wait $DAEMON_PID
		;;
	"config")
		shift
		if test "x$CCOW_EMBEDDED" = x; then
			sysctl_apply
		else
			sysctl_apply "-embedded"
		fi
		debug_wait "config"
		exec efscli config node ${@}
		;;
	"toolbox")
		shift
		cmd=${@}
		if test "x$cmd" = x; then
			echo
			echo "Welcome to EdgeFS Toolbox."
			echo "Hint: type efscli to begin"
			echo
			exec bash
		else
			exec bash -c "$cmd"
		fi
		;;
	"daemon")
		shift
		coro_wait
		if ! test "x$DAEMON_INDEX" = x -o "x$DAEMON_INDEX" = x0; then
			role=`efscli config role`

			if test "x$role" = xgateway; then
				# as we only need one daemon runnig on a gateway node
				# lets terminate secondary gateways here
				echo "Container disabled due to gateway secondary"
				idle
				exit
			fi

                        if is_gateway; then
                                echo "Container disabled due to daemon stub"
                                idle
                                exit
                        fi

			# for data containers, we need to start personal ccow-auditd
			start_msg "auditd"
			debug_wait "auditd"
			ccow-auditd &
		fi
		start_msg "ccowd"
		debug_wait "ccowd"
		exec ccow-daemon $1 $2 $3 $4 $5 $6 $7 $8 $9
		;;
	"auditd")
		shift
		coro_wait
		start_msg "auditd"
		debug_wait "auditd"
		exec ccow-auditd $1 $2 $3 $4 $5 $6 $7 $8 $9
		;;
        "mgmt")
                shift
		coro_wait
		start_msg "grpc-efsproxy"
		debug_wait "grpc-efsproxy"
		exec grpc-efsproxy -f $1 $2 $3 $4 $5 $6 $7 $8 $9
                ;;
        "nfs")
                shift
		start_msg "ganesha"
		debug_wait "ganesha"
		export CCOW_SVCTYPE="nfs"
		exec grpc-nfs -s $CCOW_SVCNAME $1 $2 $3 $4 $5 $6 $7 $8 $9
                ;;
        "smb")
                shift
		start_msg "smbd"
		debug_wait "smbd"
		export CCOW_SVCTYPE="smb"
		exec grpc-smb -s $CCOW_SVCNAME $1 $2 $3 $4 $5 $6 $7 $8 $9
                ;;
        "s3x")
                shift
		start_msg "ccowhttpd"
		debug_wait "ccowhttpd"
		export CCOW_SVCTYPE="s3"
		exec grpc-s3x -s $CCOW_SVCNAME $1 $2 $3 $4 $5 $6 $7 $8 $9
                ;;
        "s3")
                shift
		start_msg "s3gw"
		debug_wait "s3gw"
		export CCOW_SVCTYPE="s3"
		exec grpc-s3 -s $CCOW_SVCNAME $1 $2 $3 $4 $5 $6 $7 $8 $9
                ;;
        "iscsi")
                shift
		start_msg "tgtd-0"
		debug_wait "tgtd-0"
		export CCOW_SVCTYPE="iscsi"
		exec grpc-iscsi -s $CCOW_SVCNAME $1 $2 $3 $4 $5 $6 $7 $8 $9
                ;;
        "sql")
                shift
		start_msg "sql"
		debug_wait "sql"
		export CCOW_SVCTYPE="sql"
		exec grpc-sql -s $CCOW_SVCNAME $1 $2 $3 $4 $5 $6 $7 $8 $9
                ;;
        "isgw")
                shift
		start_msg "isgwd"
		debug_wait "isgwd"
		export CCOW_SVCTYPE="isgw"
		exec grpc-isgw -s $CCOW_SVCNAME $1 $2 $3 $4 $5 $6 $7 $8 $9
                ;;
        "wait")
	        shift
		echo "All edgefs cluster devices are zapped"
		while true
		do
			echo "Waiting for cluster deletion..."
			sleep 5
		done
		;;
	"corosync")
		shift
		corosync_pre
		stats_cleanup
		start_msg "corosync"
		grep ring0_addr $NEDGE_HOME/etc/corosync/corosync.conf
		debug_wait "corosync"
		exec corosync -f $1 $2 $3 $4 $5 $6 $7 $8 $9
		;;
	-h | --help)
        	usage
        	exit 0
        	;;
    	*)
        	echo >&2 "$UTIL: unknown command \"$1\" (use --help for help)"
        	exit 1
        	;;
esac
