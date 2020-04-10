############################################################
# Dockerfile to build EdgeFS container image
# Based on Ubuntu
############################################################

# Set the base image to Ubuntu to produce amd64 binary
FROM ubuntu:18.04 as builder

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update &&\
    apt-get install -y software-properties-common &&\
    add-apt-repository ppa:git-core/ppa &&\
    apt-get update &&\
    apt-get install -y -qq --no-install-recommends git make

RUN mkdir -p /opt/nedge

# copy project files to container
COPY . /opt/edgefs

WORKDIR /opt/edgefs

RUN /bin/bash -c "cd /opt/edgefs ; ls -la ; export NEDGE_HOME=/opt/nedge ; source /opt/edgefs/env.sh ; make clean ; make NEDGE_NDEBUG=1 NEDGE_VERSION=${NEDGE_VERSION} world"

RUN rm -f /opt/nedge/lib/*.a /opt/nedge/lib/*.la
RUN cp -ar /opt/nedge/etc /opt/nedge/etc.default


FROM ubuntu:18.04
MAINTAINER EdgeFS
LABEL description="EdgeFS Multi-Cloud Distributed Storage System"

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y && \
    apt-get install libssl1.0.0 iputils-ping iproute2 libnss3 libsnmp30 udev \
        bsdmainutils libcgroup1 libcurl4 nvi curl gdisk bcache-tools parted \
	openssl netbase rpcbind gdb libaio1 libnuma1 nvme-cli krb5-user \
        -y --no-install-recommends && \
    \
    apt-get purge -y --auto-remove && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    find /usr/share/doc -depth -type f ! -name copyright|xargs rm -f && \
    rm -rf /usr/share/man /usr/share/groff /usr/share/info /usr/share/lintian /usr/share/linda /var/cache/man && \
    \
    mkdir /data

COPY --from=builder /opt/nedge /opt/nedge/
COPY --from=builder /opt/edgefs/scripts/toolbox /usr/bin/
WORKDIR /opt/nedge
ENTRYPOINT ["/opt/nedge/sbin/edgefs-start.sh"]
