#
# Use is subject of licensing terms
# Nexenta Systems, Inc.
#

SRCDIR=$(NEDGE_HOME)

MAKEFLAGS += -j

CORO_DEPS=jemalloc libqb knet corosync
DEPS=lmdb libuv nanomsg liberasurecode libbacktrace kvssd \
     json-c h2o smartmontools libco
CCOW_DEPS=stgt fio nfs-ganesha samba
DQLITE_DEPS=raft sqlite dqlite

all: install

coro-deps:
	# must be serial due to dependency
	for p in $(CORO_DEPS); do \
		make -C $$p || exit 1; \
	done

$(DEPS):: coro-deps
	$(MAKE) -C $@ install

install: $(DEPS)

$(CCOW_DEPS)::
	$(MAKE) -C $@ install

dqlite-deps:
	# must be serial due to dependency
	for p in $(DQLITE_DEPS); do \
		make -C $$p || exit 1; \
	done

ccow-deps: $(CCOW_DEPS) dqlite-deps

uninstall:
	for p in $(CORO_DEPS) $(DEPS) $(CCOW_DEPS); do make -C $$p uninstall || exit 1; done

clean:
	for p in $(CORO_DEPS) $(DEPS) $(CCOW_DEPS); do make -C $$p clean || exit 1; done
