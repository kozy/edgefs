#
# Use is subject of licensing terms
# Nexenta Systems, Inc.
#

SRCDIR=$(NEDGE_HOME)
NAME=dqlite

.configure:
	cd ../.. && flock -e .gitmodules git submodule update --depth 1 --recursive --init deps/dqlite/$(NAME)
	touch $@

.build: .configure
	cd $(NAME) && autoreconf -i
	cd $(NAME) && \
		SQLITE_CFLAGS="-I$(SRCDIR)/include" SQLITE_LIBS="-L$(SRCDIR)/lib -lsqlite3" \
		UV_CFLAGS="-I$(SRCDIR)/include" UV_LIBS="-L$(SRCDIR)/lib -luv" \
		RAFT_CFLAGS="-I$(SRCDIR)/include" RAFT_LIBS="-L$(SRCDIR)/lib -lraft" \
		CO_CFLAGS="-I$(SRCDIR)/include" CO_LIBS="-L$(SRCDIR)/lib -lco" \
			./configure --prefix=$(SRCDIR)
	make -C $(NAME) -j
	touch $@

install: .build
	make -C $(NAME) install
	mkdir -p $(SRCDIR)/include/dqlite/lib
	cp $(NAME)/src/client.h $(SRCDIR)/include/dqlite/
	cp $(NAME)/src/tuple.h $(SRCDIR)/include/dqlite/
	cp $(NAME)/src/protocol.h $(SRCDIR)/include/dqlite/
	cp $(NAME)/src/lib/buffer.h $(SRCDIR)/include/dqlite/lib/
	cp $(NAME)/src/lib/serialize.h $(SRCDIR)/include/dqlite/lib/
	cp $(NAME)/src/lib/byte.h $(SRCDIR)/include/dqlite/lib/

all: install

uninstall: .build
	make -C $(NAME) uninstall

clean:
	rm -rf $(NAME); mkdir $(NAME)
	rm -f .configure .build
