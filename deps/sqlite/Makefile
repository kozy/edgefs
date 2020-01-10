#
# Use is subject of licensing terms
# Nexenta Systems, Inc.
#

SRCDIR=$(NEDGE_HOME)
NAME=sqlite

.configure:
	cd ../.. && flock -e .gitmodules git submodule update --depth 1 --recursive --init deps/sqlite/$(NAME)
	touch $@

.build: .configure
	cd $(NAME) && ./configure --prefix=$(SRCDIR) --enable-replication
	make -C $(NAME)
	touch $@

install: .build
	make -C $(NAME) install

all: install

uninstall: .build
	make -C $(NAME) uninstall

clean:
	rm -rf $(NAME); mkdir $(NAME)
	rm -f .configure .build
