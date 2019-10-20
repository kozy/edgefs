#
# Use is subject of licensing terms
# Nexenta Systems, Inc.
#

SRCDIR=$(NEDGE_HOME)
NAME=libco

.configure:
	cd ../.. && flock -e .gitmodules git submodule update --depth 1 --recursive --init deps/libco/$(NAME)
	touch $@

.build: .configure
	PREFIX=$(SRCDIR) make -C $(NAME)
	touch $@

install: .build
	PREFIX=$(SRCDIR) make -C $(NAME) install

all: install

uninstall: .build

clean:
	rm -rf $(NAME); mkdir $(NAME)
	rm -f .configure .build
	# remove symlinks
	rm -f $(SRCDIR)/lib/libco.so.0 $(SRCDIR)/lib/libco.so
