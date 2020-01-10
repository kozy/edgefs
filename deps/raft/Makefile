#
# Use is subject of licensing terms
# Nexenta Systems, Inc.
#

SRCDIR=$(NEDGE_HOME)
NAME=raft

ifdef NEDGE_NDEBUG
DEBUG_FLAGS=-DUSE_JE_MALLOC
DEBUG_LDFLAGS=-ljemalloc -fno-omit-frame-pointer
else
DEBUG_FLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common

ifeq (${CC},clang)
DEBUG_LDFLAGS=-fsanitize=address
else
DEBUG_LDFLAGS=-lasan
endif
 
endif

.configure:
	cd ../.. && flock -e .gitmodules git submodule update --depth 1 --recursive --init deps/raft/$(NAME)
	touch $@

.build: .configure
	cd $(NAME) && autoreconf -i
	cd $(NAME) && UV_CFLAGS="$(DEBUG_FLAGS) -I$(SRCDIR)/include" UV_LIBS="$(DEBUG_LDFLAGS) -L$(SRCDIR)/lib -luv" \
		./configure --prefix=$(SRCDIR) --enable-ccowfsio
	touch $@

install: .build
	make -C $(NAME) install

all: install

uninstall: .build
	make -C $(NAME) uninstall

clean:
	rm -rf $(NAME); mkdir $(NAME)
	rm -f .configure .build
