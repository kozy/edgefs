#
# Use is subject of licensing terms
# Nexenta Systems, Inc.
#

SRCDIR=$(NEDGE_HOME)
NAME=samba

ifdef NEDGE_NDEBUG
DEBUG_FLAGS=-DUSE_JE_MALLOC
DEBUG_LDFLAGS=-fno-omit-frame-pointer
GAN_ALLOCATOR=jemalloc
else
DEBUG_FLAGS=-fsanitize=address -fno-omit-frame-pointer -fno-common
DEBUG_LDFLAGS=-lasan
GAN_ALLOCATOR=libc
endif

all: install

.configure:
	cd ../.. && flock -e .gitmodules git submodule update --depth 1 --recursive --init deps/samba/$(NAME)
	cd $(NAME) && ./configure --disable-python --without-ad-dc --disable-cups \
		--without-json --without-libarchive --without-acl-support \
		--without-ldap --without-ads --without-pam --prefix=$(NEDGE_HOME)
	touch $@

.build: .configure
	make -C $(NAME) -j
	touch $@

install: .build

clean:
	rm -rf .configure .build
	rm -rf $(NAME); mkdir $(NAME)
