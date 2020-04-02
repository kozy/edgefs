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
	cd $(NAME) && ./configure --without-ad-dc --disable-cups --disable-iprint \
		--without-systemd --without-json --without-libarchive --without-acl-support \
		--with-system-mitkrb5 --with-ads --with-ldap --without-pam --bundled-libraries=ALL \
		--with-static-modules=ALL --nonshared-binary=smbd/smbd --enable-fhs \
		--sysconfdir=$(SRCDIR)/etc \
		--localstatedir=$(SRCDIR)/var \
		--with-logfilebase=$(SRCDIR)/var/log \
		--with-lockdir=$(SRCDIR)/var/lock \
		--with-piddir=$(SRCDIR)/var/run \
		--with-privatedir=$(SRCDIR)/etc/samba \
		--with-configdir=$(SRCDIR)/etc/samba \
		--with-cachedir=$(SRCDIR)/var/cache  \
		--with-pammodulesdir=$(SRCDIR)/lib/security \
		--enable-edgefs \
		--prefix=$(SRCDIR)
	touch $@

.build: .configure
	make -C $(NAME) -j
	touch $@

install: .build
	mkdir -p $(SRCDIR)/etc/samba
	cp -a $(NAME)/bin/default/source3/smbd/smbd $(SRCDIR)/sbin/

clean:
	rm -rf .configure .build
	rm -rf $(NAME); mkdir $(NAME)
