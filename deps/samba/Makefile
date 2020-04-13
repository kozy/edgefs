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
		--with-static-modules=ALL --nonshared-binary=!smbstatus,ALL --enable-fhs \
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
	make -C $(NAME) -j smbd/smbd winbindd client/smbclient net smbpasswd ntlm_auth nss_winbind
	touch $@

install: .build
	strip $(NAME)/bin/default/nsswitch/libnss_winbind.so
	cp -a $(NAME)/bin/default/nsswitch/libnss_winbind.so* $(SRCDIR)/lib/
	strip $(NAME)/bin/default/nsswitch/libwinbind-client-samba4.so
	cp -a $(NAME)/bin/default/nsswitch/libwinbind-client-samba4.so $(SRCDIR)/lib/
	strip $(NAME)/bin/default/lib/replace/libreplace-samba4.so
	cp -a $(NAME)/bin/default/lib/replace/libreplace-samba4.so $(SRCDIR)/lib/
	strip $(NAME)/bin/default/source3/utils/smbpasswd
	cp -a $(NAME)/bin/default/source3/utils/smbpasswd $(SRCDIR)/sbin/
	strip $(NAME)/bin/default/source3/winbindd/winbindd
	cp -a $(NAME)/bin/default/source3/winbindd/winbindd $(SRCDIR)/sbin/
	strip $(NAME)/bin/default/source3/utils/net
	cp -a $(NAME)/bin/default/source3/utils/net $(SRCDIR)/sbin/
	cp -a $(NAME)/bin/default/source3/smbd/smbd $(SRCDIR)/sbin/
	cp -a ads-join.sh $(SRCDIR)/sbin/
	mkdir -p $(DESTDIR)$(SRCDIR)/etc/samba/exports
	mkdir -p $(DESTDIR)$(SRCDIR)/var/run/samba/ncalrpc
	touch $(DESTDIR)$(SRCDIR)/etc/samba/exportslist
	cp smb.conf $(DESTDIR)$(SRCDIR)/etc/samba/smb.conf
	$(shell echo /bin/echo -ne \"root\\nroot\\n\" \| ${NAME}/bin/default/source3/utils/smbpasswd -c ${DESTDIR}${SRCDIR}/etc/samba/smb.conf -s -a root)

clean:
	rm -rf .configure .build
	rm -rf $(NAME); mkdir $(NAME)
