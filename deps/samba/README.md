Testing in dev environment.

1. make install to ensure that proper smbd binary copied and etc/samba created at NEDGE_HOME location

2. copy test-bk.conf over to $NEDGE_HOME/etc/samba/smb.conf, modify it to reflect the right cltest/test/bk1...

3. add SMB user root and password

./samba/bin/smbpasswd -c etc/samba/smb.conf -a root

4. start with the command, debug enabled, will auto exit on unmount

smbd -i -F -d 3

Use -d NUMBER for greater debug level

5. on the client install cifs-utils package, and run mount command like below and enter root's user password

mount -t cifs -o vers=3.0 //10.3.30.78/bk1 /tmp/a
