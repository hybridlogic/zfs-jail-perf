#!/bin/sh
pkg_add -r python26
pkg_add -r ezjail

if [ "`uname -r`" == "8.2-RELEASE" ]; then
	fetch ftp://ftp.freebsd.org/pub/FreeBSD/ports/amd64/packages-8.2-release/Latest/python26.tbz
elif [ "`uname -r`" == "8.3-RELEASE" ]; then
	fetch ftp://ftp.freebsd.org/pub/FreeBSD/ports/amd64/packages-8.3-release/Latest/python26.tbz
else
	fetch ftp://ftp.freebsd.org/pub/FreeBSD/ports/amd64/packages-9.0-release/Latest/python26.tbz
fi

/usr/local/bin/python zfs-perf-test.py
