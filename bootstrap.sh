#!/bin/sh
pkg_add -r python26
pkg_add -r ezjail

# XXX Fetch the correct tarball depending
fetch ftp://ftp.freebsd.org/pub/FreeBSD/ports/amd64/packages-8.2-release/Latest/python26.tbz

/usr/local/bin/python zfs-perf-test.py
