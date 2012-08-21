#!/bin/sh
pkg_add -r python26
pkg_add -r ezjail

/usr/local/bin/python zfs-perf-test.py
