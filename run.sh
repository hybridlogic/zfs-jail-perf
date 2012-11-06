#!/bin/sh
for X in RenameFilesystemLoad PruneSnapshots LotsOfTinySnapshots SnapshotUsedFilesystemLoad ReplayLargeLoad; do python zfs-perf-test.py --jail $X; done
