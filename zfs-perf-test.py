
from __future__ import division, unicode_literals, absolute_import

import os.path
import subprocess
from random import randrange
from time import time
from collections import deque
from tempfile import mktemp
from pickle import dump

import jailsetup

MEASUREMENTS = 1000

# Get a bunch of files from all over the place to use for future read load
FILES = deque()
for dirpath, dirnames, filenames in os.walk('/'):
    for f in filenames:
        FILES.append(os.path.join(dirpath, f))
    if len(FILES) > 10000:
        break


def mean(values):
    return sum(values) / len(values)


def measure(f):
    before = time()
    f()
    after = time()
    return after - before


def measure_read():
    try:
        return measure(lambda: os.stat(FILES[0]))
    finally:
        FILES.rotate()



def measure_read_jail(jail_id, samples):
    output = subprocess.check_output(["jexec", jail_id, "sh", "-c", "time stat /"])
    print output
    return [1.0]



def measure_write():
    return measure(lambda: open(mktemp(), 'a').close())



def measure_write_jail(jail_id, samples):
    output = subprocess.check_output(["jexec", jail_id, "sh", "-c", "touch /tmp/tmp12345"])
    print output
    return [2.0]



class Jail(object):
    def __init__(self, name):
        self.name = name
        jailsetup.initial_setup(self.name)


    def start(self):
        jailsetup.start_jail(self.name)


    def stop(self):
        jailsetup.stop_jail(self.name)



class ZFSLoad(object):
    def __init__(self, root, zpool):
        self.root = root
        self.zpool = zpool
        # Create a new filesystem to play around with
        self.filesystem = 'zfs-perf-test-%d' % (randrange(2 ** 16),)
        self._create_filesystem(self.filesystem)

        # Take a snapshot of that filesystem to later replay onto
        self._create_snapshot(self.filesystem, 'start')

        # Make some changes so we have a sizable change log to replay
        self._create_changes(self.filesystem)

        # Take the new snapshot
        self._create_snapshot(self.filesystem, 'end')

        # Record the changes into a file to replay from
        self._snapshot = self._record_changes(
            self.filesystem, 'start', 'end')

        # Get rid of the changes so we can replay them
        self._destroy_snapshot(self.filesystem, 'end')


    def _run(self, *command, **kwargs):
        result = subprocess.call(command, **kwargs)
        print command, result


    def _create_filesystem(self, filesystem):
        fqfn = "%s/%s" % (self.zpool, self.filesystem)
        self._run("zfs", "create", fqfn)
        self._run(
            "zfs", "set", "mountpoint=%s/%s" % (self.root, filesystem),
            fqfn)


    def _create_snapshot(self, filesystem, name):
        self._run(
            "zfs", "snapshot", "%s/%s@%s" % (self.zpool, filesystem, name))

    def _create_changes(self, filesystem):
        pattern = (
            "she slit the sheet the sheet she slit and on the slitted sheet "
            "she sits.") * 64
        for i in range(2 ** 16):
            fObj = open("%s/%s/data.%d" % (self.root, filesystem, i), "w")
            fObj.write(pattern)
            fObj.close()
            pattern = pattern[1:] + pattern[0]


    def _record_changes(self, filesystem, start, end):
        output_filename = "%s_%s_%s" % (filesystem, start, end)
        fObj = open(output_filename, "w")
        self._run(
            "zfs", "send", "-I",
            "%s/%s@%s" % (self.zpool, filesystem, start),
            "%s/%s@%s" % (self.zpool, filesystem, end),
            stdout=fObj)
        fObj.close()
        return output_filename


    def _destroy_snapshot(self, filesystem, name):
        self._run(
            "zfs", "destroy", "%s/%s@%s" % (self.zpool, filesystem, name))


    def _receive_snapshot(self, filesystem, onto, input_filename):
        fObj = open(input_filename, 'r')
        return subprocess.Popen(["zfs", "recv"], stdin=fObj)


    def start(self):
        # Replay the change log asynchronously
        self.process = self._receive_snapshot('start', self._snapshot)


    def stop(self):
        # Stop whatever command is currently in progress
        self.process.kill()



def main():
    load = ZFSLoad('/hcfs', jailsetup.ZPOOL)
    jail = Jail("testjail-%d" % (randrange(2 ** 16),))

    read_measurements = [measure_read() for i in range(MEASUREMENTS)]
    write_measurements = [measure_write() for i in range(MEASUREMENTS)]

    load.start()
    try:
        loaded_read_measurements = [measure_read() for i in range(MEASUREMENTS)]
        loaded_write_measurements = [measure_write() for i in range(MEASUREMENTS)]
    finally:
        load.stop()

    jail.start()
    try:
        jail_read_measurements = measure_read_jail(jail.name, MEASUREMENTS)
        jail_write_measurements = measure_write_jail(jail.name, MEASUREMENTS)

        load.start()
        try:
            loaded_jail_read_measurements = measure_read_jail(jail.name, MEASUREMENTS)
            loaded_jail_write_measurements = measure_write_jail(jail.name, MEASUREMENTS)
        finally:
            load.stop()
    finally:
        jail.stop()

    print 'mean unloaded read time', mean(read_measurements)
    print 'mean unloaded write time', mean(write_measurements)

    print 'mean unloaded jail read time', mean(jail_read_measurements)
    print 'mean unloaded jail write time', mean(jail_write_measurements)

    print 'mean loaded read time', mean(read_measurements)
    print 'mean loaded write time', mean(write_measurements)

    print 'mean loaded jail read time', mean(loaded_jail_read_measurements)
    print 'mean loaded jail write time', mean(loaded_jail_write_measurements)

    output = open('zfs-perf-test.pickle', 'w')
    dump(dict(
            read_measurements=read_measurements,
            write_measurements=write_measurements,
            jail_read_measurements=jail_read_measurements,
            jail_write_measurements=jail_write_measurements,
            loaded_read_measurements=loaded_read_measurements,
            loaded_write_measurements=loaded_write_measurements,
            loaded_jail_read_measurements=loaded_jail_read_measurements,
            loaded_jail_write_measurements=loaded_jail_write_measurements),
                output)


if __name__ == '__main__':
    main()

