
from __future__ import division, unicode_literals, absolute_import

import os.path
from time import time
from collections import deque
from tempfile import mktemp

MEASUREMENTS = 10

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
    return []



def measure_write():
    return measure(lambda: open(mktemp(), 'a').close())



def measure_write_jail(jail_id, samples):
    return []



class Jail(object):
    def __init__(self, filesystem):
        pass


    def start(self):
        pass


    def stop(self):
        pass



class ZFSLoad(object):
    def __init__(self, filesystem):
        pass


    def start(self):
        pass


    def stop(self):
        pass



def main():
    zfs_pool = 'hpool'
    load = ZFSLoad(zfs_pool)
    jail = Jail(zfs_pool)

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
        jail_read_measurements = measure_read_jail(jail.id, MEASUREMENTS)
        jail_write_measurements = measure_write_jail(jail.id, MEASUREMENTS)

        load.start()
        try:
            loaded_jail_read_measurements = measure_read_jail(jail.id, MEASUREMENTS)
            loaded_jail_write_measurements = measure_write_jail(jail.id, MEASUREMENTS)
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
    pickle.dump(dict(
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

