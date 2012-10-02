
# TODO: http://wiki.freebsd.org/BenchmarkAdvice

from __future__ import division, unicode_literals, absolute_import

import os.path
from sys import stdout, argv

from random import randrange
from time import time, ctime
from tempfile import mktemp
from pickle import dump

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, gatherResults
from twisted.internet.utils import getProcessOutput
from twisted.internet.error import ProcessDone
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.python.log import startLogging, err

import jailsetup
import loads

TOUCH = b"touch"
STAT = b"stat"
ZFS = b"/sbin/zfs"
JEXEC = b"/usr/sbin/jexec"
JLS = b"/usr/sbin/jls"
PKG_ADD = b"/usr/sbin/pkg_add"

if loads.LARGE_MODE:
    PYTHON = b"/usr/bin/python"
    TMP = b"/tmp/jails/tmpfiles"
    READ_FILES_FACTOR = 255
else:
    PYTHON = b"/usr/local/bin/python"
    TMP = b"/usr/jails/tmpfiles"
    READ_FILES_FACTOR = 2

WARMUP_MEASUREMENTS = 1000
MEASUREMENTS = WARMUP_MEASUREMENTS * 10

def check_output(popenargs):
    try:
        output = blockingCallFromThread(
            reactor, getProcessOutput, popenargs[0], popenargs[1:])
    except Exception, e:
        print e
    else:
        return output


def _summarize(proto):
    if proto.out:
        print "\toutput:\t", b"".join(proto.out)[:80]
    if proto.err:
        print "\terrput:\t", b"".join(proto.err)
    if proto.endedReason.check(ProcessDone):
        print "\tended successfully"
    else:
        print "\tended:\t", proto.endedReason.getErrorMessage()


def mean(values):
    return sum(values) / len(values)



def _initialize_for_read(tmpfile):
    fObj = open(tmpfile, 'w')
    fObj.write(b''.join(chr(i) for i in range(255)) * READ_FILES_FACTOR)
    fObj.close()



def measure_read(samples):
    FILES = []
    for x in range(samples):
        if not os.path.exists(TMP):
            os.mkdir(TMP)
        tmpfile = TMP + b'/' + bytes(x)
        _initialize_for_read(tmpfile)
        FILES.append(tmpfile)

    times = []
    for fName in FILES:
        before = time()
        open(fName).read()
        after = time()
        times.append(after - before)
    return times



def measure_write(samples):
    filenames = [mktemp() for i in range(samples)]
    times = []
    for filename in filenames:
        before = time()
        open(filename, "a").close()
        after = time()
        times.append(after - before)
    return times



def measure_read_jail(jail_id, samples):
    output = check_output([
            JLS, b"-j", jail_id, b"-h"])
    path = output.splitlines()[1].split()[8]
    FILES = []
    for x in range(samples):
        tmpfile = path + b'/' + bytes(x)
        _initialize_for_read(tmpfile)
        FILES.append(str(x))

    output = check_output([
            JEXEC, jail_id, PYTHON, b"-c",
            b"def measure():\n"
            b"    import sys, os, time\n"
            b"    stat = os.stat\n"
            b"    times = []\n"
            b"    for fName in sys.argv[1:]:\n"
            b"        before = time.time()\n"
            b"        open(fName).read()\n"
            b"        after = time.time()\n"
            b"        times.append(after - before)\n"
            b"    print times\n"
            b"measure()\n"] + FILES)
    return eval(output)



def measure_write_jail(jail_id, samples):
    output = check_output([
            JEXEC, jail_id, PYTHON, b"-c",
            b"def measure():\n"
            b"    import time, sys\n"
            b"    times = []\n"
            b"    for filename in sys.argv[1:]:\n"
            b"        before = time.time()\n"
            b"        open(filename, 'a').close()\n"
            b"        after = time.time()\n"
            b"        times.append(after - before)\n"
            b"    print times\n"
            b"measure()\n"] + [mktemp() for i in range(samples)])
    return eval(output)



class Jail(object):
    def __init__(self, name):
        self.name = name
        jailsetup.initial_setup(self.name)


    @inlineCallbacks
    def start(self):
        self.id = yield deferToThread(jailsetup.start_jail, self.name)
        yield getProcessOutput(JEXEC, [self.id, PKG_ADD, b"python26.tbz"])


    def stop(self):
        return deferToThread(jailsetup.stop_jail, self.name)



def milli(seconds):
    return '%.2f ms' % (seconds * 1000,)



class ParallelLoad(object):
    def __init__(self, loads):
        self.loads = loads

    def start(self, benchmarkFilesystem):
        return gatherResults([load.start(benchmarkFilesystem) for load in self.loads])


    def stop(self):
        return gatherResults([load.stop() for load in self.loads])



def main(argv):
    startLogging(stdout)

    if len(argv) == 0:
        argv = [b"ReplayLargeLoad"]
    load = ParallelLoad([
            getattr(loads, arg)(b'/hcfs', jailsetup.ZPOOL)
            for arg in argv])
    jail = Jail(b"testjail-%d" % (randrange(2 ** 16),))

    print 'Starting benchmark'
    d = deferToThread(benchmark, load, jail)
    d.addErrback(err, "Benchmark failed")
    d.addBoth(lambda ignored: reactor.stop())
    print 'Running reactor'
    reactor.run()



class Blocking(object):
    def __init__(self, what):
        self.what = what


    def start(self):
        return blockingCallFromThread(self.what.start)


    def stop(self):
        return blockingCallFromThread(self.what.stop)



def benchmark(load, jail):
    print 'Initializing...'
    load = Blocking(load)
    jail = Blocking(jail)

    print ctime(), "STARTING UNLOADED TEST"
    read_measurements = measure_read(MEASUREMENTS)
    write_measurements = measure_write(MEASUREMENTS)
    print ctime(), "DONE UNLOADED TEST"

    print ctime(), "STARTING LOADED TEST"

    load.start(benchmarkFilesystem=b'/')
    try:
        loaded_read_measurements = measure_read(MEASUREMENTS)
        loaded_write_measurements = measure_write(MEASUREMENTS)
    finally:
        load.stop()

    print ctime(), "DONE LOADED TEST"

    print ctime(), "STARTING JAIL TEST"

    jail.start()
    try:
        jail_read_measurements = measure_read_jail(jail.id, MEASUREMENTS)
        jail_write_measurements = measure_write_jail(jail.id, MEASUREMENTS)

        print ctime(), "DONE JAIL TEST"

        print ctime(), "STARTING LOADED JAIL TEST"
        load.start(benchmarkFilesystem=b"/usr/jails/" + jail.name)
        try:
            loaded_jail_read_measurements = measure_read_jail(jail.id, MEASUREMENTS)
            loaded_jail_write_measurements = measure_write_jail(jail.id, MEASUREMENTS)
        finally:
            load.stop()

        print ctime(), "DONE LOADED JAIL TEST"
    finally:
        jail.stop()

    print 'mean unloaded read time', milli(mean(read_measurements[WARMUP_MEASUREMENTS:]))
    print 'mean unloaded write time', milli(mean(write_measurements[WARMUP_MEASUREMENTS:]))

    print 'mean loaded read time', milli(mean(loaded_read_measurements[WARMUP_MEASUREMENTS:]))
    print 'mean loaded write time', milli(mean(loaded_write_measurements[WARMUP_MEASUREMENTS:]))

    print 'mean unloaded jail read time', milli(mean(jail_read_measurements[WARMUP_MEASUREMENTS:]))
    print 'mean unloaded jail write time', milli(mean(jail_write_measurements[WARMUP_MEASUREMENTS:]))

    print 'mean loaded jail read time', milli(mean(loaded_jail_read_measurements[WARMUP_MEASUREMENTS:]))
    print 'mean loaded jail write time', milli(mean(loaded_jail_write_measurements[WARMUP_MEASUREMENTS:]))

    output = open(b'zfs-perf-test.pickle', 'w')
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
    main(argv[1:])
