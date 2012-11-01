
# TODO: http://wiki.freebsd.org/BenchmarkAdvice

from __future__ import division, unicode_literals, absolute_import

import os.path
from sys import stdout, argv, platform
import gc

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
from twisted.python import usage

import jailsetup
import loads

TOUCH = b"touch"
STAT = b"stat"
ZFS = b"/sbin/zfs"
JEXEC = b"/usr/sbin/jexec"
JLS = b"/usr/sbin/jls"
PKG_ADD = b"/usr/sbin/pkg_add"

if loads.LARGE_MODE:
    READ_FILES_FACTOR = 255
else:
    READ_FILES_FACTOR = 2

if 'freebsd' in platform:
    PYTHON = b"/usr/local/bin/python"
    TMP = b"/usr/jails/tmpfiles"
elif 'linux' in platform:
    PYTHON = b"/usr/bin/python"
    TMP = b"/tmp/jails/tmpfiles"
else:
    raise Exception("Don't understand this platform")

WARMUP_MEASUREMENTS = 100
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
    if values:
        return sum(values) / len(values)
    return 0



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



def measure_write(tmpdir, samples):
    filenames = [mktemp(tmpdir=tmpdir) for i in range(samples)]
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
            b"    import sys, time\n"
            b"    times = []\n"
            b"    for fName in sys.argv[1:]:\n"
            b"        before = time.time()\n"
            b"        open(fName).read()\n"
            b"        after = time.time()\n"
            b"        times.append(after - before)\n"
            b"    print times\n"
            b"import gc\n"
            b"gc.disable()\n"
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
            b"import gc\n"
            b"gc.disable()\n"
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


class Options(usage.Options):
    optFlags = [('jail', 'j', "Benchmark jail performance as well")]

    def __init__(self):
        usage.Options.__init__(self)
        self['loads'] = [b"ReplayLargeLoad"]


    def parseArgs(self, *loads):
        self['loads'] = sorted(loads)



def main(argv):
    startLogging(stdout)

    o = Options()
    o.parseOptions(argv)

    load = ParallelLoad([
            getattr(loads, arg)(b'/hcfs', jailsetup.ZPOOL)
            for arg in o['loads']])
    if o['jail']:
        jail = Jail(b"testjail-%d" % (randrange(2 ** 16),))
    else:
        jail = None

    print 'Starting benchmark'
    d = deferToThread(benchmark, load, jail)
    def succeeded(results):
        output = open(b'results-%s-%d.pickle' % (",".join(o['loads']), time()), 'w')
        dump(results, output)
        output.close()
    d.addCallback(succeeded)
    d.addErrback(err, "Benchmark failed")
    d.addBoth(lambda ignored: reactor.stop())
    print 'Running reactor'
    reactor.run()



def benchmark(load, jail):
    print 'Initializing...'

    print ctime(), "STARTING UNLOADED TEST"
    read_measurements = measure_read(MEASUREMENTS)
    write_measurements = measure_write(b"/tmp", MEASUREMENTS)
    write_jail_dir_measurements = measure_write(
        b"/usr/jails/" + jail.name + b"/tmp", MEASUREMENTS)
    print ctime(), "DONE UNLOADED TEST"

    print ctime(), "STARTING LOADED TEST"

    blockingCallFromThread(reactor, load.start, benchmarkFilesystem=b'root')
    try:
        loaded_read_measurements = measure_read(MEASUREMENTS)
        loaded_write_measurements = measure_write(MEASUREMENTS)
        loaded_write_jail_dir_measurements = measure_write(
            b"/usr/jails/" + jail.name + b"/tmp", MEASUREMENTS)
    finally:
        blockingCallFromThread(reactor, load.stop)

    print ctime(), "DONE LOADED TEST"

    if jail is None:
        jail_read_measurements = jail_write_measurements = loaded_jail_read_measurements = loaded_jail_write_measurements = []
    else:
        print ctime(), "STARTING JAIL TEST"

        blockingCallFromThread(reactor, jail.start)
        try:
            jail_read_measurements = measure_read_jail(jail.id, MEASUREMENTS)
            jail_write_measurements = measure_write_jail(jail.id, MEASUREMENTS)

            print ctime(), "DONE JAIL TEST"

            print ctime(), "STARTING LOADED JAIL TEST"
            blockingCallFromThread(reactor, load.start, benchmarkFilesystem=b"/usr/jails/" + jail.name)
            try:
                loaded_jail_read_measurements = measure_read_jail(jail.id, MEASUREMENTS)
                loaded_jail_write_measurements = measure_write_jail(jail.id, MEASUREMENTS)
            finally:
                blockingCallFromThread(reactor, load.stop)

            print ctime(), "DONE LOADED JAIL TEST"
        finally:
            blockingCallFromThread(reactor, jail.stop)

    print 'mean unloaded read time', milli(mean(read_measurements))
    print 'mean unloaded write time', milli(mean(write_measurements))

    print 'mean loaded read time', milli(mean(loaded_read_measurements))
    print 'mean loaded write time', milli(mean(loaded_write_measurements))

    print 'mean unloaded jail read time', milli(mean(jail_read_measurements))
    print 'mean unloaded jail write time', milli(mean(jail_write_measurements))

    print 'mean loaded jail read time', milli(mean(loaded_jail_read_measurements))
    print 'mean loaded jail write time', milli(mean(loaded_jail_write_measurements))

    return dict(
        read_measurements=read_measurements,
        write_measurements=write_measurements,
        write_jail_dir_measurements=write_jail_dir_measurements,
        jail_read_measurements=jail_read_measurements,
        jail_write_measurements=jail_write_measurements,
        loaded_read_measurements=loaded_read_measurements,
        loaded_write_measurements=loaded_write_measurements,
        loaded_write_jail_dir_measurements=loaded_write_jail_dir_measurements,
        loaded_jail_read_measurements=loaded_jail_read_measurements,
        loaded_jail_write_measurements=loaded_jail_write_measurements)


if __name__ == '__main__':
    gc.disable()
    main(argv[1:])
