
# TODO: http://wiki.freebsd.org/BenchmarkAdvice

from __future__ import division, unicode_literals, absolute_import

import os.path
from sys import stdout

from random import randrange
from time import time, ctime
from tempfile import mktemp
from pickle import dump

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.utils import getProcessOutput
from twisted.internet.error import ProcessDone
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.internet.protocol import ProcessProtocol
from twisted.python.log import startLogging, err

import jailsetup

TOUCH = b"touch"
STAT = b"stat"
ZFS = b"/sbin/zfs"

if False:
    PYTHON = b"/usr/bin/python"
    TMP = b"/tmp/jails/tmpfiles"
    CHANGE_FILES_COUNT = 2 ** 20
    READ_FILES_FACTOR = 255
else:
    PYTHON = b"/usr/local/bin/python"
    TMP = b"/usr/jails/tmpfiles"
    CHANGE_FILES_COUNT = 2 ** 8
    READ_FILES_FACTOR = 2

WARMUP_MEASUREMENTS = 1000
MEASUREMENTS = WARMUP_MEASUREMENTS * 10

def check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
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
        print "\terrput:\t", b"".join(proto.err)[:80]
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
            b"jls", b"-j", jail_id, b"-h"])
    path = output.splitlines()[1].split()[8]
    FILES = []
    for x in range(samples):
        tmpfile = path + b'/' + bytes(x)
        _initialize_for_read(tmpfile)
        FILES.append(str(x))

    output = check_output([
            b"jexec", jail_id, PYTHON, b"-c",
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
            b"jexec", jail_id, PYTHON, b"-c",
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


    def start(self):
        self.id = jailsetup.start_jail(self.name)
        check_output([b"jexec", self.id, b"pkg_add", b"python26.tbz"])


    def stop(self):
        jailsetup.stop_jail(self.name)



class ReplayLargeLoad(object):
    def __init__(self, root, zpool):
        self.root = root
        self.zpool = zpool
        # Create a new filesystem to play around with
        self.filesystem = b'zfs-perf-test-%d' % (randrange(2 ** 16),)
        self._create_filesystem(self.filesystem)

        # Take a snapshot of that filesystem to later replay onto
        self._create_snapshot(self.filesystem, b'start')

        # Make some changes so we have a sizable change log to replay
        self._create_changes(self.filesystem)

        # Take the new snapshot
        self._create_snapshot(self.filesystem, b'end')

        # Record the changes into a file to replay from
        self._snapshot = self._record_changes(
            self.filesystem, b'start', b'end')


    def _run(self, *command, **kwargs):
        class Collector(ProcessProtocol):
            finished = None

            @classmethod
            def run(cls, reactor):
                proto = cls()
                proto.finished = Deferred()
                reactor.spawnProcess(proto, command[0], command, **kwargs)
                return proto.finished

            def connectionMade(self):
                self.out = []
                self.err = []

            def outReceived(self, data):
                self.out.append(data)

            def errReceived(self, data):
                self.err.append(data)

            def processEnded(self, reason):
                self.endedReason = reason
                self.finished.callback(self)

        print "command:\t", command, kwargs
        proto = blockingCallFromThread(reactor, Collector.run, reactor)
        _summarize(proto)


    def _create_filesystem(self, filesystem):
        fqfn = b"%s/%s" % (self.zpool, self.filesystem)
        self._run(ZFS, b"create", fqfn)
        self._run(
            ZFS, b"set",
            b"mountpoint=%s/%s" % (self.root, filesystem),
            fqfn)
        self._run(
            ZFS, b"set",
            b"atime=off",
            fqfn)


    def _create_snapshot(self, filesystem, name):
        self._run(
            ZFS, b"snapshot", b"%s/%s@%s" % (self.zpool, filesystem, name))

    def _create_changes(self, filesystem):
        pattern = (
            b"she slit the sheet the sheet she slit and on the slitted sheet "
            b"she sits.") * 64
        for i in range(CHANGE_FILES_COUNT):
            fObj = open(b"%s/%s/data.%d" % (self.root, filesystem, i), "w")
            fObj.write(pattern)
            fObj.close()
            pattern = pattern[1:] + pattern[0]


    def _record_changes(self, filesystem, start, end):
        output_filename = b"%s_%s_%s" % (filesystem, start, end)
        fObj = open(output_filename, "w")
        self._run(
            ZFS, b"send", b"-I",
            b"%s/%s@%s" % (self.zpool, filesystem, start),
            b"%s/%s@%s" % (self.zpool, filesystem, end),
            childFDs={0: 'w', 1: fObj.fileno(), 2: 'r'})
        fObj.close()
        return output_filename


    def _destroy_snapshot(self, filesystem, name):
        self._run(
            ZFS, b"destroy", b"%s/%s@%s" % (self.zpool, filesystem, name))


    def _receive_snapshot(self, filesystem, input_filename):
        class ReceiveProto(ProcessProtocol):
            command = [ZFS, b"recv", b"-F", b"-d", b"%(zpool)s/%(filesystem)s"]

            @classmethod
            def run(cls, reactor):
                proto = cls()
                proto.finished = Deferred()
                proto.fObj = open(input_filename, 'r')
                command = [arg % dict(zpool=self.zpool, filesystem=filesystem)
                           for arg
                           in cls.command]

                print "command:\t", command
                reactor.spawnProcess(
                    proto, command, childFDs={0: proto.fObj.fileno(), 1: 'r', 2: 'r'})
                return proto

            def connectionMade(self):
                self.out = []
                self.err = []

            def outReceived(self, data):
                self.out.append(data)

            def errReceived(self, data):
                self.err.append(data)

            def kill(self):
                reactor.callFromThread(self.transport.signalProcess, "KILL")

            def wait(self):
                blockingCallFromThread(reactor, lambda: self.finished)

            def processEnded(self, reason):
                self.endedReason = reason
                print ctime(), "Load ended!"
                self.fObj.close()

                _summarize(self)

                self.finished.callback(self)

        return ReceiveProto.run(reactor)


    def start(self):
        # Unmount the filesystem before receiving into it.
        jailsetup.run_return(
            b"%s umount %s/%s" % (ZFS, self.zpool, self.filesystem))
        self._stopLoad = False
        # Replay the change log asynchronously
        print ctime(), 'Load started'
        reactor.callFromThread(self._generate_load)


    def _generate_load(self):
        # Run a "zfs recv".  If it finishes, destroy the received snapshot and
        # run the same "zfs recv" again.  Continue until poked from the outside
        # to stop.  Call this in the reactor thread.
        def restart(ignored=None):
            self.process = None

            if self._stopLoad:
                return

            print 'Destroying snapshot to re-receive'
            d = deferToThread(
                self._destroy_snapshot, self.filesystem, b'end')

            def cleanupDone(ignored):
                print 'Starting load'
                self.process = self._receive_snapshot(
                    self.filesystem, self._snapshot)
                self.process.finished.addCallback(restart)
            d.addCallback(cleanupDone)
        restart()


    def stop(self):
        # Stop whatever command is currently in progress and wait for it to
        # actually exit.
        print ctime(), "Killing load and waiting.."

        # Stop the process loop
        self._stopLoad = True

        if self.process is not None:
            # Kill the currently running zfs recv
            self.process.kill()

            # Wait to be notified it exited
            self.process.wait()
            print ctime(), "Wait completed"

            # Delete the snapshot so we can receive it again.
            self._destroy_snapshot(self.filesystem, b'end')



def milli(seconds):
    return '%.2f ms' % (seconds * 1000,)



def main():
    startLogging(stdout)

    print 'Starting benchmark'
    d = deferToThread(benchmark)
    d.addErrback(err, "Benchmark failed")
    d.addBoth(lambda ignored: reactor.stop())
    print 'Running reactor'
    reactor.run()


def benchmark():
    print ctime(), "STARTING UNLOADED TEST"

    load = ReplayLargeLoad(b'/hcfs', jailsetup.ZPOOL)
    jail = Jail(b"testjail-%d" % (randrange(2 ** 16),))

    read_measurements = measure_read(MEASUREMENTS)
    write_measurements = measure_write(MEASUREMENTS)

    print ctime(), "DONE UNLOADED TEST"

    print ctime(), "STARTING LOADED TEST"

    load.start()
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
        load.start()
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
    main()
