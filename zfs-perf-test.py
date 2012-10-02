
# TODO: http://wiki.freebsd.org/BenchmarkAdvice

from __future__ import division, unicode_literals, absolute_import

import os.path
from sys import stdout

from random import randrange
from time import time, ctime
from tempfile import mktemp
from pickle import dump

from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from twisted.internet.utils import getProcessOutput
from twisted.internet.error import ProcessDone
from twisted.internet.threads import deferToThread, blockingCallFromThread
from twisted.internet.protocol import ProcessProtocol
from twisted.python.log import startLogging, err

import jailsetup
from loads import LARGE_MODE, BaseLoad

TOUCH = b"touch"
STAT = b"stat"
ZFS = b"/sbin/zfs"
JEXEC = b"/usr/sbin/jexec"
JLS = b"/usr/sbin/jls"
PKG_ADD = b"/usr/sbin/pkg_add"

if LARGE_MODE:
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



# recv - other filesystem - small case
class LotsOfTinySnapshots(BaseLoad):
    """
    
    """
    def _oneStep(self):
        pass


    @inlineCallbacks
    def start(self):
        # Get rid of any leftovers from previous runs
        yield self._destroy_filesystem(self.filesystem)
        yield self._create_filesystem(self.filesystem)

        self.snapshots_for_replay = []
        snapshot_base = b"%s_%%s_%%s" % (self.filesystem,)

        # Generate a bunch of snapshots to later replay
        previous = b'0'
        self._create_snapshot(self.filesystem, previous)
        for i in range(1, 100):
            # Stuff some bytes into it to make the snapshot interesting
            self._create_changes(self.filesystem)
            # Take the snapshot
            yield self._create_snapshot(self.filesystem, bytes(i))
            # Dump it into a file for later replay
            snapshot = snapshot_base % (previous, i)
            self._record_changes(snapshot, bytes(previous), bytes(i))
            previous = i
            self.snapshots_for_replay.append(snapshot)

        # Delete all of the snapshots just taken
        for i in range(100):
            self._destroy_snapshot(self.filesystem, bytes(i))

        # Save a list of all the snapshots we took
        self.snapshots = self.snapshots_for_replay[:]

        # Start the process of replaying them
        self._startCooperativeTask()


    def _oneStep(self):
        if not self.snapshots_for_replay:
            return self._reset_snapshots()

        snapshot = self.snapshots_for_replay.pop(0)
        return self._receive_snapshot(self.filesystem, snapshot)


# recv - not-exist


# snapshot - other filesystem


# snapshot - same filesystem
class SnapshotUsedFilesystemLoad(BaseLoad):
    _iteration = 0

    def _oneStep(self):
        self._iteration += 1
        return self._create_snapshot(self.benchmarkFilesystem, bytes(iteration))
        

    def start(self):
        self._task = self._startCooperativeTask()
        return succeed(None)



# recv - other filesystem - large case
class ReplayLargeLoad(BaseLoad):
    """
    """
    def _oneStep(self):
        # Run a "zfs recv".  If it finishes, destroy the received snapshot and
        # run the same "zfs recv" again.  Continue until poked from the outside
        # to stop.  Call this in the reactor thread.
        yield self._destroy_snapshot(self.filesystem, b'end')
        self.process = self._receive_snapshot(self.filesystem, self._snapshot)
        yield self.process.finished


    @inlineCallbacks
    def start(self):
        self._stopLoad = False

        # Get rid of any leftovers from previous runs
        yield self._destroy_filesystem(self.filesystem)
        yield self._create_filesystem(self.filesystem)

        # Take a snapshot of that filesystem to later replay onto
        yield self._create_snapshot(self.filesystem, b'start')

        # Make some changes so we have a sizable change log to replay
        yield self._create_changes(self.filesystem)

        # Take the new snapshot
        yield self._create_snapshot(self.filesystem, b'end')

        # Record the changes into a file to replay from
        self._snapshot = yield self._record_changes(
            self.filesystem, b'start', b'end')

        # Unmount the filesystem before receiving into it.
        yield getProcessOutput(
            ZFS, [b"umount", b"%s/%s" % (self.zpool, self.filesystem)])

        # Replay the change log asynchronously
        print ctime(), 'Load started'
        self._startCooperativeTask()


    def stop(self):
        # Stop whatever command is currently in progress and wait for it to
        # actually exit.
        print ctime(), "Killing load and waiting.."

        # Stop the process loop
        self._stopLoad = True

        if self.process is not None:
            # Kill the currently running zfs recv
            self.process.kill()

        return BaseLoad.stop(self)



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



class Blocking(object):
    def __init__(self, what):
        self.what = what


    def start(self):
        return blockingCallFromThread(self.what.start)


    def stop(self):
        return blockingCallFromThread(self.what.stop)



def benchmark():
    print 'Initializing...'
    load = Blocking(ReplayLargeLoad(b'/hcfs', jailsetup.ZPOOL))
    jail = Blocking(Jail(b"testjail-%d" % (randrange(2 ** 16),)))

    print ctime(), "STARTING UNLOADED TEST"
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
