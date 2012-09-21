
# TODO: http://wiki.freebsd.org/BenchmarkAdvice

from __future__ import division, unicode_literals, absolute_import

import os.path
import subprocess
from signal import SIGCHLD, SIG_DFL, signal

from random import randrange
from time import time, ctime
from collections import deque
from tempfile import mktemp
from pickle import dump

import jailsetup

TOUCH = "touch"
STAT = "stat"
PYTHON = "/usr/local/bin/python"

WARMUP_MEASUREMENTS = 1000
MEASUREMENTS = WARMUP_MEASUREMENTS * 10

# Get a bunch of files from all over the place to use for future read load

def check_output(*popenargs, **kwargs):
    r"""Run command with arguments and return its output as a byte string.

    If the exit code was non-zero it raises a CalledProcessError.  The
    CalledProcessError object will have the return code in the returncode
    attribute and output in the output attribute.

    The arguments are the same as for the Popen constructor.  Example:

    >>> check_output(["ls", "-l", "/dev/null"])
    'crw-rw-rw- 1 root root 1, 3 Oct 18  2007 /dev/null\n'

    The stdout argument is not allowed as it is used internally.
    To capture standard error in the result, use stderr=STDOUT.

    >>> check_output(["/bin/sh", "-c",
    ...               "ls -l non_existent_file ; exit 0"],
    ...              stderr=STDOUT)
    'ls: non_existent_file: No such file or directory\n'
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd)
    return output



def mean(values):
    return sum(values) / len(values)



def _parse_time(output):
    # time output looks like this:
    #     <program output>
    # 0.000u 0.001s 0:00.00 0.0%      0+0k 0+0io 0pf+0w
    # The default time format is `%Uu %Ss %E %P %X+%Dk %I+%Oio %Fpf+%Ww'
    # %U  The time the process spent in user mode in cpu seconds.
    # %S  The time the process spent in kernel mode in cpu seconds.
    # %E  The elapsed (wall clock) time in seconds.

    measurement = output.splitlines()[-1]
    parts = measurement.strip().split()
    user = float(parts[0][:-1])
    system = float(parts[1][:-1])
    m, s = parts[2].split(':')
    wall = int(m) * 60 + float(s)

    return wall



def _initialize_for_read(tmpfile):
    fObj = open(tmpfile, 'w')
    fObj.write(b''.join(chr(i) for i in range(255)) * 255)
    fObj.close()



def measure_read(samples):
    FILES = []
    for x in range(samples):
        tmpfilesbase = "/usr/jails/tmpfiles"
        if not os.path.exists(tmpfilesbase):
            os.mkdir(tmpfilesbase)
        tmpfile = tmpfilesbase + '/' + str(x)
        _initialize_for_read(tmpfile)
        FILES.append(tmpfile)

    stat = os.stat
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
            "jls", "-j", jail_id, "-h"])
    path = output.splitlines()[1].split()[8]
    FILES = []
    for x in range(samples):
        tmpfile = path + '/' + str(x)
        _initialize_for_read(tmpfile)
        FILES.append(str(x))

    output = check_output([
            "jexec", jail_id, PYTHON, "-c",
            "def measure():\n"
            "    import sys, os, time\n"
            "    stat = os.stat\n"
            "    times = []\n"
            "    for fName in sys.argv[1:]:\n"
            "        before = time.time()\n"
            "        open(fName).read()\n"
            "        after = time.time()\n"
            "        times.append(after - before)\n"
            "    print times\n"
            "measure()\n"] + FILES)
    return eval(output)



def measure_write_jail(jail_id, samples):
    output = check_output([
            "jexec", jail_id, PYTHON, "-c",
            "def measure():\n"
            "    import time, sys\n"
            "    times = []\n"
            "    for filename in sys.argv[1:]:\n"
            "        before = time.time()\n"
            "        open(filename, 'a').close()\n"
            "        after = time.time()\n"
            "        times.append(after - before)\n"
            "    print times\n"
            "measure()\n"] + [mktemp() for i in range(samples)])
    return eval(output)



class Jail(object):
    def __init__(self, name):
        self.name = name
        jailsetup.initial_setup(self.name)


    def start(self):
        self.id = jailsetup.start_jail(self.name)
        check_output(["jexec", self.id, "pkg_add", "python26.tbz"])


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
            "zfs", "set",
            "mountpoint=%s/%s" % (self.root, filesystem),
            fqfn)
        self._run(
            "zfs", "set",
            "atime=off",
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


    def _receive_snapshot(self, filesystem, input_filename):
        fObj = open(input_filename, 'r')
        # Unmount the filesystem before receiving into it.
        jailsetup.run_return("zfs umount %s/%s" % (self.zpool, filesystem))
        return subprocess.Popen([
                "zfs", "recv", "-F", "-d", "%s/%s" % (self.zpool, filesystem)],
                                stdin=fObj)


    def start(self):
        signal(SIGCHLD, self._sigchld)
        # Replay the change log asynchronously; TODO measure how long this
        # runs for, so we can be sure it runs for the duration of the test.
        print ctime(), 'Load started'
        self.process = self._receive_snapshot(
            self.filesystem, self._snapshot)


    def _sigchld(self, *args):
        print ctime(), 'Load ended?'


    def stop(self):
        # Stop whatever command is currently in progress and wait for it to
        # actually exit.
        print ctime(), 'Load stopped'
        signal(SIGCHLD, SIG_DFL)
        print ctime(), "Killing and waiting.."
        self.process.kill()
        self.process.wait()
        print ctime(), "Wait completed"
        # Delete the snapshot so we can receive it again.
        self._destroy_snapshot(self.filesystem, 'end')



def milli(seconds):
    return '%.2f ms' % (seconds * 1000,)



def main():
    load = ZFSLoad('/hcfs', jailsetup.ZPOOL)
    jail = Jail("testjail-%d" % (randrange(2 ** 16),))

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
