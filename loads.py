from twisted.internet.protocol import ProcessProtocol
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue, succeed, maybeDeferred
from twisted.internet.error import ProcessDone
from twisted.internet.utils import getProcessOutput
from twisted.internet import reactor
from twisted.internet import task
from random import randrange
import os, time, random
from time import ctime

ZFS = b"/sbin/zfs"
LARGE_MODE = True

if LARGE_MODE:
    CHANGE_FILES_COUNT = 2 ** 20
else:
    CHANGE_FILES_COUNT = 2 ** 8


def _summarize(proto):
    if proto.endedReason.check(ProcessDone):
        print "\t(%s)ended successfully" % (proto.pid,)
    else:
        print "\t(%s)ended:\t%s" % (proto.pid, proto.endedReason.getErrorMessage())
    if proto.out:
        print "\toutput:\t", b"".join(proto.out)[:80]
    if proto.err:
        print "\terrput:\t", b"".join(proto.err)


class BaseLoad(object):
    tag = 'zfs-perf-test'

    def __init__(self, root, zpool):
        """
        @param root: A string giving the root path of the zpool to benchmark in.

        @param zpool: The zfs pool name to benchmark in.
        """
        self.root = root
        self.zpool = zpool
        self.filesystem = b'hcfs/%s-%d' % (self.tag, randrange(2 ** 16),)
        self.cooperativeTask = None
        self._done = None
        self._stopFlag = False
        self._cooperator = None


    def _startCooperativeTask(self):
        if self._cooperator is not None:
            raise Exception("Don't start me twice!")
        self._cooperator = task.cooperate(self._generator())
        self._done = self._cooperator.whenDone()
        self._done.addBoth(self._cleanup)


    def _cleanup(self, passthrough):
        return passthrough


    def _stopCooperativeTask(self):
        self._stopFlag = True
        return self._done


    def _generator(self):
        """
        Return a generator which loops until self._stopFlag becomes false,
        running a job on each interation.
        """
        while not self._stopFlag:
            yield self._oneStep()
        self._cooperator = None


    def _oneStep(self):
        # Override me!
        return 1


    def start(self, benchmarkFilesystem):
        """
        @param benchmarkFilesystem: The name of the filesystem being benchmarked.
        """
        # Runs in reactor thread.  Return a Deferred that fires when
        # load is started.  Load runs until stop is called.
        pass


    def stop(self):
        # Runs in reactor thread.  Return a Deferred that fires when
        # load is stopped.
        return maybeDeferred(self._stopCooperativeTask)


    @inlineCallbacks
    def _create_filesystem(self, filesystem):
        fqfn = b"%s/%s" % (self.zpool, filesystem)
        yield self._run(ZFS, b"create", fqfn)
        yield self._run(
            ZFS, b"set",
            b"mountpoint=%s/%s" % (self.root, filesystem),
            fqfn)
        yield self._run(
            ZFS, b"set",
            b"atime=off",
            fqfn)


    def _create_snapshot(self, filesystem, name):
        return self._run(
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


    @inlineCallbacks
    def _record_changes(self, filesystem, start, end):
        output_filename = b"%s_%s_%s" % (filesystem, start, end)
        fObj = open(output_filename, "w")
        yield self._run(
            ZFS, b"send", b"-I",
            b"%s/%s@%s" % (self.zpool, filesystem, start),
            b"%s/%s@%s" % (self.zpool, filesystem, end),
            childFDs={0: 'w', 1: fObj.fileno(), 2: 'r'})
        fObj.close()
        returnValue(output_filename)


    def _destroy_snapshot(self, filesystem, name):
        return self._run(
            ZFS, b"destroy", b"%s/%s@%s" % (self.zpool, filesystem, name))


    def _destroy_filesystem(self, filesystem):
        return self._run(
            ZFS, b"destroy", b"-r", b"%s/%s" % (self.zpool, filesystem))


    def _receive_snapshot(self, filesystem, input_filename):
        class ReceiveProto(ProcessProtocol):
            command = [ZFS, b"recv", b"-F", b"-d", b"%(zpool)s"]

            @classmethod
            def run(cls, reactor):
                proto = cls()
                proto.finished = Deferred()
                proto.fObj = open(input_filename, 'r')
                command = [arg % dict(zpool=self.zpool, filesystem=filesystem)
                           for arg
                           in cls.command]

                reactor.spawnProcess(
                    proto, command[0], command, childFDs={0: proto.fObj.fileno(), 1: 'r', 2: 'r'})
                proto.pid = proto.transport.pid
                print "command (%d):\t%s" % (proto.pid, command), '<', input_filename
                return proto

            def connectionMade(self):
                self.out = []
                self.err = []

            def outReceived(self, data):
                self.out.append(data)

            def errReceived(self, data):
                self.err.append(data)

            def kill(self):
                self.transport.signalProcess("KILL")

            def wait(self):
                return self.finished

            def processEnded(self, reason):
                self.endedReason = reason
                print ctime(), "Load ended!"
                self.fObj.close()

                _summarize(self)

                self.finished.callback(self)

        return ReceiveProto.run(reactor).finished



    def _run(self, *command, **kwargs):
        class Collector(ProcessProtocol):
            finished = None

            @classmethod
            def run(cls, reactor):
                proto = cls()
                proto.finished = Deferred()
                reactor.spawnProcess(proto, command[0], command, **kwargs)
                proto.pid = proto.transport.pid
                return proto

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

        proto = Collector.run(reactor)
        print "command (%d):\t %s %s" % (proto.pid, command, kwargs)
        d = proto.finished
        d.addCallback(_summarize)
        return d



# rename - filesystem
class RenameFilesystemLoad(BaseLoad):
    """
    Forever 'loop' while renaming a filesystem backwards and forwards
    between hpool/hcfs-trash and back...
    """
    tag = 'zfs-rename-test'

    @inlineCallbacks
    def start(self, benchmarkFilesystem):
        """
        Return a C{Deferred} which fires when the filesystem is created.
        """
        yield self._create_filesystem(self.filesystem)
        if not os.path.ismount("/hcfs-trash"):
            yield self._run(ZFS, "create", "hpool/hcfs-trash")
            yield self._run(ZFS, "set", "mountpoint=/hcfs-trash", "hpool/hcfs-trash")

        # Now kick off the thing which runs forever in a loop.
        self._startCooperativeTask()


    @inlineCallbacks
    def _oneStep(self):
        trash_timestamp = str(time.time())
        # Trash the filesystem (cnp'd almost directly from safemounthandler for
        # maximum realism, except added split sadness)
        args = {
            'zpool': self.zpool, 'timestamp': trash_timestamp, 'filesystem': self.filesystem,
            'trash-filesystem': self.filesystem.replace('hcfs/', 'hcfs-trash/'),
            }
        def cmd(s):
            return (s % args).split(" ")

        yield self._run(ZFS, *cmd("rename %(zpool)s/%(filesystem)s %(zpool)s/%(trash-filesystem)s-%(timestamp)s"))
        yield self._run(ZFS, *cmd("set mountpoint=/%(trash-filesystem)s-%(timestamp)s %(zpool)s/%(trash-filesystem)s-%(timestamp)s"))

        # Un-trash the filesystem
        yield self._run(ZFS, *cmd("set mountpoint=/%(filesystem)s %(zpool)s/%(trash-filesystem)s-%(timestamp)s"))
        yield self._run(ZFS, *cmd("rename %(zpool)s/%(trash-filesystem)s-%(timestamp)s %(zpool)s/%(filesystem)s"))



class PruneSnapshots(BaseLoad):
    tag = 'zfs-prune-test'
    random = random.Random(x=1)

    @inlineCallbacks
    def _newSnapshot(self):
        yield self._create_snapshot(self.filesystem, str(self.snapshotCounter))
        yield self._create_changes(self.filesystem)
        self.snapshots.append(self.snapshotCounter)
        self.snapshotCounter += 1

    @inlineCallbacks
    def start(self, benchmarkFilesystem):
        yield self._create_filesystem(self.filesystem)
        self.snapshots = []
        self.snapshotCounter = 0
        # If we were smartarses we could use self.snapshotCounter as the
        # assigment target here, but we prefer readable code ;-)
        for i in range(10):
            yield self._newSnapshot()

        # Now kick off the thing which runs forever in a loop.
        self._startCooperativeTask()


    @inlineCallbacks
    def _oneStep(self):
        yield self._newSnapshot()
        # Randomly pick a snapshot and take it out.
        target = self.random.choice(self.snapshots)
        yield self._destroy_snapshot(self.filesystem, target)
        self.snapshots.remove(target)



# recv - other filesystem - small case
class LotsOfTinySnapshots(BaseLoad):
    """

    """
    COUNT = 50

    @inlineCallbacks
    def start(self, benchmarkFilesystem):

        # Get rid of any leftovers from previous runs
        yield self._destroy_filesystem(self.filesystem)
        yield self._create_filesystem(self.filesystem)

        self.snapshots = []

        # Generate a bunch of snapshots to later replay
        previous = b'0'
        yield self._create_snapshot(self.filesystem, previous)
        for i in range(1, self.COUNT):
            # Stuff some bytes into it to make the snapshot interesting
            self._create_changes(self.filesystem)
            # Take the snapshot
            yield self._create_snapshot(self.filesystem, bytes(i))
            # Dump it into a file for later replay
            snapshot = yield self._record_changes(self.filesystem, bytes(previous), bytes(i))
            previous = i
            self.snapshots.append(snapshot)

        # Delete all of the snapshots just taken
        yield self._reset_snapshots()

        # Start the process of replaying them
        self._startCooperativeTask()


    @inlineCallbacks
    def _reset_snapshots(self):
        """
        Delete all the snapshots we received, reload the list of
        snapshots that we can now receive.
        """
        # Keep the first snapshot, since that's what the first saved
        # incremental stream is based on.
        for i in range(1, self.COUNT):
            yield self._destroy_snapshot(self.filesystem, bytes(i))
        self.snapshots_for_replay = self.snapshots[:]


    def _oneStep(self):
        if not self.snapshots_for_replay:
            return self._reset_snapshots()

        snapshot = self.snapshots_for_replay.pop(0)
        return self._receive_snapshot(self.filesystem, snapshot)


    @inlineCallbacks
    def _cleanup(self, passthrough):
        # Delete the entire filesystem we were using
        yield self._destroy_filesystem(self.filesystem)
        # And delete all the saved incremental streams we created too
        for s in self.snapshots:
            os.remove(s)
        returnValue(passthrough)


# recv - not-exist


# snapshot - other filesystem


# snapshot - same filesystem
class SnapshotUsedFilesystemLoad(BaseLoad):
    _iteration = 0

    def _oneStep(self):
        self._iteration += 1
        return self._create_snapshot(self.benchmarkFilesystem, bytes(self._iteration))


    def start(self, benchmarkFilesystem):
        self.benchmarkFilesystem = benchmarkFilesystem
        self._task = self._startCooperativeTask()
        return succeed(None)



# recv - other filesystem - large case
class ReplayLargeLoad(BaseLoad):
    """
    """
    process = None

    def _oneStep(self):
        # Run a "zfs recv".  If it finishes, destroy the received snapshot and
        # run the same "zfs recv" again.  Continue until poked from the outside
        # to stop.  Call this in the reactor thread.
        yield self._destroy_snapshot(self.filesystem, b'end')
        self.process = self._receive_snapshot(self.filesystem, self._snapshot)
        yield self.process.finished
        self.process = None


    @inlineCallbacks
    def start(self, benchmarkFilesystem):
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

        if self.process is not None:
            # Kill the currently running zfs recv
            self.process.kill()

        return BaseLoad.stop(self)
