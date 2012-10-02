from twisted.internet.protocol import ProcessProtocol
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from twisted.internet.error import ProcessDone
from twisted.internet import reactor
from twisted.internet import task
from random import randrange
import os, time, random
from time import ctime

ZFS = b"/sbin/zfs"
LARGE_MODE = False

if LARGE_MODE:
    CHANGE_FILES_COUNT = 2 ** 20
else:
    CHANGE_FILES_COUNT = 2 ** 8


def _summarize(proto):
    if proto.out:
        print "\toutput:\t", b"".join(proto.out)[:80]
    if proto.err:
        print "\terrput:\t", b"".join(proto.err)
    if proto.endedReason.check(ProcessDone):
        print "\tended successfully"
    else:
        print "\tended:\t", proto.endedReason.getErrorMessage()


class BaseLoad(object):
    tag = 'zfs-perf-test'

    def __init__(self, root, zpool, benchmarkFilesystem):
        """
        @param root: A string giving the root path of the zpool to benchmark in.

        @param zpool: The zfs pool name to benchmark in.

        @param benchmarkFilesystem: The name of the filesystem being benchmarked.
        """
        self.root = root
        self.zpool = zpool
        self.benchmarkFilesystem = benchmarkFilesystem
        self.filesystem = b'%s-%d' % (self.tag, randrange(2 ** 16),)
        self.cooperativeTask = None
        self._stopFlag = False
        self._cooperator = None


    def _startCooperativeTask(self):
        if self._cooperator is not None:
            raise Exception("Don't start me twice!")
        self._cooperator = task.cooperate(self._generator())


    def _stopCooperativeTask(self):
        self._stopFlag = True
        return self._cooperator.whenDone()


    def _generator(self):
        """
        Return a generator which loops until self._stopFlag becomes false,
        running a job on each interation.
        """
        while not self._stopFlag:
            yield self._oneStep()


    def _oneStep(self):
        # Override me!
        return 1


    def start(self):
        # Runs in reactor thread.  Return a Deferred that fires when
        # load is started.  Load runs until stop is called.
        pass


    def stop(self):
        # Runs in reactor thread.  Return a Deferred that fires when
        # load is stopped.
        return self._stopCooperativeTask()


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
                    proto, command[0], command, childFDs={0: proto.fObj.fileno(), 1: 'r', 2: 'r'})
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

        return ReceiveProto.run(reactor)



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
        d = Collector.run(reactor)
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
    def start(self):
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
        yield self._run(ZFS, *("rename %s/hcfs/%s %s/hcfs-trash/%s-%s" % (
            self.zpool, self.filesystem, self.zpool, trash_timestamp, self.filesystem)).split(" "))
        yield self._run(ZFS, *("set mountpoint=/hcfs-trash/%s-%s %s/hcfs-trash/%s-%s" % (
            trash_timestamp, self.filesystem, self.zpool, trash_timestamp, self.filesystem)).split(" "))

        # Un-trash the filesystem
        yield self._run(ZFS, *("set mountpoint=/hcfs/%s %s/hcfs-trash/%s-%s" % (
            self.filesystem, self.zpool, trash_timestamp, self.filesystem)).split(" "))
        yield self._run(ZFS, *("rename %s/hcfs-trash/%s-%s %s/hcfs/%s" % (
            trash_timestamp, self.filesystem, self.zpool, self.filesystem, self.zpool)).split(" "))



class PruneSnapshots(BaseLoad):
    tag = 'zfs-prune-test'
    random = random.Random(x=1)

    @inlineCallbacks
    def _newSnapshot(self):
        yield self._create_snapshot(self.filesystem, str(self.snapshotCounter))
        yield self._create_changes(self.filesystem)
        self.snapshots.append(self.snapshotCounter)
        self.snaphotCounter += 1

    @inlineCallbacks
    def start(self):
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
        yield self._destroy_snapshot(target)
        self.snapshots.remove(target)


