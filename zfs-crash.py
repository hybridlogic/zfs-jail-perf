
# send - filesystem, snapshots
# recv - same filesystem
# recv - not-exist
# rollback - filesystem, snapshots
# rename - filesystem
# snapshot - filesystem
# destroy a snapshot - filesystem, snapshot

# zfs list -t filesystem -r -H -o name,used hpool/hcfs
# /sbin/zfs list -H -o name -r hpool/hcfs/site-slinq.com

# A => B
# - snapshot A
# - clone B from A's snapshot
# - promote B to parent of A
# - destroy A

from twisted.internet import reactor

NOT_EXIST = object()
EXIST = object()

def create(fsName):
    """
    Create a new zfs filesystem.  Return a Deferred which fires when
    creation is complete.
    """
    return getProcessOutput(["zfs", "create", "-o", "version=4", fsName])


def snapshot(fsName, snapshotName):
    """
    Take a new snapshot of the given filesystem.  Give the snapshot
    the given name.
    """
    return getProcessOutput(["zfs", "snapshot", "%s@%s" % (fsName, snapshotName)])


def destroy(fsName, snapshotName):
    """
    Destroy the snapshot with the given name on the given filesystem.
    """
    return getProcessOutput(["zfs", "destroy", "%s@%s" % (fsName, snapshotName)])


def _initialize_for_read(tmpfile):
    fObj = open(tmpfile, 'w')
    fObj.write(b''.join(chr(i) for i in range(255)) * 255)
    fObj.close()


@inlineCallbacks
def workPatternA(fsName):
    yield create(fsName)
    try:
        for i in range(10):
            _initialize_for_read(...)
            yield snapshot(fsName, str(i))
            yield destroy(fsName, str(i))
        yield snapshot(fsName, "a")
        yield send(fsName, None, "a", FilePath("temp path"))
        yield recv(fsName + "-received", FilePath("temp path"))
        yield moreThings()
    finally:
        pass # Delete everything that got created

def main():
    def thrash():
        workPatterns = itertools.cycle([workPatternA])
        fsCounter = 0
        while True:
            d = workPatterns.next()("zfs-crash-%d" % (fsCounter,))
            d.addBoth(log.msg, "workPatternA %d complete" % (fsCounter,))
            yield d

    work = thrash()
    for i in range(concurrency):
        cooperate(work).whenDone().addBoth(log.msg, "Thrash %d cooperate complete" % (i,))
    reactor.run()

    
def send():
    pass

def recv():
    pass

def rollback():
    pass

def rename():
    pass
