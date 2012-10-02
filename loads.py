# rename - filesystem
class RenameFilesystemLoad(object):
    def __init__(self, root, zpool, benchmarkFilesystem):
        """
        @param root: A string giving the root path of the zpool to benchmark in.

        @param zpool: The zfs pool name to benchmark in.

        @param benchmarkFilesystem: The name of the filesystem being benchmarked.
        """


    def start(self):
        # Runs in reactor thread.  Return a Deferred that fires when
        # load is started.  Load runs until stop is called.


    def stop(self):
        # Runs in reactor thread.  Return a Deferred that fires when
        # load is stopped.
    

