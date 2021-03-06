import os
#import time
import shlex

def run_return(cmd):
    import commands
    status, output = commands.getstatusoutput(cmd)
    return output.split('\n'), status

JAIL_NAME = 'testjail4'
ZPOOL = 'hpool'
ZFS = '%s/%s' % (ZPOOL, JAIL_NAME)
FS_MOUNT_BASE = '/usr'

EZJAIL = '/usr/local/etc/rc.d/ezjail.sh'
if not os.path.exists(EZJAIL):
    # New ezjail compat
    EZJAIL = '/usr/local/etc/rc.d/ezjail'

def check_zfs(fsname):
    lines, status = run_return("zfs list %s" % (fsname,))
    return not lines[0].startswith('cannot open')

def initial_setup(name):
    print "Please run ezjail-admin -b if you've not built a world yet...",
    print "proceeding in 5 seconds, press Ctrl-C to cancel."
    #time.sleep(5) # TODO put me back
    print "Checking that hpool exists"
    if not check_zfs(ZPOOL):
        print "ZFS pool", ZPOOL, "doesn't exist, bailing."
        return

    for fs in ['/hcfs', '/jails', '/jails/basejail', '/jails/%s' % (name,)]:
        unified = ZPOOL + fs; fsmount = FS_MOUNT_BASE + fs
        if not check_zfs(unified):
            print "Creating", unified
            run_return("zfs create %(unified)s; zfs set mountpoint=%(fsmount)s %(unified)s" % 
                    dict(unified=unified, fsmount=fsmount))
        else:
            print unified, "already exists"

    print "Initialising basejail"
    def s(cmd):
        print "Running", cmd
        os.system(cmd)
    s("ezjail-admin update")
    s("ezjail-admin create -r /usr/jails/%s %s 127.0.1.1" % (name, name))
    s("cp python26.tbz /usr/jails/%s/" % (name,))



def start_jail(name):
    os.system("%s onestart %s" % (EZJAIL, name))
    jails, status = run_return("jls -h -q")
    for info in jails:
        parts = shlex.split(info)
        if parts[23] == name:
            return parts[5]
        # FreeBSD 9.1 version.
        if parts[28] == name:
            return parts[6]
    raise Exception("Could not determine jail id")



def stop_jail(name):
    # XXX Who knows how this works?
    # os.system("/usr/local/etc/rc.d/ezjail.sh onestop %s" % (name,))
    pass



if __name__ == "__main__":
    initial_setup()
