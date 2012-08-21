import os, time, shlex

def run_return(cmd):
    import commands
    status, output = commands.getstatusoutput(cmd)
    return output.split('\n'), status

JAIL_NAME = 'testjail4'
ZPOOL = 'hpool'
ZFS = '%s/%s' % (ZPOOL, JAIL_NAME)

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

    for fs in ['/jails', '/jails/basejail', '/jails/%s' % (name,)]:
        unified = ZPOOL + fs; fsmount = '/usr' + fs
        if not check_zfs(unified):
            print "Creating", unified
            run_return("zfs create %(unified)s; zfs set mountpoint=%(fsmount)s %(unified)s" % 
                    dict(unified=unified, fsmount=fsmount))
        else:
            print unified, "already exists"

    print "Initialising basejail"
    os.system("ezjail-admin update")
    os.system("ezjail-admin create -r /usr/jails/%s %s 127.0.1.1" % (name, name))



def start_jail(name):
    os.system("/usr/local/etc/rc.d/ezjail.sh onestart %s" % (name,))
    jails, status = run_return("jls -h -q")
    for info in jails:
        parts = shlex.split(info)
        if parts[8] == name:
            return parts[5]
    raise Exception("Could not determine jail id")



def stop_jail(name):
    # XXX Who knows how this works?
    # os.system("/usr/local/etc/rc.d/ezjail.sh onestop %s" % (name,))
    pass



if __name__ == "__main__":
    initial_setup()
