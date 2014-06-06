#!/usr/bin/env python
import os, sys, struct, shlex
import tarfile
import stat
from bup import options, git, metadata, vfs, xstat
from bup.protocol import *
from bup.helpers import *

suspended_w = None
dumb_server_mode = False

def do_help(conn, junk):
    conn.write('Commands:\n    %s\n' % '\n    '.join(sorted(commands)))
    conn.ok()

def _set_mode():
    global dumb_server_mode
    dumb_server_mode = os.path.exists(git.repo('bup-dumb-server'))
    debug1('bup server: serving in %s mode\n' 
           % (dumb_server_mode and 'dumb' or 'smart'))

def _init_session(reinit_with_new_repopath=None):
    if reinit_with_new_repopath is None and git.repodir:
        return
    git.check_repo_or_die(reinit_with_new_repopath)
    # OK. we now know the path is a proper repository. Record this path in the
    # environment so that subprocesses inherit it and know where to operate.
    os.environ['BUP_DIR'] = git.repodir
    debug1('bup server: bupdir is %r\n' % git.repodir)
    _set_mode()


def init_dir(conn, arg):
    if len(arg) > 1:
        raise Exception("init-dir takes only 1 argument. %i given.\n" % len(arg))
    path = arg[0]
    git.init_repo(path)
    debug1('bup server: bupdir initialized: %r\n' % git.repodir)
    _init_session(path)
    conn.ok()


def set_dir(conn, arg):
    if len(arg) > 1:
        raise Exception("set-dir takes only 1 argument. %i given.\n" % len(arg))
    path = arg[0]
    _init_session(path)
    conn.ok()

    
def list_indexes(conn, junk):
    _init_session()
    suffix = ''
    if dumb_server_mode:
        suffix = ' load'
    for f in os.listdir(git.repo('objects/pack')):
        if f.endswith('.idx'):
            conn.write('%s%s\n' % (f, suffix))
    conn.ok()


def send_index(conn, arg):
    if len(arg) > 1:
        raise Exception("send-index takes only 1 argument. %i given.\n" % len(name))
    _init_session()
    name = arg[0]
    assert(name.find('/') < 0)
    assert(name.endswith('.idx'))
    idx = git.open_idx(git.repo('objects/pack/%s' % name))
    conn.write(struct.pack('!I', len(idx.map)))
    conn.write(idx.map)
    conn.ok()


def receive_objects_v2(conn, junk):
    global suspended_w
    _init_session()
    suggested = set()
    if suspended_w:
        w = suspended_w
        suspended_w = None
    else:
        if dumb_server_mode:
            w = git.PackWriter(objcache_maker=None)
        else:
            w = git.PackWriter()
    while 1:
        ns = conn.read(4)
        if not ns:
            w.abort()
            raise Exception('object read: expected length header, got EOF\n')
        n = struct.unpack('!I', ns)[0]
        #debug2('expecting %d bytes\n' % n)
        if not n:
            debug1('bup server: received %d object%s.\n' 
                % (w.count, w.count!=1 and "s" or ''))
            fullpath = w.close(run_midx=not dumb_server_mode)
            if fullpath:
                (dir, name) = os.path.split(fullpath)
                conn.write('%s.idx\n' % name)
            conn.ok()
            return
        elif n == 0xffffffff:
            debug2('bup server: receive-objects suspended.\n')
            suspended_w = w
            conn.ok()
            return
            
        shar = conn.read(20)
        crcr = struct.unpack('!I', conn.read(4))[0]
        n -= 20 + 4
        buf = conn.read(n)  # object sizes in bup are reasonably small
        #debug2('read %d bytes\n' % n)
        _check(w, n, len(buf), 'object read: expected %d bytes, got %d\n')
        if not dumb_server_mode:
            oldpack = w.exists(shar, want_source=True)
            if oldpack:
                assert(not oldpack == True)
                assert(oldpack.endswith('.idx'))
                (dir,name) = os.path.split(oldpack)
                if not (name in suggested):
                    debug1("bup server: suggesting index %s\n"
                           % git.shorten_hash(name))
                    debug1("bup server:   because of object %s\n"
                           % shar.encode('hex'))
                    conn.write('index %s\n' % name)
                    suggested.add(name)
                continue
        nw, crc = w._raw_write((buf,), sha=shar)
        _check(w, crcr, crc, 'object read: expected crc %d, got %d\n')
    # NOTREACHED

def _restore_files_tarpipe(conn, req):
    """restores files by encoding them into a tar
    """
    tarmode = "w|" + req.transfermode[3:]
    tar = tarfile.open(mode=tarmode, fileobj=conn, dereference=False)
    
    top = vfs.RefList(None)   # get top of backup tree
    start = top.resolve(req.bup_path)
    
    # get the node iterator for the rest of the restore operation
    node_iter = vfs.restore_files_iter(conn, start, None)
    
    # iterate over all nodes and write them as tar blocks
    for n in node_iter:
        # skip directories, since we write full paths to tar files
        if stat.S_ISDIR(n.mode):
            continue
        
        # Manufacture tar metadata (note: must use fullname to get good results)
        info = tarfile.TarInfo(name=n.fullname(stop_at=n.fs_top()))
        info.size = n.size()    # expensive but necessary!
        
        # populate metadata 
        meta = n.metadata()
        if meta:
            info.mode = meta.mode
            info.mtime = xstat.nsecs_to_timespec(meta.mtime)[0]
            info.ctime = xstat.nsecs_to_timespec(meta.ctime)[0]
            info.atime = xstat.nsecs_to_timespec(meta.atime)[0]
            info.uid = meta.uid
            info.gid = meta.gid
            info.uname = meta.user
            info.gname = meta.group
        else:
            info.mode = n.mode
        
        if stat.S_ISREG(meta.mode):
            info.type = tarfile.REGTYPE
        elif stat.S_ISLNK(meta.mode):
            info.type = tarfile.SYMTYPE
        elif stat.S_ISCHR(meta.mode):
            info.type = tarfile.CHRTYPE
        elif stat.S_ISBLK(meta.mode):
            info.type = tarfile.BLKTYPE
        elif stat.S_ISFIFO(meta.mode):
            info.type = tarfile.FIFOTYPE
        
        f = n.open()
        # handle links separately since they're a bit weird in bup
        if stat.S_ISLNK(meta.mode):
            info.linkname = f.read(info.size)
            tar.addfile(info, None)
        else:
            tar.addfile(info, f)
        f.close()
    tar.close()

def restore_files(conn, extra):
    """restore-files mode is used for requesting files from a remote bup
    repository.
    """
    # Server enters restore-files mode
    _init_session()

    # Get session data
    req = RestoreSessionRequest.read(conn, extra)

    if req.transfermode.startswith('tar'):
        # we require quiet mode so reading a tar file straight off can be
        # accomplished.
        quiet_mode(conn, 'true')
        _restore_files_tarpipe(conn, req)
        # clean exit - we need to close the connection once we're done.
        # this enables a script to read the tar file directly off the 
        # pipe.
        conn.outp.close()
        conn.close()
        raise NormalExit()
    else:
        raise SessionException('invalid transfer mode for restore files not caught')

def quiet_mode(conn, arg):
    """sets the server to suppress the conn.ok() calls. Useful for
    talking to the server with non-bup instrumentation. """
    arg = ''.join(arg).lower()
    if arg == 'on' or arg == 'true':
        conn.quiet = True
    elif arg == 'off' or arg == 'false':
        conn.quiet = False
    else:
        raise Exception('quiet-mode: invalid arguments')
    conn.ok()

def _check(w, expected, actual, msg):
    if expected != actual:
        w.abort()
        raise Exception(msg % (expected, actual))


def read_ref(conn, arg):
    if len(arg) > 1:
        raise Exception("read_ref takes only 1 argument. %i given.\n" % len(arg))
    refname = arg[0]
    _init_session()
    r = git.read_ref(refname)
    conn.write('%s\n' % (r or '').encode('hex'))
    conn.ok()


def update_ref(conn, arg):
    if len(arg) > 1:
        raise Exception("update_ref takes only 1 argument. %i given.\n" % len(arg))
    refname = arg[0]
    _init_session()
    newval = conn.readline().strip()
    oldval = conn.readline().strip()
    git.update_ref(refname, newval.decode('hex'), oldval.decode('hex'))
    conn.ok()


cat_pipe = None
def cat(conn, arg):
    if len(arg) > 1:
        raise Exception("cat takes only 1 argument. %i given.\n" % len(arg))
    id = arg[0]
    global cat_pipe
    _init_session()
    if not cat_pipe:
        cat_pipe = git.CatPipe()
    try:
        for blob in cat_pipe.join(id):
            conn.write(struct.pack('!I', len(blob)))
            conn.write(blob)
    except KeyError, e:
        log('server: error: %s\n' % e)
        conn.write('\0\0\0\0')
        conn.error(e)
    else:
        conn.write('\0\0\0\0')
        conn.ok()

optspec = """
bup server
"""
o = options.Options(optspec)
(opt, flags, extra) = o.parse(sys.argv[1:])

if extra:
    o.fatal('no arguments expected')

debug2('bup server: reading from stdin.\n')

commands = {
    'quit': None,
    'help': do_help,
    'init-dir': init_dir,
    'set-dir': set_dir,
    'list-indexes': list_indexes,
    'send-index': send_index,
    'receive-objects-v2': receive_objects_v2,
    'restore-files': restore_files,
    'quiet-mode': quiet_mode,
    'read-ref': read_ref,
    'update-ref': update_ref,
    'cat': cat,
}

# FIXME: this protocol is totally lame and not at all future-proof.
# (Especially since we abort completely as soon as *anything* bad happens)
conn = Conn(sys.stdin, sys.stdout)
lr = linereader(conn)
for _line in lr:
    line = _line.strip()
    if not line:
        continue
    debug1('bup server: command: %r\n' % line)
    words = shlex.split(line)
    cmd = words[0]
    rest = words[1:]
    if cmd == 'quit':
        break
    else:
        cmd = commands.get(cmd)
        if cmd:
            try:
                cmd(conn, rest)
            except NormalExit:
                break
        else:
            raise Exception('unknown server command: %r\n' % line)

debug1('bup server: done\n')
