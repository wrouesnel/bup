#!/usr/bin/env python
import os, sys, struct, shlex
import tarfile
import stat
import cStringIO
from collections import deque
from bup import options, git, metadata, vfs, xstat, vint, client
from bup.protocol import *
from bup.helpers import *

suspended_w = None
dumb_server_mode = False
# TODO: derive a server object which can express protocol versions
cli = None  # The server simply wraps a client.

def do_help(conn, junk):
    conn.write('Commands:\n    %s\n' % '\n    '.join(sorted(commands)))
    conn.ok()

def _set_mode():
    global dumb_server_mode
    dumb_server_mode = os.path.exists(git.repo('bup-dumb-server'))
    debug1('bup server: serving in %s mode\n' 
           % (dumb_server_mode and 'dumb' or 'smart'))

def _init_session(reinit_with_new_repopath=None):
    global cli
    if reinit_with_new_repopath is None and cli is not None:
        return
    cli = client.Client(reinit_with_new_repopath)
    debug1('bup server: bupdir is %r\n' % cli.bup_repo)
    _set_mode()

def init_dir(conn, arg):
    global cli
    if len(arg) > 1:
        raise Exception("init-dir takes only 1 argument. %i given.\n" % len(arg))
    path = arg[0]
    # This is a bit of legacy. But _init_session will fallthrough correctly.
    cli = client.Client(path, create=True)
    debug1('bup server: bupdir initialized: %r\n' % cli.bup_repo)
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
    conn.write( '\n'.join(cli.list_indexes()) )
    conn.write( '\n' )
    conn.ok()

def send_index(conn, arg):
    if len(arg) > 1:
        raise Exception("send-index takes only 1 argument. %i given.\n" % len(name))
    _init_session()
    name = arg[0]
    # TODO: Not the nicest object orientation. Fix with abstracted server-class?
    idx = cli.send_index(name)
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
        w = cli.new_packwriter()
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

def _restore_files_blob_request(conn, req, hashpipeline):
    """Process a pipeline request object and send hashlists
    """
    for (start,length) in req:
        for i in range(start):
            self.popleft()  # discard objects being skipped
        # TODO: It would be more efficient to send blob-runs as one big
        # object and lose the header overhead.
        RestoreHeader(RestoreHeader.H_BLOBS).write(conn) # header
        Blobs.create_from_pipeline(hashpipeline, length).write(conn)

def _restore_files_hashlist_request(conn, req, metapipeline, hashpipeline):
    """Process a pipeline request object and send hashlists
    """
    for (start,length) in req:
        for i in range(start):
            self.popleft()  # discard objects being skipped
        
        # TODO: how should we determine how much to do at once?
        RestoreHeader(RestoreHeader.H_HASHLIST).write(conn) # header
        hs = Hashlists.create_from_pipeline(metapipeline, length)
        hs.write(conn, hashpipeline)

def _restore_files_metadata_request(conn, req, 
                                    metapipeline, pathstack, node_iter):
    """Handle normal pipelined metadata requests from a client process.
    """
    # Since we can't actually know we won't run out of metadata to send before
    # completing the metadata request, we buffer up in memory first, then
    # send it. Even then, if there's a lot of buffered up metadata requests
    # then we may send a lot of finished packets.
    send_queue = deque()    # deque's are faster then lists for linear things
    finished = False
    
    # Pop ranges off the request and process them
    try:
        for (start,length) in req:
            # move to start of span
            for i in range(start):
                node_iter.next()
            # iterate over span and make nodes
            for i in range(length):
                node = node_iter.next()
                pmeta = ProtocolMetadata.create_from_node(node, pathstack)
                send_queue.append((node, pmeta))
    except StopIteration:
        finished = True

    RestoreHeader(RestoreHeader.H_METADATA).write(conn) # header
    vint.write_vuint(conn, len(send_queue)) # size
    
    for (node,pmeta) in send_queue: # LIFO
        pmeta.write(conn)   # write protocol metadata
        # Only pipeline if object can return a hashlist (dirs cannot)
        if not pmeta.meta.isdir():
            metapipeline.append(node)   # store node in meta pipeline
    
    # Iteration finished during this range
    if finished:
        RestoreHeader(RestoreHeader.H_FINISHED).write(conn)

def _restore_files_bup_transfer(conn, req):
    """restore files using the bup transfer mode
    """
    debug1('bup server: bup transfermode')
    metapipeline = deque()
    hashpipeline = deque()
    pathstack = PathStack()
    
    debug1('bup server: waiting for initial client metadata request\n')
    top = vfs.RefList(cli, None)   # get top of backup tree
    start = top.resolve(req.bup_path)
    
    start.name = '' # we never reuse this node, but we don't want to send the
                    # name either since the client already knows it.

    # get the node iterator for the rest of the restore operation
    node_iter = vfs.restore_files_iter(conn, start, pathstack)
    while True:
        try:
            clientheader = RestoreHeader.read(conn)
        except HeaderException, e:
            raise RestoreException('unknown header type', None, 
                                  inner_exception=e)

        # None data types - immediate action needed
        if clientheader.type == RestoreHeader.H_FAILED:
            debug1('bup server: client error. aborting operation\n')
            raise RestoreException('client reported operation failed.',
                                  clientheader)
        elif clientheader.type == RestoreHeader.H_FINISHED:
            debug1('bup server: client requested finish.')
            break
                
        # requesting types
        req = PipelineRequest.read(conn)
        if clientheader.type == RestoreHeader.H_BLOBS:
            _restore_files_blob_request(conn, req, hashpipeline)
        elif clientheader.type == RestoreHeader.H_HASHLIST:
            _restore_files_hashlist_request(conn, req, metapipeline, 
                                            hashpipeline)
        elif clientheader.type == RestoreHeader.H_METADATA:
            _restore_files_metadata_request(conn, req, metapipeline, 
                                            pathstack, node_iter)

def _restore_files_tarpipe(conn, req):
    """restores files by encoding them into a tar
    """
    tarmode = "w|" + req.transfermode[3:]
    tar = tarfile.open(mode=tarmode, fileobj=conn, dereference=False)
    
    top = vfs.RefList(cli, None)   # get top of backup tree
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
    
    if req.transfermode == 'bup':
        # bup differential transfer mode
        _restore_files_bup_transfer(conn, req)   
    elif req.transfermode.startswith('tar'):
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
def get(conn, arg):
    if len(arg) > 1:
        raise Exception("get takes only 1 argument. %i given.\n" % len(arg))
    id = arg[0]
    global cat_pipe
    _init_session()
    if not cat_pipe:
        cat_pipe = git.CatPipe()
    # get objects are very small, so we can reliably send the entire thing
    # as a single bvec.
    it = cat_pipe.get(id)
    type = it.next()
    dio = cStringIO.StringIO()
    for blob in it:
        dio.write(blob)
    vint.write_bvec(conn, type)
    vint.write_bvec(conn, dio.getvalue())
    dio.close()
    conn.ok()
    
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

def list_refs(conn, refname = None):
    """server wrapper for bup.git.list_refs. writes output as
    bvec and struct packed data, terminates with an empty bvec."""
    _init_session()
    for name, sha in git.list_refs(refname):
        vint.write_bvec(conn, name)
        conn.write(packhash(sha))
    vint.write_bvec(conn, '')
    conn.ok()

def rev_list(conn, extra):
    """server wrapper for bup.git.rev_list. writes output as 
    vint,packedhash pairs"""
    args = extra.split(' ')
    count = None
    ref = None
    if len(args) == 2:
        ref = args[0]
        count = args[1]
    elif len(args) == 1:
        ref = args[0]
    elif len(args) > 2:
        raise Exception("rev-list takes at most 2 arguments. %i given." % len(args))
    else:
        raise Exception("rev-list requires at least 1 argument")
    for date,commit in git.rev_list(ref, count):
        # We reverse the tuple format, since a 0-length bvec then becomes
        # the EOF signal.
        vint.write_bvec(conn, commit)
        vint.write_vuint(conn, date)
    vint.write_bvec(conn, commit)
    conn.ok()

def rev_parse(conn, arg):
    """server wrapper for bup.git.rev_parse. writes output as a bvec,
    0 length for None."""
    result = git.rev_parse(arg)
    if result:
        vint.write_bvec(conn, result)
    else:
        vint.write_bvec(conn, '')
    conn.ok()

def size(conn, arg):
    """server side implementation of total size calculation.
    Returns number of bytes for each hash supplied"""
    values = [ cli.size(hash) for hash in arg ]
    conn.write('\n'.join(values))
    conn.ok()

optspec = """bup server
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
    # bup.git protocol wrappers
    'read-ref': read_ref,
    'update-ref': update_ref,
    'list-refs' : list_refs,
    'rev-list' : rev_parse,
    'cat': cat,
    'total-size' : size,
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
