"""
Client abstraction library for bup.

All public facing interfaces deal in binary types - calls down to the git
layer need to encode for the command line.
"""

import re, struct, errno, time, zlib
from bup import git, ssh
from bup import vint
from bup.protocol import *
from bup.helpers import *

bwlimit = None


class ClientError(Exception):
    pass


def _raw_write_bwlimit(f, buf, bwcount, bwtime):
    if not bwlimit:
        f.write(buf)
        return (len(buf), time.time())
    else:
        # We want to write in reasonably large blocks, but not so large that
        # they're likely to overflow a router's queue.  So our bwlimit timing
        # has to be pretty granular.  Also, if it takes too long from one
        # transmit to the next, we can't just make up for lost time to bring
        # the average back up to bwlimit - that will risk overflowing the
        # outbound queue, which defeats the purpose.  So if we fall behind
        # by more than one block delay, we shouldn't ever try to catch up.
        for i in xrange(0,len(buf),4096):
            now = time.time()
            next = max(now, bwtime + 1.0*bwcount/bwlimit)
            time.sleep(next-now)
            sub = buf[i:i+4096]
            f.write(sub)
            bwcount = len(sub)  # might be less than 4096
            bwtime = next
        return (bwcount, bwtime)


def parse_remote(remote):
    protocol = r'([a-z]+)://'
    host = r'(?P<sb>\[)?((?(sb)[0-9a-f:]+|[^:/]+))(?(sb)\])'
    port = r'(?::(\d+))?'
    path = r'(/.*)?'
    url_match = re.match(
            '%s(?:%s%s)?%s' % (protocol, host, port, path), remote, re.I)
    if url_match:
        if not url_match.group(1) in ('ssh', 'bup', 'file'):
            raise ClientError, 'unexpected protocol: %s' % url_match.group(1)
        return url_match.group(1,3,4,5)
    else:
        rs = remote.split(':', 1)
        if len(rs) == 1 or rs[0] in ('', '-'):
            return 'file', None, None, rs[-1]
        else:
            return 'ssh', rs[0], None, rs[1]

class Client:
    """default bup client implementation for access to a local repository"""
    def __init__(self, bup_repo = None, create=False):
        self._busy = None
        
        self.bup_repo = bup_repo    # Keep track of bup repo path.
        
        if create:
            git.init_repo(bup_repo)
        else:
            git.check_repo_or_die(bup_repo)

    def check_ok(self):
        """verify a client action completed successfully. For local clients
        this will always return true, since exceptions are allowed to
        propagate.
        """
        return True

    def check_busy(self):
        if self._busy:
            raise ClientError('already busy with command %r' % self._busy)
        
    def ensure_busy(self):
        if not self._busy:
            raise ClientError('expected to be busy, but not busy?!')
        
    def _not_busy(self):
        self._busy = None

    def new_packwriter(self, compression_level = 1):
        self.check_busy()
        return git.PackWriter(compression_level = compression_level)

    def read_ref(self, refname):
        self.check_busy()
        r = git.read_ref(refname)
        self.check_ok() # FIXME: is there any reason a local client needs this?
        return r

    def update_ref(self, refname, newval, oldval):
        self.check_busy()
        git.update_ref(refname, newval, oldval)
        self.check_ok()

    # Former VFS helper functions. These are needed to calculate
    # blob sizes. They are translated to client-call format.
    # They do not need to be implemented in a remote client.
    def _treeget(self, hash):
        it = self.get(hash)
        type = it.next()
        assert(type == 'tree')
        return git.tree_decode(''.join(it))
    
    def _tree_decode(self, hash):
        tree = [(int(name,16),stat.S_ISDIR(mode),sha)
                for (mode,name,sha)
                in self._treeget(hash)]
        assert(tree == list(sorted(tree)))
        return tree
    
    def _chunk_len(self, hash):
        return sum(len(b) for b in self.cat(hash('hex')))
        
    def _last_chunk_info(self, hash):
        tree = self._tree_decode(hash)
        assert(tree)
        (ofs,isdir,sha) = tree[-1]
        if isdir:
            (subofs, sublen) = self._last_chunk_info(sha)
            return (ofs+subofs, sublen)
        else:
            return (ofs, self._chunk_len(sha))

    def _total_size(self, hash):
        (lastofs, lastsize) = self._last_chunk_info(hash)
        return lastofs + lastsize

    # TODO: implement a caching mechanism for file sizes at the repository
    # level. SQLite db maybe?
    def size(self, hash):
        """get the size in bytes of the object pointed to by hash in the
        git repository."""
        self.check_busy()
        it = self.get(hash)
        objtype = it.next()
        assert(objtype == 'tree' or objtype == 'blob')
        # Must handle trees/blobs differently
        if objtype == 'tree':
            return self._total_size(hash)
        elif objtype == 'blob':
            b = ''.join(it)
            return len(b)

    def get(self, id):
        """read the item pointed to by id. yields a string indicating type
        and then object data."""
        self.check_busy()
        self._busy = 'get'
        for d in git.cp().get(id.encode('hex')):
            yield d
        e = self.check_ok()
        self._not_busy()
        if e:
            raise KeyError(str(e))

    def cat(self, id):
        """cat dumps all reachable blobs. use get if you just want exactly
        one blob."""
        self.check_busy()
        self._busy = 'cat'
        for blob in git.cp().join(id.encode('hex')):
            yield blob
        e = self.check_ok()
        self._not_busy()
        if e:
            raise KeyError(str(e))
    
    def list_refs(self, refname = None):
        """git.list_refs over bup-server shell."""
        self.check_busy()
        self._busy = 'list-refs'
        if refname:
            self.conn.write('list-refs {0}\n'.format(refname))
        else:
            self.conn.write('list-refs\n')
        while 1:
            name = vint.read_bvec(self.conn)
            if len(name) == 0:
                break   # empty line indicates EOF
            sha = readpackedhash(self.conn)
            yield (name,sha)
        self.check_ok()
        self._not_busy()
    
    def rev_list(self, ref, count=None):
        """git.rev_list over bup-server shell"""
        self.check_busy()
        self._busy = 'rev-list'
        if count:
            self.conn.write('rev-list {0} {1}\n'.format(ref, count))
        else:
            self.conn.write('rev-list {0}\n'.format(ref))
        while 1:
            commit = vint.read_bvec(self.conn)
            if len(commit) == 0:
                break   # empty vec == eof.
            date = vint.read_vuint(self.conn)
            yield (date,commit)
        self.check_ok()
        self._not_busy()
        
    def rev_parse(self, committish):
        """git.rev_parse over bup-server shell"""
        self.check_busy()
        self._busy = 'rev-parse'
        self.conn.write('rev-parse {0}'.format(committish))
        hash = vint.read_bvec(self.conn)
        result = None
        if len(hash) != 0:
            result = hash
        self.check_ok()
        self._not_busy()
        return result
    
    def tags(self):
        """git.tags implementation over bup-server shell."""
        tags = {}
        for (n,c) in self.list_refs():
            if n.startswith('refs/tags/'):
                name = n[10:]
                if not c in tags:
                    tags[c] = []
                tags[c].append(name)  # more than one tag can point at 'c'
        return tags

class RemoteClient(Client):
    """client for remote access to a bup repository via bup-server"""
    def __init__(self, remote, create=False):
        self._busy = self.conn = None
        self.sock = self.p = self.pout = self.pin = None
        is_reverse = os.environ.get('BUP_SERVER_REVERSE')
        if is_reverse:
            assert(not remote)
            remote = '%s:' % is_reverse
        (self.protocol, self.host, self.port, self.dir) = parse_remote(remote)
        self.cachedir = git.repo('index-cache/%s'
                                 % re.sub(r'[^@\w]', '_', 
                                          "%s:%s" % (self.host, self.dir)))
        if is_reverse:
            self.pout = os.fdopen(3, 'rb')
            self.pin = os.fdopen(4, 'wb')
            self.conn = Conn(self.pout, self.pin)
        else:
            if self.protocol in ('ssh', 'file'):
                try:
                    # FIXME: ssh and file shouldn't use the same module
                    self.p = ssh.connect(self.host, self.port, 'server')
                    self.pout = self.p.stdout
                    self.pin = self.p.stdin
                    self.conn = Conn(self.pout, self.pin)
                except OSError, e:
                    raise ClientError, 'connect: %s' % e, sys.exc_info()[2]
            elif self.protocol == 'bup':
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.host, atoi(self.port) or 1982))
                self.sockw = self.sock.makefile('wb')
                self.conn = DemuxConn(self.sock.fileno(), self.sockw)
        if self.dir:
            self.dir = re.sub(r'[\r\n]', ' ', self.dir)
            if create:
                self.conn.write('init-dir %s\n' % self.dir)
            else:
                self.conn.write('set-dir %s\n' % self.dir)
            self.check_ok()
        self.sync_indexes()

    def __del__(self):
        try:
            self.close()
        except IOError, e:
            if e.errno == errno.EPIPE:
                pass
            else:
                raise

    def close(self):
        if self.conn and not self._busy:
            self.conn.write('quit\n')
        if self.pin:
            self.pin.close()
        if self.sock and self.sockw:
            self.sockw.close()
            self.sock.shutdown(socket.SHUT_WR)
        if self.conn:
            self.conn.close()
        if self.pout:
            self.pout.close()
        if self.sock:
            self.sock.close()
        if self.p:
            self.p.wait()
            rv = self.p.wait()
            if rv:
                raise ClientError('server tunnel returned exit code %d' % rv)
        self.conn = None
        self.sock = self.p = self.pin = self.pout = None

    def check_ok(self):
        if self.p:
            rv = self.p.poll()
            if rv != None:
                raise ClientError('server exited unexpectedly with code %r'
                                  % rv)
        try:
            return self.conn.check_ok()
        except Exception, e:
            raise ClientError, e, sys.exc_info()[2]

    def check_busy(self):
        if self._busy:
            raise ClientError('already busy with command %r' % self._busy)
        
    def ensure_busy(self):
        if not self._busy:
            raise ClientError('expected to be busy, but not busy?!')
        
    def _not_busy(self):
        self._busy = None

    def sync_indexes(self):
        self.check_busy()
        conn = self.conn
        mkdirp(self.cachedir)
        # All cached idxs are extra until proven otherwise
        extra = set()
        for f in os.listdir(self.cachedir):
            debug1('%s\n' % f)
            if f.endswith('.idx'):
                extra.add(f)
        needed = set()
        conn.write('list-indexes\n')
        for line in linereader(conn):
            if not line:
                break
            assert(line.find('/') < 0)
            parts = line.split(' ')
            idx = parts[0]
            if len(parts) == 2 and parts[1] == 'load' and idx not in extra:
                # If the server requests that we load an idx and we don't
                # already have a copy of it, it is needed
                needed.add(idx)
            # Any idx that the server has heard of is proven not extra
            extra.discard(idx)

        self.check_ok()
        debug1('client: removing extra indexes: %s\n' % extra)
        for idx in extra:
            os.unlink(os.path.join(self.cachedir, idx))
        debug1('client: server requested load of: %s\n' % needed)
        for idx in needed:
            self.sync_index(idx)
        git.auto_midx(self.cachedir)

    def sync_index(self, name):
        #debug1('requesting %r\n' % name)
        self.check_busy()
        mkdirp(self.cachedir)
        fn = os.path.join(self.cachedir, name)
        if os.path.exists(fn):
            msg = "won't request existing .idx, try `bup bloom --check %s`" % fn
            raise ClientError(msg)
        self.conn.write('send-index %s\n' % name)
        n = struct.unpack('!I', self.conn.read(4))[0]
        assert(n)
        with atomically_replaced_file(fn, 'w') as f:
            count = 0
            progress('Receiving index from server: %d/%d\r' % (count, n))
            for b in chunkyreader(self.conn, n):
                f.write(b)
                count += len(b)
                qprogress('Receiving index from server: %d/%d\r' % (count, n))
            progress('Receiving index from server: %d/%d, done.\n' % (count, n))
            self.check_ok()

    def _make_objcache(self):
        return git.PackIdxList(self.cachedir)

    def _suggest_packs(self):
        ob = self._busy
        if ob:
            assert(ob == 'receive-objects-v2')
            self.conn.write('\xff\xff\xff\xff')  # suspend receive-objects-v2
        suggested = []
        for line in linereader(self.conn):
            if not line:
                break
            debug2('%s\n' % line)
            if line.startswith('index '):
                idx = line[6:]
                debug1('client: received index suggestion: %s\n'
                       % git.shorten_hash(idx))
                suggested.append(idx)
            else:
                assert(line.endswith('.idx'))
                debug1('client: completed writing pack, idx: %s\n'
                       % git.shorten_hash(line))
                suggested.append(line)
        self.check_ok()
        if ob:
            self._busy = None
        idx = None
        for idx in suggested:
            self.sync_index(idx)
        git.auto_midx(self.cachedir)
        if ob:
            self._busy = ob
            self.conn.write('%s\n' % ob)
        return idx

    def new_packwriter(self, compression_level = 1):
        self.check_busy()
        def _set_busy():
            self._busy = 'receive-objects-v2'
            self.conn.write('receive-objects-v2\n')
        return PackWriter_Remote(self.conn,
                                 objcache_maker = self._make_objcache,
                                 suggest_packs = self._suggest_packs,
                                 onopen = _set_busy,
                                 onclose = self._not_busy,
                                 ensure_busy = self.ensure_busy,
                                 compression_level = compression_level)

    def read_ref(self, refname):
        self.check_busy()
        self.conn.write('read-ref %s\n' % refname)
        r = self.conn.readline().strip()
        self.check_ok()
        if r:
            assert(len(r) == 40)   # hexified sha
            return r.decode('hex')
        else:
            return None   # nonexistent ref

    def update_ref(self, refname, newval, oldval):
        self.check_busy()
        self.conn.write('update-ref %s\n%s\n%s\n' 
                        % (refname, newval.encode('hex'),
                           (oldval or '').encode('hex')))
        self.check_ok()

    def cat(self, id):
        self.check_busy()
        self._busy = 'cat'
        self.conn.write('cat %s\n' % re.sub(r'[\n\r]', '_', id))
        while 1:
            sz = struct.unpack('!I', self.conn.read(4))[0]
            if not sz: break
            yield self.conn.read(sz)
        e = self.check_ok()
        self._not_busy()
        if e:
            raise KeyError(str(e))
    
    def list_refs(self, refname = None):
        """git.list_refs over bup-server shell."""
        self.check_busy()
        self._busy = 'list-refs'
        if refname:
            self.conn.write('list-refs {0}\n'.format(refname))
        else:
            self.conn.write('list-refs\n')
        while 1:
            name = vint.read_bvec(self.conn)
            if len(name) == 0:
                break   # empty line indicates EOF
            sha = readpackedhash(self.conn)
            yield (name,sha)
        self.check_ok()
        self._not_busy()
    
    def rev_list(self, ref, count=None):
        """git.rev_list over bup-server shell"""
        self.check_busy()
        self._busy = 'rev-list'
        if count:
            self.conn.write('rev-list {0} {1}\n'.format(ref, count))
        else:
            self.conn.write('rev-list {0}\n'.format(ref))
        while 1:
            commit = vint.read_bvec(self.conn)
            if len(commit) == 0:
                break   # empty vec == eof.
            date = vint.read_vuint(self.conn)
            yield (date,commit)
        self.check_ok()
        self._not_busy()
        
    def rev_parse(self, committish):
        """git.rev_parse over bup-server shell"""
        self.check_busy()
        self._busy = 'rev-parse'
        self.conn.write('rev-parse {0}'.format(committish))
        hash = vint.read_bvec(self.conn)
        result = None
        if len(hash) != 0:
            result = hash
        self.check_ok()
        self._not_busy()
        return result
    
    def tags(self):
        """git.tags implementation over bup-server shell."""
        tags = {}
        for (n,c) in self.list_refs():
            if n.startswith('refs/tags/'):
                name = n[10:]
                if not c in tags:
                    tags[c] = []
                tags[c].append(name)  # more than one tag can point at 'c'
        return tags


class PackWriter_Remote(git.PackWriter):
    def __init__(self, conn, objcache_maker, suggest_packs,
                 onopen, onclose,
                 ensure_busy,
                 compression_level=1):
        git.PackWriter.__init__(self, objcache_maker)
        self.file = conn
        self.filename = 'remote socket'
        self.suggest_packs = suggest_packs
        self.onopen = onopen
        self.onclose = onclose
        self.ensure_busy = ensure_busy
        self._packopen = False
        self._bwcount = 0
        self._bwtime = time.time()

    def _open(self):
        if not self._packopen:
            self.onopen()
            self._packopen = True

    def _end(self):
        if self._packopen and self.file:
            self.file.write('\0\0\0\0')
            self._packopen = False
            self.onclose() # Unbusy
            self.objcache = None
            return self.suggest_packs() # Returns last idx received

    def close(self):
        id = self._end()
        self.file = None
        return id

    def abort(self):
        raise ClientError("don't know how to abort remote pack writing")

    def _raw_write(self, datalist, sha):
        assert(self.file)
        if not self._packopen:
            self._open()
        self.ensure_busy()
        data = ''.join(datalist)
        assert(data)
        assert(sha)
        crc = zlib.crc32(data) & 0xffffffff
        outbuf = ''.join((struct.pack('!I', len(data) + 20 + 4),
                          sha,
                          struct.pack('!I', crc),
                          data))
        try:
            (self._bwcount, self._bwtime) = _raw_write_bwlimit(
                    self.file, outbuf, self._bwcount, self._bwtime)
        except IOError, e:
            raise ClientError, e, sys.exc_info()[2]
        self.outbytes += len(data)
        self.count += 1

        if self.file.has_input():
            self.suggest_packs()
            self.objcache.refresh()

        return sha, crc
