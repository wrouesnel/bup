#!/usr/bin/env python

import sys, stat, time, os, errno, re
from bup import metadata, options, git, index, drecurse, hlinkdb
from bup import xstat
from bup.helpers import *
from bup.hashsplit import GIT_MODE_TREE, GIT_MODE_FILE

class IterHelper:
    def __init__(self, l):
        self.i = iter(l)
        self.cur = None
        self.next()

    def next(self):
        try:
            self.cur = self.i.next()
        except StopIteration:
            self.cur = None
        return self.cur


def check_index(reader):
    try:
        log('check: checking forward iteration...\n')
        e = None
        d = {}
        for e in reader.forward_iter():
            if e.children_n:
                if opt.verbose:
                    log('%08x+%-4d %r\n' % (e.children_ofs, e.children_n,
                                            e.name))
                assert(e.children_ofs)
                assert(e.name.endswith('/'))
                assert(not d.get(e.children_ofs))
                d[e.children_ofs] = 1
            if e.flags & index.IX_HASHVALID:
                assert(e.sha != index.EMPTY_SHA)
                assert(e.gitmode)
        assert(not e or e.name == '/')  # last entry is *always* /
        log('check: checking normal iteration...\n')
        last = None
        for e in reader:
            if last:
                assert(last > e.name)
            last = e.name
    except:
        log('index error! at %r\n' % e)
        raise
    log('check: passed.\n')

# try and convert real path to bup path based on graft_points
# note: expects absolute paths to be in graft_points
def graftpath(graft_points, path):
    for (oldpath, newpath) in graft_points:
        assert(os.path.isabs(oldpath))
        assert(os.path.isabs(newpath))
        
        if path.startswith(oldpath):
            pathsuffix = path[len(oldpath):]
            # eat preceding sep
            if pathsuffix[:1] == os.path.sep:
                pathsuffix = pathsuffix[1:]
            result = os.path.join(newpath, pathsuffix)
            return result
    return path

# try convert bup path to real path based on graft points
# note: expects absolute paths to be in graft_points
def ungraftpath(graft_points, path):
    for (oldpath, newpath) in graft_points:
        assert(os.path.isabs(oldpath))
        assert(os.path.isabs(newpath))
        
        if path.startswith(newpath):
            pathsuffix = path[len(newpath):]
            # eat preceding sep
            if pathsuffix[:1] == os.path.sep:
                pathsuffix = pathsuffix[1:]
            result = os.path.join(oldpath, pathsuffix)
            return result
    return path

def clear_index(indexfile):
    indexfiles = [indexfile, indexfile + '.meta', indexfile + '.hlink']
    for indexfile in indexfiles:
        path = git.repo(indexfile)
        try:
            os.remove(path)
            if opt.verbose:
                log('clear: removed %s\n' % path)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise

def regraft_index(new_graft_points):
    ri = index.Reader(indexfile)
    hlinks = hlinkdb.HLinkDB(indexfile + '.hlink')
    msr = index.MetaStoreReader(indexfile + '.meta')
    msw = index.MetaStoreWriter(indexfile + '.meta')

    tstart = int(time.time()) * 10**9

    bup_dir = os.path.abspath(git.repo())
    total = 0
    
    # Sort graft points by bup archive path in order of length
    new_graft_points.sort(reverse=True, key=lambda graft: graft[1])

    # Iterate on each graft point separately    
    for (fsprefix, bupprefix) in new_graft_points:
        rig = IterHelper(ri.iter(name=bupprefix))
        while rig.cur:
            # Only regraft entries which would be updated anyway. This
            # saves a whole lot of disk IO polling for metadata.
            if rig.cur.flags & index.IX_HASHVALID:
                # FIXME: what do we do about hardlinks ???
                # Ungrafting should give us a new real path
                newrealpath = ungraftpath([(fsprefix,bupprefix)], 
                                          rig.cur.name)
                # Log some progress
                if opt.verbose>=2:
                    sys.stdout.write('%s -> %s\n' % 
                                     (rig.cur.name, newrealpath))
                    sys.stdout.flush()
                    qprogress('Regrafting: %d\r' % total)
                elif not (total % 128):
                    qprogress('Regrafting: %d\r' % total)
                total += 1
                # update HLinkDB
                if not stat.S_ISDIR(rig.cur.mode) and rig.cur.nlink > 1:
                    hlinks.del_path(rig.cur.name)
                if not stat.S_ISDIR(pst.st_mode) and pst.st_nlink > 1:
                    # TODO: should we be using the grafted path?
                    hlinks.add_path(path, pst.st_dev, pst.st_ino)
                
                # Get metadata
                pst = xstat.lstat(newrealpath)
                meta = metadata.from_path(newrealpath, statinfo=pst, 
                                          archive_path=newrealpath)
                # Do the regraft
                meta_ofs = msw.store(meta)
                rig.cur.from_stat(pst, meta_ofs, tstart,
                                  check_device=opt.check_device)
                rig.cur.repack()                
            rig.next()

def update_index(top, excluded_paths, exclude_rxs, new_graft_points):
    # tmax and start must be epoch nanoseconds.
    tmax = (time.time() - 1) * 10**9
    ri = index.Reader(indexfile)
    msw = index.MetaStoreWriter(indexfile + '.meta')
    wi = index.Writer(indexfile, msw, tmax)
 
    tstart = int(time.time()) * 10**9

    hlinks = hlinkdb.HLinkDB(indexfile + '.hlink')

    hashgen = None
    if opt.fake_valid:
        def hashgen(name):
            return (GIT_MODE_FILE, index.FAKE_SHA)

    total = 0
    bup_dir = os.path.abspath(git.repo())
    for top, top_path in tops:
        # graft the real top to bup top
        grafted_top = graftpath(new_graft_points, top)    
          
        rig = IterHelper(ri.iter(name=grafted_top))
        for (path,pst) in drecurse.recursive_dirlist([top], xdev=opt.xdev,
                                                     bup_dir=bup_dir,
                                                     excluded_paths=excluded_paths,
                                                     exclude_rxs=exclude_rxs):
            # convert path to bup archive form
            grafted_path = graftpath(new_graft_points, path)
            if opt.verbose>=2 or (opt.verbose==1 and stat.S_ISDIR(pst.st_mode)):
                sys.stdout.write('%s -> %s\n' % (path, grafted_path))
                sys.stdout.flush()
                qprogress('Indexing: %d\r' % total)
            elif not (total % 128):
                qprogress('Indexing: %d\r' % total)
            total += 1
            while rig.cur and rig.cur.name > grafted_path:  # deleted paths
                if rig.cur.exists():
                    rig.cur.set_deleted()
                    rig.cur.repack() 
                if rig.cur.nlink > 1 and not stat.S_ISDIR(rig.cur.mode):
                    hlinks.del_path(rig.cur.name)
                rig.next()
            if rig.cur and rig.cur.name == grafted_path:    # paths that already existed
                if not stat.S_ISDIR(rig.cur.mode) and rig.cur.nlink > 1:
                    hlinks.del_path(rig.cur.name)
                if not stat.S_ISDIR(pst.st_mode) and pst.st_nlink > 1:
                    # TODO: should we be using the grafted path?
                    hlinks.add_path(path, pst.st_dev, pst.st_ino)
                meta = metadata.from_path(path, statinfo=pst,
                                          archive_path=path)
                # Clear these so they don't bloat the store -- they're
                # already in the index (since they vary a lot and they're
                # fixed length).  If you've noticed "tmax", you might
                # wonder why it's OK to do this, since that code may
                # adjust (mangle) the index mtime and ctime -- producing
                # fake values which must not end up in a .bupm.  However,
                # it looks like that shouldn't be possible:  (1) When
                # "save" validates the index entry, it always reads the
                # metadata from the filesytem. (2) Metadata is only
                # read/used from the index if hashvalid is true. (3) index
                # always invalidates "faked" entries, because "old != new"
                # in from_stat().
                meta.ctime = meta.mtime = meta.atime = 0
                meta_ofs = msw.store(meta)
                rig.cur.from_stat(pst, meta_ofs, tstart,
                                  check_device=opt.check_device)
                if not (rig.cur.flags & index.IX_HASHVALID):
                    if hashgen:
                        (rig.cur.gitmode, rig.cur.sha) = hashgen(path)
                        rig.cur.flags |= index.IX_HASHVALID
                if opt.fake_invalid:
                    rig.cur.invalidate()
                rig.cur.repack()
                rig.next()
            else:  # new paths
                meta = metadata.from_path(path, statinfo=pst, archive_path=path)
                # See same assignment to 0, above, for rationale.
                meta.atime = meta.mtime = meta.ctime = 0
                meta_ofs = msw.store(meta)
                wi.add(grafted_path, pst, meta_ofs, hashgen = hashgen)
                if not stat.S_ISDIR(pst.st_mode) and pst.st_nlink > 1:
                    # TODO: should we be using the grafted path?
                    hlinks.add_path(path, pst.st_dev, pst.st_ino)

    progress('Indexing: %d, done.\n' % total)
    
    hlinks.prepare_save()

    if ri.exists():
        ri.save()
        wi.flush()
        if wi.count:
            wr = wi.new_reader()
            if opt.check:
                log('check: before merging: oldfile\n')
                check_index(ri)
                log('check: before merging: newfile\n')
                check_index(wr)
            mi = index.Writer(indexfile, msw, tmax)

            for e in index.merge(ri, wr):
                # FIXME: shouldn't we remove deleted entries eventually?  When?
                mi.add_ixentry(e)
            
            ri.close()
            mi.close()
            wr.close()
        wi.abort()
    else:
        wi.close()

    msw.close()
    hlinks.commit_save()


optspec = """
bup index <-p|m|s|u> [options...] <filenames...>
--
 Modes:
p,print    print the index entries for the given names (also works with -u)
m,modified print only added/deleted/modified files (implies -p)
s,status   print each filename with a status char (A/M/D) (implies -p)
u,update   recursively update the index entries for the given file/dir names (default if no mode is specified)
check      carefully check index file integrity
clear      clear the index
regraft    remap modified files real filesystem paths according to new graft points (needs --graft)
 Options:
graft=    a graft point of *old_path*=*new_path* (can be used more then once)
H,hash     print the hash for each object next to its name
l,long     print more information about each file
no-check-device don't invalidate an entry if the containing device changes
fake-valid mark all index entries as up-to-date even if they aren't
fake-invalid mark all index entries as invalid
f,indexfile=  the name of the index file (normally BUP_DIR/bupindex)
exclude=   a path to exclude from the backup (can be used more than once)
exclude-from= a file that contains exclude paths (can be used more than once)
exclude-rx= skip paths that match the unanchored regular expression
v,verbose  increase log output (can be used more than once)
x,xdev,one-file-system  don't cross filesystem boundaries
"""
o = options.Options(optspec)
(opt, flags, extra) = o.parse(sys.argv[1:])

if not (opt.modified or \
        opt['print'] or \
        opt.status or \
        opt.update or \
        opt.check or \
        opt.clear or \
        opt.regraft):
    opt.update = 1
if (opt.fake_valid or opt.fake_invalid) and not opt.update:
    o.fatal('--fake-{in,}valid are meaningless without -u')
if opt.fake_valid and opt.fake_invalid:
    o.fatal('--fake-valid is incompatible with --fake-invalid')
if opt.clear and opt.indexfile:
    o.fatal('cannot clear an external index (via -f)')

if opt.regraft and opt.update:
    o.fatal('--regraft is incompatible with update')
if opt.regraft and extra:
    o.fatal('--regraft does not accept a path')

# FIXME: remove this once we account for timestamp races, i.e. index;
# touch new-file; index.  It's possible for this to happen quickly
# enough that new-file ends up with the same timestamp as the first
# index, and then bup will ignore it.
tick_start = time.time()
time.sleep(1 - (tick_start - int(tick_start)))

git.check_repo_or_die()
indexfile = opt.indexfile or git.repo('bupindex')

handle_ctrl_c()

if opt.check:
    log('check: starting initial check.\n')
    check_index(index.Reader(indexfile))

if opt.clear:
    log('clear: clearing index.\n')
    clear_index(indexfile)

excluded_paths = parse_excludes(flags, o.fatal)
exclude_rxs = parse_rx_excludes(flags, o.fatal)
paths = index.reduce_paths(extra)

graft_points = []
if opt.graft:
    for (option, parameter) in flags:
        if option == "--graft":
            splitted_parameter = parameter.split('=')
            if len(splitted_parameter) != 2:
                o.fatal("a graft point must be of the form old_path=new_path")
            old_path, new_path = splitted_parameter
            if not (old_path and new_path):
                o.fatal("a graft point cannot be empty")
            graft_points.append((realpath(old_path), new_path))

graft_points.sort(reverse=True)

if opt.regraft:
    regraft_index(graft_points)
    
if opt.update:
    if not extra:
        o.fatal('update mode (-u) requested but no paths given')
    update_index(rp, excluded_paths, exclude_rxs, graft_points)

if opt['print'] or opt.status or opt.modified:
    for (name, ent) in index.Reader(indexfile).filter(extra or ['']):
        if (opt.modified 
            and (ent.is_valid() or ent.is_deleted() or not ent.mode)):
            continue
        line = ''
        if opt.status:
            if ent.is_deleted():
                line += 'D '
            elif not ent.is_valid():
                if ent.sha == index.EMPTY_SHA:
                    line += 'A '
                else:
                    line += 'M '
            else:
                line += '  '
        if opt.hash:
            line += ent.sha.encode('hex') + ' '
        if opt.long:
            line += "%7s %7s " % (oct(ent.mode), oct(ent.gitmode))
        print line + (name or './')

if opt.check and (opt['print'] or opt.status or opt.modified or opt.update):
    log('check: starting final check.\n')
    check_index(index.Reader(indexfile))

if saved_errors:
    log('WARNING: %d errors encountered.\n' % len(saved_errors))
    sys.exit(1)
