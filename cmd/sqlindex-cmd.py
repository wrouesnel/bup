#!/usr/bin/env python

import sys, stat, time, os, errno, re
from bup import sql, metadata, options, git, drecurse, hlinkdb, index
from bup.helpers import *
from bup.hashsplit import GIT_MODE_TREE, GIT_MODE_FILE

def update_index(top, excluded_paths, exclude_rxs):
    # tmax and start must be epoch nanoseconds.
    tmax = (time.time() - 1) * 10**9
    tstart = int(time.time()) * 10**9
    curIndex = sql.Index(indexfile + '.db')

    hashgen = None
    if opt.fake_valid:
        def hashgen(name):
            return (GIT_MODE_FILE, index.FAKE_SHA)

    total = 0
    bup_dir = os.path.abspath(git.repo())
    index_start = time.time()
    for (path,pst) in drecurse.recursive_dirlist([top], xdev=opt.xdev,
                                                 bup_dir=bup_dir,
                                                 excluded_paths=excluded_paths,
                                                 exclude_rxs=exclude_rxs):
        if opt.verbose>=2 or (opt.verbose==1 and stat.S_ISDIR(pst.st_mode)):
            sys.stdout.write('%s\n' % path)
            sys.stdout.flush()
            elapsed = time.time() - index_start
            paths_per_sec = total / elapsed if elapsed else 0
            qprogress('Indexing: %d (%d paths/s)\r' % (total, paths_per_sec))
        elif not (total % 128):
            elapsed = time.time() - index_start
            paths_per_sec = total / elapsed if elapsed else 0
            qprogress('Indexing: %d (%d paths/s)\r' % (total, paths_per_sec))
        total += 1
        try:
            meta = metadata.from_path(path, statinfo=pst)
        except (OSError, IOError), e:
            add_error(e)
            continue
        # See same assignment to 0, above, for rationale.
        meta.atime = meta.mtime = meta.ctime = 0
        curIndex.add(path, pst, meta, hashgen = hashgen)

    elapsed = time.time() - index_start
    paths_per_sec = total / elapsed if elapsed else 0
    progress('Indexing: %d, done (%d paths/s).\n' % (total, paths_per_sec))

optspec = """
bup index <-p|m|s|u> [options...] <filenames...>
--
 Modes:
p,print    print the index entries for the given names (also works with -u)
m,modified print only added/deleted/modified files (implies -p)
s,status   print each filename with a status char (A/M/D) (implies -p)
u,update   recursively update the index entries for the given file/dir names (default if no mode is specified)
check      carefully check index file integrity
clear      clear the default index
 Options:
H,hash     print the hash for each object next to its name
l,long     print more information about each file
no-check-device don't invalidate an entry if the containing device changes
fake-valid mark all index entries as up-to-date even if they aren't
fake-invalid mark all index entries as invalid
f,indexfile=  the name of the index file (normally BUP_DIR/bupindex)
exclude= a path to exclude from the backup (may be repeated)
exclude-from= skip --exclude paths in file (may be repeated)
exclude-rx= skip paths matching the unanchored regex (may be repeated)
exclude-rx-from= skip --exclude-rx patterns in file (may be repeated)
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
        opt.clear):
    opt.update = 1
if (opt.fake_valid or opt.fake_invalid) and not opt.update:
    o.fatal('--fake-{in,}valid are meaningless without -u')
if opt.fake_valid and opt.fake_invalid:
    o.fatal('--fake-valid is incompatible with --fake-invalid')
if opt.clear and opt.indexfile:
    o.fatal('cannot clear an external index (via -f)')

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
    sql.Index(indexfile).check()

if opt.clear:
    log('clear: clearing index.\n')
    sql.Index(indexfile).clear()

excluded_paths = parse_excludes(flags, o.fatal)
exclude_rxs = parse_rx_excludes(flags, o.fatal)
paths = index.reduce_paths(extra)

if opt.update:
    if not extra:
        o.fatal('update mode (-u) requested but no paths given')
    for (rp,path) in paths:
        update_index(rp, excluded_paths, exclude_rxs)

if opt['print'] or opt.status or opt.modified:
    pass
#     for (name, ent) in index.Reader(indexfile).filter(extra or ['']):
#         if (opt.modified 
#             and (ent.is_valid() or ent.is_deleted() or not ent.mode)):
#             continue
#         line = ''
#         if opt.status:
#             if ent.is_deleted():
#                 line += 'D '
#             elif not ent.is_valid():
#                 if ent.sha == index.EMPTY_SHA:
#                     line += 'A '
#                 else:
#                     line += 'M '
#             else:
#                 line += '  '
#         if opt.hash:
#             line += ent.sha.encode('hex') + ' '
#         if opt.long:
#             line += "%7s %7s " % (oct(ent.mode), oct(ent.gitmode))
#         print line + (name or './')

if opt.check and (opt['print'] or opt.status or opt.modified or opt.update):
    log('check: starting final check.\n')
    sql.Index(indexfile).check()

if saved_errors:
    log('WARNING: %d errors encountered.\n' % len(saved_errors))
    sys.exit(1)
