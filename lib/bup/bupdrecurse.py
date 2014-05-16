"""bup repository drecurse

This is a version of the drecurse.py which operates on the bup VFS layer.

Unlike drecurse, this version returns filenames and whole bup metadata
objects rather then stat info from the filesystem.
"""

import stat
from bup import vfs, metadata
from bup.helpers import *

# similar to the index.reduce_paths function, but intended to operate on
# repository paths. unlike index.reduce_paths, this explicitely deletes
# trailing '/' since in the VFS these cause symlink deferencing.
def reduce_bup_paths(paths):
    prev = None
    outpaths = []
    for path in paths:
        if prev and (prev == path 
                     or (prev.endswith('/') and path.startswith(prev))):
            continue # already superceded by previous path
        outpaths.append(path)
        prev = path
    outpaths.sort(reverse=True)
    return outpaths

def _bup_dirlist(parentnode):
    """
    Sort objects from bup vfs and metadata as normal drecurse does.
    """
    assert(stat.S_ISDIR(parentnode.mode)) # we should only be called on VFS dirs
    l = []
    # Note: drecurse appends a / to directories. But bup already did that,
    # so we don't need to here.
    for n in parentnode:
        meta = n.metadata()
        # if no metadata, replace with blank metadata
        if meta is None:
            meta = metadata.Metadata()
        l.append((n, meta))
    l.sort(key=lambda e: e[0].name ,reverse=True)
    return l

def _bup_recursive_dirlist(parentnode, prefixnode=None, prependnode=None, 
                           excluded_paths=None, exclude_rxs=None):
    """recursive function for bup_drecurse.
    prefix is a node we should remove as the parent.
    prepend is the node which should be added as parent (typically a symlink).
    """
    for (n,meta) in _bup_dirlist(parentnode):
        if stat.S_ISDIR(n.mode):
            for i in _bup_recursive_dirlist(parentnode=n,
                                            prefixnode=prefixnode,
                                            prependnode=prependnode,
                                            excluded_paths=excluded_paths,
                                            exclude_rxs=exclude_rxs):
                yield i
        if prependnode is None:
            meta.path = n.fullname(stop_at=prefixnode)
        else:
            meta.path = prependnode.fullname() + '/' + n.fullname(stop_at=prefixnode)
        yield meta

def bup_recursive_dirlist(path, excluded_paths=None, exclude_rxs=None):
    """recursive_dirlist function that operates on bup repositories rather
    then real filesystems, and supports similar semantics.
    """
    top = vfs.RefList(None)  
    
    start = top.lresolve(path)  # Get the nice-name (i.e. symlink name)
    meta = start.metadata()     # Get metadata
    if meta is None:
        meta = metadata.Metadata()
    meta.path = start.fullname()    # Append path to metadata
    
    # Now get the real object we need to resolve against
    n = start.resolve()
    
    # Recurse into directory 
    if stat.S_ISDIR(n.mode):
        for i in _bup_recursive_dirlist(parentnode=n,
                                        prefixnode=n,
                                        prependnode=start,
                                        excluded_paths=excluded_paths,
                                        exclude_rxs=exclude_rxs):
            yield i
    
    yield meta