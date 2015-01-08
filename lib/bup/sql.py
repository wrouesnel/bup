'''
bup SQL resources

We implement the closure table pattern to represent file paths.

All filenames are stored as sqlite3 blob objects and encoded as binary to 
maximize compatibility.
'''

import sys
import os
import sqlite3

import metadata, os, stat, struct, tempfile
from bup import xstat, git
from bup.helpers import *

# Constants
VERSION = 1

FAKE_SHA = '\x01'*20

# SQL definition for local table index
# FIXME: what indexes do we need? Should we break this up to be easier to
# code with?
INDEX_DB = """
CREATE TABLE hashes (
    fsid      INTEGER NOT NULL,
    offset    INTEGER NOT NULL,
    length    INTEGER NOT NULL,
    hash      BLOB PRIMARY KEY
);

CREATE TABLE meta (
    metaid    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    metablob  BLOB UNIQUE
);

CREATE TABLE stat (
    fsid       INTEGER NOT NULL PRIMARY KEY,
    
    dev        INTEGER,
    ino        INTEGER,
    mode       INTEGER,
    nlink      INTEGER,
    uid        INTEGER,
    gid        INTEGER,
    rdev       INTEGER,
    size       INTEGER,
    atime      INTEGER,
    atime_ns   INTEGER,
    mtime      INTEGER,
    mtime_ns   INTEGER,
    ctime      INTEGER,
    ctime_ns   INTEGER
);

CREATE TABLE filesystem (
    fsid    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    ancestor_fsid INTEGER,
    name    BLOB NOT NULL,
    gitmode INTEGER,
    hash    BLOB,
    metaid  INTEGER,
    fs_exists INTEGER,
    hashvalid   INTEGER,
    shamissing  INTEGER
);

PRAGMA journal_mode=WAL;
PRAGMA wal_autocheckpoint=0;

PRAGMA user_version=%i;
""" % VERSION

class IndexException(Exception):
    pass

class CorruptIndex(IndexException):
    pass

class SchemaMismatch(IndexException):
    pass

class IndexDataException(IndexException):
    pass

class PathNotFound(IndexDataException):
    pass

class Index:
    def __init__(self, dbpath):
        self.dbpath = dbpath
        
        self.db = None
        
        if not os.path.exists(self.dbpath):
            self._create_database()
        
        self.db = sqlite3.connect(dbpath)
        self.db.row_factory = sqlite3.Row
        
        # Check schema version
        self._check_schema()
        
        self.cur = self.db.cursor()
        
        # Database caching variables
        # It is not efficient to hit the disk on every filepath, and sqlite
        # works best with bulk queries.
        self.pathcache = {} # Stores path->FSID map to speed inserts
        self.lastpath = None # Stores last inserted path, to maintain database integrity
        self.metacache = {} # Stores meta SHA1/metaid mappings to avoid database lookups
        
    def __del__(self):
        self.db.commit()
        self.db.close()

    def _check_schema(self):
        '''verify the database schema is as we expect it to be'''
        result = self.db.execute('PRAGMA user_version;').fetchone()
        if result['user_version'] != VERSION:
            raise SchemaMismatch('index schema mismatch', 
                                 result['user_version'], VERSION) 

    def _create_database(self):
        '''create and initialize a new schema'''
        db = sqlite3.connect(self.dbpath)
        db.row_factory = sqlite3.Row
        
        # Check we connected to a blank DB
        result = db.execute('PRAGMA user_version;').fetchone()
        if result['user_version'] != 0:
            log('database created before we could get to it.')
            db.close()
            return
        
        # The database is empty, try and create it. This should not fail.
        db.executescript(INDEX_DB)
        db.commit()
        db.close()

    def _insert_or_replace_fs_stat(self, fsid, st):
        """maps a stat struct to the stat table format for a given fsid"""
        atime = xstat.nsecs_to_timespec(st.st_atime)
        mtime = xstat.nsecs_to_timespec(st.st_mtime)
        ctime = xstat.nsecs_to_timespec(st.st_ctime)
        
        self.cur.execute('''INSERT OR REPLACE INTO stat 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);''',
        (fsid, st.st_dev, st.st_ino, st.st_mode, st.st_nlink, 
        st.st_uid, st.st_gid, st.st_rdev, st.st_size, 
         atime[0], atime[1], mtime[0], mtime[1], ctime[0], ctime[1]))
    
    def _insert_or_ignore_fs_meta(self, meta):
        """deduplicates encoded FS metadata objects into the database.
        data is deduplicated by storing an in-memory table of hashes
        to meta ID mappings.
        """
        # TODO: maintain dict size somehow
        metadata = meta.encode()
        metasha = Sha1(metadata)
        metablob = sqlite3.Binary(meta.encode())
        
        # Lookup sha first
        metaid = self.metacache.get(metasha, None)
        if metaid is None:
            # Query and insert
            self.cur.execute('''SELECT metaid FROM meta 
            WHERE metablob=?''', (metablob,))
            r = self.cur.fetchall()
            assert(len(r) <= 1)
            # If this metadata exists, deduplicate it.
            if len(r) == 0:
                self.cur.execute('''INSERT INTO meta (metablob)
                VALUES (?)''', (metablob,))
                metaid = self.cur.lastrowid
            else:
                metaid = r[0]['metaid']
            # Update in-memory cache
            self.metacache[metasha] = metaid
        
        assert(metaid is not None)
        return metaid

    def __iter__(self):
        """default iterator method - akin to iterating over root"""
        return self.reader()

    def reader(self,root='/'):
        """iterates over index entries starting from root.
        Returns SQL dictionary objects"""
        
        iroot = slashremove(root) 
        
        # Get the root node's fsid
        self.cur.execute("""
        WITH RECURSIVE subtree(fsid,path) AS (
            VALUES (?,'')
            UNION ALL
            SELECT filesystem.fsid, subtree.path || '/' || filesystem.name FROM filesystem, subtree
            WHERE filesystem.ancestor_fsid=subtree.fsid
        )
        SELECT fsid FROM subtree
        WHERE path=?;
        """, (iroot,))
        r = self.cur.fetchone()
        if r is None:
            raise PathNotFound('the requested path was not found', root)
        root_fsid = r['fsid']
        
        # Run the real query
        self.cur.execute("""
        WITH RECURSIVE subtree(fsid,path) AS (
            VALUES (?,'')
            UNION ALL
            SELECT filesystem.fsid, subtree.path || '/' || filesystem.name FROM filesystem, subtree
            WHERE filesystem.ancestor_fsid=subtree.fsid
        )
        SELECT  subtree.path,
                stat.atime, stat.atime_ns,
                stat.mtime, stat.mtime_ns, 
                stat.ctime, stat.ctime_ns,
                meta.metablob 
        FROM subtree
        INNER JOIN filesystem ON subtree.fsid=filesystem.fsid
        LEFT JOIN stat ON filesystem.fsid=stat.fsid
        LEFT JOIN meta ON filesystem.metaid=meta.metaid
        """, root_fsid)
        
        yield self.cur.fetchone()

    def add(self, path, pst, meta=None, hashgen=None):
        """add a path to the index"""

        # Ensure we're dealing with an absolute path
        rp = realpath(path)
        
        # Break into components
        pc = path_components(rp)

        ancestor = 0 # fsid we want to start from should always be 0
        
        # Keep the path cache size manageable by pruning entries.        
        self.pathcache = { key:self.pathcache[key] for key in (v[1] for v in pc) if key in self.pathcache }
        
        # Find the longest path we have cached
        highest_idx = len(pc) # start at end
        for name,path in reversed(pc):
            if path in self.pathcache:
                ancestor = self.pathcache[path]
                break
            highest_idx -= 1
        
#       # This is way too slow to ever do - but we can post-merge to eliminate
        # duplicates.
#         # If we need to descend in path depth by any amount, then we can't
#         # reliably know there isn't a name collision at the bottom somewhere.
#         for idx in range(highest_idx, len(pc)-1):
#             name = pc[idx][0]
#               
#             self.cur.execute('''
#             SELECT tree.fsid FROM tree
#             INNER JOIN filesystem ON tree.fsid = filesystem.fsid
#             WHERE filesystem.name = ? AND ancestor = ?;''',
#             (sqlite3.Binary(name), ancestor))
#               
#             highest_idx = idx
#               
#             rows = self.cur.fetchall()
#             if len(rows) == 0:
#                 break
#             elif len(rows) == 1:
#                 ancestor = rows[0]['fsid']
#                 self.pathcache[pc[idx][1]] = ancestor
#             else:
#                 raise CorruptIndex('got more then 1 identical name in same directory')
         
        # Did we reach the bottom of the tree?
        if highest_idx == len(pc)-1:
            update = True   # Yes - do an update and not an insert
        
        # Convert supplied metadata to SQL friendly forms
        hashvalid = False
        if hashgen:
            (gitmode, sha) = hashgen(name)
            hashvalid = True
        else:
            (gitmode, sha) = (0, None)
        
        shamissing = False  # Should we set shamissing always?
        exists = False if pst is None else True
        sha_blob = None if sha is None else sqlite3.Binary(sha)
        
        # If we have blob metadata, deduplicate and store it.
        metaid = None
        if meta:
            metaid = self._insert_or_ignore_fs_meta(meta)
        
        # Create all the tree we need to have.
        for idx in range(highest_idx, len(pc)):
            name_blob = buffer(pc[idx][0])
            
            # Create the filesystem item
            self.cur.execute('''INSERT INTO filesystem 
            (name,gitmode,fs_exists,hashvalid,shamissing,hash,metaid,ancestor_fsid) 
            VALUES (?,?,?,?,?,?,?,?);''', 
            (name_blob,gitmode,exists,hashvalid,shamissing,
             sha_blob,metaid,ancestor))
            
            descendant = self.cur.lastrowid  # Get the FSID
            
            # Set new ancestor object
            ancestor = descendant
            self.pathcache[pc[idx][1]] = ancestor
        
        # Add additional metadata
        self._insert_or_replace_fs_stat(ancestor, pst)
        
        return ancestor # Return the fsid for other users
    
    def clear(self):
        '''drop all data from the database'''
        # FIXME: Should we truncate?
        self.db.close() # Close current connection
        os.unlink(self.dbpath) # Destroy old db
        self._create_database() # Recreate a new blank db
        
    def check(self):
        '''check the database integrity'''
        r = self.db.execute('PRAGMA integrity_check;')
        for row in r.fetchall():
            log('%s' % (row[0]))
            
        r = self.db.execute('PRAGMA foreign_key_check;')
        for row in r.fetchall():
            log('%s' % (row[0]))