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
    pathhash  BLOB NOT NULL PRIMARY KEY,
    objhash   BLOB NOT NULL,
    offset    INTEGER NOT NULL,
    length    INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE meta (
    metahash  BLOB NOT NULL PRIMARY KEY,
    metablob  BLOB
) WITHOUT ROWID;

CREATE TABLE filesystem (
    name        BLOB NOT NULL,
    ancestor_pathhash BLOB NOT NULL,
    metahash    BLOB,
    
    buphash    BLOB,
    hashvalid   INTEGER,
    gitmode     INTEGER,
    path_exists INTEGER,
    shamissing  INTEGER,
    
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
    ctime_ns   INTEGER,
    
    PRIMARY KEY (ancestor_pathhash, name)
) WITHOUT ROWID;

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
    def __init__(self, dbpath, metacache_max = 16777216):
        """represents an sqlite-based index on disk.
        
        :metacache_max number of bytes of metadata to cache before flushing to disk
        """
        self.dbpath = dbpath
        
        self.db = None

        if not os.path.exists(self.dbpath):
            self._create_database()
        
        self.db = sqlite3.connect(dbpath)
        self.db.row_factory = sqlite3.Row
        
        # Check schema version
        self._check_schema()
        
        self.cur = self.db.cursor()
        self.cur.execute('PRAGMA synchronous = OFF;')
        
        # Database caching variables
        # It is not efficient to hit the disk on every filepath, and sqlite
        # works best with bulk queries.
        self.metacache = {} # Stores meta SHA1/metaid mappings to avoid database lookups 
        self.metacache_max = metacache_max
        self.metacache_cur = 0
        
    def __del__(self):
        self._flush_metacache()
    
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
    
    def _flush_metacache(self):
        """flushes metacache to database"""
        self.cur.executemany('''
        INSERT OR IGNORE INTO meta (metahash,metablob)
        VALUES (?,?)''', self.metacache.iteritems())
        # Drop the cache.
        self.metacache = {}
        self.metacache_cur = 0
            
    def _insert_or_ignore_fs_meta(self, meta):
        """deduplicates encoded FS metadata objects into the database.
        data is deduplicated by storing an in-memory table of hashes
        to meta ID mappings.
        """
        metadata = buffer(meta.encode())
        metasha = buffer(Sha1(metadata).digest())
        
        # Add the data to the memory map
        if metasha not in self.metacache:
            self.metacache[metasha] = metadata
            self.metacache_cur += len(metadata)
        
        # If the metadata cache is full, flush it to the database
        if self.metacache_cur >= self.metacache_max:
            self._flush_metacache()
        
        # Return the hash value
        return metasha

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
        
        # Break into components
        pc = path_components(path)
        name = pc[-1][0]
        ancestor_pathhash = None
        if len(pc) > 1:
            ancestor_pathhash = Sha1(pc[-2][1]).digest()
        
        # Convert supplied metadata to SQL friendly forms
        hashvalid = False
        if hashgen:
            (gitmode, buphash) = hashgen(name)
            hashvalid = True
        else:
            (gitmode, buphash) = (0, None)
        
        shamissing = False  # Should we set shamissing always?
        exists = False if pst is None else True
        
        # If we have blob metadata, deduplicate and store it.
        metahash = None
        if meta:
            metahash = self._insert_or_ignore_fs_meta(meta)
        
        atime = xstat.nsecs_to_timespec(pst.st_atime)
        mtime = xstat.nsecs_to_timespec(pst.st_mtime)
        ctime = xstat.nsecs_to_timespec(pst.st_ctime)
        
        fs_tuple = (
                    buffer(name), 
                    buffer(ancestor_pathhash), 
                    buffer(metahash) if metahash else None, 
                    buffer(buphash) if buphash else None, 
                    hashvalid, 
                    gitmode, 
                    exists, 
                    shamissing,
                    pst.st_dev, 
                    pst.st_ino, 
                    pst.st_mode, 
                    pst.st_nlink, 
                    pst.st_uid, 
                    pst.st_gid, 
                    pst.st_rdev, 
                    pst.st_size, 
                    atime[0], 
                    atime[1], 
                    mtime[0], 
                    mtime[1], 
                    ctime[0], 
                    ctime[1]
                    )

        self.cur.execute('''INSERT OR REPLACE INTO filesystem 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);''', 
        fs_tuple)
        
        return
    
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