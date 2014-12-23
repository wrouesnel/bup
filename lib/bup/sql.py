'''
bup SQL resources
'''

BUP_INDEX='bupindex.db'

# SQL definition for local table index
INDEX_DB = """
CREATE TABLE hashes (
    fsid      INTEGER NOT NULL,
    offset    INTEGER NOT NULL,
    length    INTEGER NOT NULL,
    hash      BLOB,
    PRIMARY KEY (hash)
);

CREATE TABLE meta (
    metaid    INTEGER NOT NULL,
    metablob    BLOB,
    PRIMARY KEY (metaid)
);

CREATE TABLE stat (
    statid  INTEGER NOT NULL,
    
    dev        INTEGER,
    ino        INTEGER,
    mode       INTEGER,
    nlink      INTEGER,
    uid        INTEGER,
    gid        INTEGER,
    rdev       INTEGER,
    size       INTEGER,
    blksize    INTEGER,
    blocks     INTEGER,
    atime      INTEGER,
    atime_ns   INTEGER,
    mtime      INTEGER,
    mtime_ns   INTEGER,
    ctime      INTEGER,
    ctime_ns   INTEGER,
    PRIMARY KEY (statid)
);

CREATE TABLE filesystem (
    fsid    INTEGER NOT NULL,
    name    BLOB,
    gitmode INTEGER NOT NULL,
    hash    BLOB,
    statid  INTEGER,
    metaid  INTEGER,
    exists  INTEGER,
    hashvalid  INTEGER,
    shamissing INTEGER,
    PRIMARY KEY (fsid)
);

CREATE TABLE tree (
    ancestor    INTEGER NOT NULL,
    descendent  INTEGER NOT NULL,
    PRIMARY KEY (ancestor,descendent)
);
"""

def insert_entry(parent, child, stat):
    pass

def delete_entry(e):
    pass

