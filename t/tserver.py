# Unit testing for bup-server

import os, sys, tempfile, subprocess, struct, threading, tarfile

# configure path to libraries for test
libpath = os.path.join(os.path.dirname(__file__), "../lib")
os.environ['PYTHONPATH'] = libpath + ':' + os.environ.get('PYTHONPATH', '')
sys.path[:0] = [libpath]

from collections import deque
from bup import vfs, hashsplit, git, metadata, vint, protocol
from bup.protocol import *
from bup.helpers import *
from wvtest import *

def bcmp(f1, f2, size):
    for i in range(size):
        b1 = f1.read(1)
        b2 = f2.read(1)
        if b1 != b2:
            return False
    return True

def check_ok(conn):
    for rl in linereader(conn):
        if not rl:
            continue
        if rl == 'ok':
            break
        if rl.startswith('error '):
            WVFAIL(rl.startswith('error '))

# Shell command runner for test functions
def ex(*cmd):
    try:
        cmd_str = ' '.join(cmd)
        print >> sys.stderr, cmd_str
        rc = subprocess.call(cmd)
        if rc < 0:
            print >> sys.stderr, 'terminated by signal', - rc
            sys.exit(1)
        elif rc > 0:
            print >> sys.stderr, 'returned exit status', rc
            sys.exit(1)
    except OSError, e:
        print >> sys.stderr, 'subprocess call failed:', e
        sys.exit(1)

top_dir = '..'  # root of src dir
bup_tmp = os.path.realpath('tmp')   # path to testdata tmp dir
bup_path = top_dir + '/bup' # path to bup executable
start_dir = os.getcwd() # directory we start in (the t/ dir)

# Setup the sample data repository for other tests in this file
try:
    os.mkdir(bup_tmp)
except Exception:
    pass
tmpdir = tempfile.mkdtemp(dir=bup_tmp, prefix='bup-tserver-')
bup_dir = tmpdir + '/bup'   # bup repo
data_path = top_dir + '/t/sampledata'

# Save them into the repository
test_backup_path = '/test/latest'   # path of the test backup we use
ex(bup_path, '-d', bup_dir, 'init')
ex(bup_path, '-d', bup_dir, 'index', '-v', data_path)
ex(bup_path, '-d', bup_dir, 'save', 
   '--name' , 'test', '-tvvn', 'test', data_path)

def verify_file(pmeta, hashlist, blob_iter):
    """Verifies a file being proposed for restore against the local copy.
    """
    sys.stdout.write(pmeta.path + ' ')
    
    # path exists?
    if not os.path.lexists(pmeta.path):
        print "does not exist in filesystem!"
        return False
    
    if pmeta.meta.isdir():
        # verify dir exists
        if os.path.isdir(pmeta.path):
            # directory verified
            sys.stdout.write('\n')
            return True

    if not stat.S_ISREG(pmeta.meta.mode):
        # not a directory and not a regular file. probably a symlink.
        # TODO: what happens to hardlinks?
        linktarget = next(blob_iter)
        # TODO: somehow we should check this? but it's a VFS issue not an os
        # one.
        # link verified
        sys.stdout.write('\n')
        return True
    
    # Verify file contents
    f = open(pmeta.path, "rb")
    
    # verify that the hashes match the disk file, and the returned file
    # matches the disk file
    for (ofs, hash) in hashlist:
        # get the blob
        blob = blob_iter.next()
        blob_len = len(blob)
        
        # get the local blob
        localblob = f.read(blob_len)
        
        # hash both using our hash function
        blobhash = git.calc_hash('blob', blob)
        localblobhash = git.calc_hash('blob', localblob)
        
        # blobs should be equal
        if blob != localblob:
            print "CONTENT mismatch"
            WVFAIL(True)
            
        # hashes should be equal
        if blobhash != localblobhash != hash:
            print "hash mismatch"
            WVFAIL(True)   
    
    # iterator should be depleted
    try:
        notablob = blob_iter.next()
    except StopIteration:
        sys.stdout.write('\n')
        return True
    
    print "more blobs then hashlist entries!"
    return False

@wvtest
def test_restore_files_bup():
    """
    Manual, dummy implementation of bup-server restore-files functionality.
    Emulates various aspects of sending/receiving files to check the
    underlying logic.
    """
    
    # Start a bup-server
    server = subprocess.Popen([bup_path, 'server'],
                              stdin = subprocess.PIPE,
                              stdout = subprocess.PIPE)
    # remember, stdout is coming into us so it's the connections INPUT
    conn = Conn(server.stdout, server.stdin) 
    
    conn.write('set-dir %s\n' % bup_dir) # connect to the new repo
    check_ok(conn)
    
    sessionreq = protocol.RestoreSessionRequest(test_backup_path)
    sessionreq.write(conn) # start session
    
    pathstack = PathStack('/')

    # Check that we can send receive data 1 file at a time.
    RestoreHeader(RestoreHeader.H_METADATA).write(conn)
    PipelineRequest([(0,1)]).write(conn)
    
    pmeta = None
    hashlist = None    
    
    while True:
        serverheader = RestoreHeader.read(conn)
        # break on finish
        if serverheader.type == RestoreHeader.H_FINISHED:
            RestoreHeader(RestoreHeader.H_FINISHED).write(conn)
            break
        elif serverheader.type == RestoreHeader.H_BLOBS:
                verify_file(pmeta, hashlist, Blobs.read(conn))
                RestoreHeader(RestoreHeader.H_METADATA).write(conn)
                PipelineRequest([(0,1)]).write(conn)
        elif serverheader.type == RestoreHeader.H_HASHLIST:
            for h in Hashlists.read_hashlists_iter(conn):
                RestoreHeader(RestoreHeader.H_BLOBS).write(conn)
                PipelineRequest([(0,len(h))]).write(conn)
                hashlist = h
                
        elif serverheader.type == RestoreHeader.H_METADATA:
            size = vint.read_vuint(conn)
            for i in range(size):
                pmeta = ProtocolMetadata.read(conn, pathstack)
                if not pmeta.meta.isdir():
                    RestoreHeader(RestoreHeader.H_HASHLIST).write(conn)
                    PipelineRequest([(0,1)]).write(conn)
                else:
                    RestoreHeader(RestoreHeader.H_METADATA).write(conn)
                    PipelineRequest([(0,1)]).write(conn)
    
    # finish up by getting hashes and blobs for remaining items in the pipeline
    
    WVMSG("Test 'bup' transfermode...")
    
    conn.write('quit\n')    # close server connection
    thread = threading.Thread(target=lambda: server.wait())
    thread.start()
    thread.join(1)
    if thread.is_alive():
        server.terminate()  # force kill
        WVFAIL("forced thread shutdown")

@wvtest
def test_restore_files_tar():  
    # Start a bup-server
    server = subprocess.Popen([bup_path, 'server'],
                              stdin = subprocess.PIPE,
                              stdout = subprocess.PIPE)
    # remember, stdout is coming into us so it's the connections INPUT
    conn = Conn(server.stdout, server.stdin) 
    
    conn.write('quiet-mode on\n')
    conn.write('set-dir %s\n' % bup_dir) # connect to the new repo
    
    sessionreq = protocol.RestoreSessionRequest(test_backup_path, 'tar')
    sessionreq.write(conn) # start session
    
    # session goes silent. At this point we should be reading tarfile data
    tar = tarfile.open(mode='r|', fileobj=conn)
    while True:
        m = tar.next()
        if m is None:
            break
        print m.name
        if m.isfile():
            mf = tar.extractfile(m)
            f = open('/' + m.name)
            if not bcmp(mf, f, m.size ):
                WVFAIL(True)
                return
            f.close()
            mf.close()
    tar.close()
    WVMSG("Test 'tar' transfer mode...")
        