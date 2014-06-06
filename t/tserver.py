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
        