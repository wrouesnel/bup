# TODO: vfs sure could use some test coverage.

import os, sys, tempfile, subprocess
from bup import vfs, hashsplit, git
from wvtest import *

top_dir = '../../..'
bup_tmp = os.path.realpath('../../../t/tmp')
bup_path = top_dir + '/bup'
start_dir = os.getcwd()

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

@wvtest
def test_get_file_shalist():
    try:
        os.mkdir(bup_tmp)
    except Exception:
        pass
    tmpdir = tempfile.mkdtemp(dir=bup_tmp, prefix='bup-tvfs-')
    bup_dir = tmpdir + '/bup'   # bup repo
    data_path = tmpdir + '/foo' # data files we make for this
    os.mkdir(data_path)

    # Make some random data files
    with open(os.path.join(data_path,'testfile1'), 'wb') as fout:
        fout.write(os.urandom(1024))
        fout.flush()
    with open(os.path.join(data_path,'testfile2'), 'wb') as fout:
        fout.write('\0' * 1024)
        fout.flush()

    # Save them into the repositoryeclipse window
    ex(bup_path, '-d', bup_dir, 'init')
    ex(bup_path, '-d', bup_dir, 'index', '-v', data_path)
    ex(bup_path, '-d', bup_dir, 'save', 
       '--name' , 'test', '-tvvn', 'test', data_path)
    
    # Hashsplit the data files manually    python c
    file1sha = vfs.local_shalist((os.path.join(data_path,'testfile1')))
    file1sha = [ (ofs,sha) for (ofs,sha,size) in file1sha  ]    
    
    # compare hash to vfs hash return
    git.check_repo_or_die(bup_dir)
    testfile1_bup = '/test/latest' + data_path + '/testfile1'
    n = vfs.RefList(None).lresolve(testfile1_bup)
    
    blob = n.open().read(1024)
    
    file1sha_bup = n.hashlist()
    
    WVPASSEQ(file1sha, file1sha_bup)
    
    # Clean up
    subprocess.call(['rm', '-rf', tmpdir])