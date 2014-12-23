#!/usr/bin/env python
import sys
from bup import git, vfs, ls, client
from bup.helpers import *


cli = client.Client(os.environ['BUP_DIR'])
top = vfs.RefList(cli, None)

# Check out lib/bup/ls.py for the opt spec
ret = ls.do_ls(sys.argv[1:], top, default='/', spec_prefix='bup ')
sys.exit(ret)
