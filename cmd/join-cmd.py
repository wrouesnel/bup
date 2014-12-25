#!/usr/bin/env python
import sys
import os
from bup import git, options, client
from bup.helpers import *

optspec = """
bup join [-r host:path] [refs or hashes...]
--
r,remote=  remote repository path
o=         output filename
"""
o = options.Options(optspec)
(opt, flags, extra) = o.parse(sys.argv[1:])

git.check_repo_or_die()

if not extra:
    extra = linereader(sys.stdin)

ret = 0

if opt.remote:
    cli = client.RemoteClient(opt.remote)
else:
    cli = client.Client(os.environ['BUP_DIR'])

if opt.o:
    outfile = open(opt.o, 'wb')
else:
    outfile = sys.stdout

for id in extra:
    # The client functions don't understand non-SHAs, but some things assume
    # they can use committish style references. rev_parse is provided to
    # solve this problem.
    sha = cli.rev_parse(id)
    try:
        for blob in cli.cat(sha):
            outfile.write(blob)
    except KeyError, e:
        outfile.flush()
        log('error: %s\n' % e)
        ret = 1

sys.exit(ret)
