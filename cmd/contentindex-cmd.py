#!/usr/bin/env python

import sys, stat, time, os, errno, re
from bup import metadata, options, git, index, drecurse
from bup.helpers import *
from bup.hashsplit import GIT_MODE_TREE, GIT_MODE_FILE



optspec = """
bup contentindex <filenames...>
--
"""
o = options.Options(optspec)
(opt, flags, extra) = o.parse(sys.argv[1:])



sys.exit(0)
