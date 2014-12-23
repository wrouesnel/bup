"""
the new bup server implementation. This is the companion object to a 
RemoteClient. Functionally, it wraps a Client object.
"""

import re, struct, errno, time, zlib
from bup import git, ssh
from bup import vint
from bup.protocol import *
from bup.helpers import *

class Server():
    """binary protocol server implementation."""
    def __init__(self, conn):
        pass
    
    def start(self):
        """pass control of the connection over to the server"""
        pass