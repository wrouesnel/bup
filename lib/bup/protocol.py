"""Shared data defining bup's on-the-wire protocol

protocol.py defines the various constants and parameters which bup protocol
communication uses.
"""

import re
from collections import deque
from bup import options

class SessionException(Exception):
    """Exception raised from session request setup.
    """
    pass

class NormalExit(Exception):
    """This crime is raised by the tarpipe to terminate the server process
    after it closes the connection.
    """
    pass

class RestoreSessionRequest:
    """Encapsulates the commands needed to start a restore session. These
    are the text commands which are sent before bup-server enters the
    binary mode of a command.
    
    This object is uncached - there is no explicit response expected and
    it is only sent once per session.
    """
    
    # tuple of valid transfer modes
    TRANSFERMODES = ('tar', 'targz', 'tarbz2')
    
    def __init__(self, bup_path, transfermode='bup'):
        # path to restore
        if (len(bup_path) == 0):
            raise SessionException("0-length restore session request.")
        
        self.bup_path = bup_path
        
        # transfer mode
        self.transfermode = transfermode
    
    @staticmethod
    def read(port, extra):
        # process command line options
        optspec = """
restore-files </branch/revision/path/to/dir>
--
 Options:
transfermode= select bup transfer mode. options are tar, targz, tarbz2.
"""
        o = options.Options(optspec)
        (opt, flags, extra) = o.parse(extra)
        
        if len(extra) > 1:
            raise SessionException("cannot handle more then 1 restore path")
        
        result = RestoreSessionRequest(extra[0]) # get the bup path
        
        # transfermode
        if opt.transfermode in RestoreSessionRequest.TRANSFERMODES:
            result.transfermode = opt.transfermode
        else:
            raise SessionException("invalid transfer mode specified")
                    
        return result
    
    def write(self, port):
        port.write('restore-files --transfermode=%s ' % self.transfermode)
        
        # remember to write the path!
        port.write(self.bup_path + '\n')
        
        # at this point the session is up. turn control back to caller.