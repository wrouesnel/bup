"""Shared data defining bup's on-the-wire protocol

protocol.py defines the various constants and parameters which bup protocol
communication uses.
"""

import re
import struct
from collections import deque
from bup import options, metadata, vint, vfs
from bup.helpers import *

class ProtocolConstBase:
    """
    forbids to overwrite existing variables 
    forbids to add new values if "locked" variable exists
    """ 
    def __setattr__(self,name,value):
        if(self.__dict__.has_key("locked")):    
            raise NameError("Class is locked can not add any attributes (%s)"%name)
        if self.__dict__.has_key(name):
            raise NameError("Can't rebind const(%s)"%name)
        self.__dict__[name]=value
    
    def __new__(cls, *args, **kwargs):
        raise TypeError("May not instantiate a ProtocolConst class.")

#############
# Protocol 1
#############

#############
# Exception Classes
#############
class RestoreException(Exception):
    """Server exception raised from errors while handling a restore
    operation.
    """
    def __init__(self, message, clientheader, handled_items=0,
                 inner_exception=None):
        Exception.__init__(self, message)
        self.clientheader = clientheader
        self.handled_items = handled_items
        self.inner_exception = inner_exception

class SessionException(Exception):
    """Exception raised from session request setup.
    """
    pass

class HeaderException(Exception):
    """Exception raised by problems with headers.
    """
    pass

class NormalExit(Exception):
    """This crime is raised by the tarpipe to terminate the server process
    after it closes the connection.
    """
    pass

##############
# Constants
##############
class Consts(ProtocolConstBase):
    """Constants that define aspects of the protocol entirely
    """
    HASHSIZE = 20      # Hash size in bytes (size of SHA-1 hash)
    
    PROTOCOL_VERSION = 1    # Protocol version in this file

###############
# Helpers
###############
def packhash(self, sha):
    """pack a SHA1 hash to a network-byte order struct"""
    return struct.pack('!%is' % Consts.HASHSIZE, sha)

def unpackhash(self,din):
    """unpack a SHA1 hash to a bytestring"""
    return struct.unpack('!%is' % Consts.HASHSIZE, din)[0]

def readpackedhash(self,port):
    return struct.unpack('!%is' % Consts.HASHSIZE, port.read(Consts.HASHSIZE))[0]

##############
# Data structures
##############
class PathStack:
    """Manages the stack of path elements associated with sent metadata.
    """
    def __init__(self, root=''):
        self.root = slashremove(root)
        
        self.stack = [] # list of paths
        self.delta = 0  # number of level changes (positive = up) since check
        
    def check_delta(self):
        delta = self.delta
        self.delta = 0      # zero delta
        return delta
    
    def push(self, name):
        self.delta -= 1
        self.stack.append(name)
        
    def pop(self, count=1):
        self.delta += 1
        self.stack = self.stack[:-count]
        
    def join(self, name):
        """return a (relative) path name from the stack for the given name"""
        relativepath = '/'.join(self.stack + [name])
        if relativepath == '':
            relativepath = '/'
        return self.root + relativepath

class RestoreSessionRequest:
    """Encapsulates the commands needed to start a restore session. These
    are the text commands which are sent before bup-server enters the
    binary mode of a command.
    
    This object is uncached - there is no explicit response expected and
    it is only sent once per session.
    """
    
    # tuple of valid transfer modes
    TRANSFERMODES = ('bup', 'tar', 'targz', 'tarbz2')
    
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
transfermode= select bup transfer mode. options are simple, tar, targz, tarbz2.
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
        
class RestoreHeader:
    """"Describes the restore-files header used between client and server
    Flags may have different meanings based on who's receiving them.
    Implements flag access as list properties to set the bits, and stores
    the meanings as class variables.
    """

    # Header types
    # we use the rightmost bit as an indicator for the size field being
    # present, so keep an eye if you need an int more then 6 bits long.
    H_NONE        = 0   # noop
    H_FAILED      = 1   # failure happened and restore is aborting
    H_FINISHED    = 2   # iteration finished, or client finished
    H_METADATA    = 3   # client wants metadata/server is sending metadata
    H_HASHLIST    = 4   # client wants hashlist/server is sending hashlist
    H_BLOBS       = 5   # client wants blobs/server is sending blobs

    # This generates a reverse dictionary for the types
    header_strs = dict((value, key)
                       for key, value in locals().items()
                           if key.startswith('H_'))

    def __init__(self, headertype):
        # TODO: check header is valid. Encode more useful data.
        self.type = headertype
        
    def __repr__(self):
        return '%s' % self.header_strs[self.type] 
    
    def type(self):
        """return string name of header type"""
        return self.header_strs[self.type]
    
    def write(self, port):
        vint.write_vuint(port, self.type)  # write header byte
    
    @staticmethod
    def read(port):   
        header = vint.read_vuint(port)   # get raw header
        o = RestoreHeader(header)  
        return o     

class ProtocolMetadata:
    """Container for VFS node metadata as used in the bup communications
    protocol (distinct from the archive protocol)
    """
    @staticmethod
    def create_from_node(node, pathstack):
        # note: you can't send these out-of-order, or the pathstack data
        # gets lost.
        o = ProtocolMetadata()
        o.name = node.name
        o.meta = node.metadata()
        if o.meta is None:  # assign minimal metadata
            o.meta = metadata.from_vfs(node)
        
        # get the path delta
        o.pathdelta = pathstack.check_delta()

        if o.pathdelta < -1:
            raise Exception("pathstack delta descended more then -1")

        return o
    
    def write(self, port):
        # output the order this element goes in the pathstack
        vint.write_vint(port, self.pathdelta)
        
        # Output filename (might be 0 length)
        vint.write_bvec(port, self.name)
        
        # Metadata
        self.meta.write(port)
        
    @staticmethod
    def read(port, pathstack=None):
        o = ProtocolMetadata()
        
        # Poplevels
        poplevels = vint.read_vint(port)
        
        # If we have a path stack (which we should), pop up the levels
        if (pathstack is not None) and poplevels > 0:
            pathstack.pop(poplevels)
        
        # Path
        o.name = vint.read_bvec(port)
        if pathstack is not None:
            o.path = pathstack.join(o.name)
        else:
            o.path = o.name 
    
        # Metadata
        o.meta = metadata.Metadata.read(port)
        
        # If meta is a directory, then push this name onto the stack.
        if (pathstack is not None) and o.meta.isdir():
            pathstack.push(o.name)
        return o

class PipelineRequest(deque):
    """Generic object for making a request for data from a server pipeline
    as a series of start/length tuples indicating object positions.
    
    Clients can request a discard by giving a start with length 0.
    Tuples start at 0.
    """
    def write(self, port):
        # number of tuples
        vint.write_vuint(port, len(self))
        
        # TODO: runtime check tuples make sense
        
        # write tuple pairs
        for (start,length) in self:
            vint.write_vuint(port, start)
            vint.write_vuint(port, length)
    
    @staticmethod
    def read(port):
        # TODO: runtime check tuples make sense
        o = PipelineRequest()
        num = vint.read_vuint(port) # read size
        
        for i in range(num):
            start = vint.read_vuint(port)
            length = vint.read_vuint(port)
            o.append((start, length))
        return o

class Hashlist(list):
    """Overload class for serializing hashlists
    """
    def hashes(self):
        return [ sha for (ofs,sha) in self ]
    
    def write(self, port):
        vint.write_vuint(port, len(self)) # length
        for (ofs, sha) in self: # write (ofs,sha) tuples
            vint.write_vuint(port, ofs)
            packed = struct.pack('!%is' % Consts.HASHSIZE, sha)
            port.write(packed)
    
    @staticmethod
    def create_from_pipeline(pipeline):
        n = pipeline.popleft()
        return Hashlist(n.hashlist())            
    
    @staticmethod
    def read(port):
        o = Hashlist()
        
        length = vint.read_vuint(port)  # length
        for i in range(length): # read (ofs,sha) tuples
            ofs = vint.read_vuint(port)
            sha = struct.unpack('!%is' % Consts.HASHSIZE, 
                              port.read(Consts.HASHSIZE))[0]
            o.append((ofs,sha))
        return o

class Hashlists:
    """container class for sending/receiving lists of hashlists.
    """
    def __init__(self, nodes):
        self.nodes = nodes
    
    @staticmethod
    def create_from_pipeline(pipeline, count):
        nodes = []
        for i in range(count):
            nodes.append(pipeline.popleft())
        return Hashlists(nodes)
    
    def write(self, port, hashpipeline = None):
        vint.write_vuint(port, len(self.nodes)) # length
        for n in self.nodes:
            h = Hashlist(n.hashlist())
            h.write(port)
            if hashpipeline is not None: # append sent data to hashlist
                for (ofs, sha) in h:
                    hashpipeline.append(sha)
    
    @staticmethod
    def read_hashlists_iter(port):
        """yields hashlist objects from port."""
        count = vint.read_vuint(port)
        for i in range(count):
            yield Hashlist.read(port)

class Blobs:
    """Object container for sending/receiving blobs of file data. Based on
    generators to keep memory use down.
    """
    def __init__(self, hashes):
        """hashes is a list of the hashes being sent by this object"""
        self.hashes = hashes
    
    @staticmethod
    def create_from_pipeline(pipeline, count):
        hashes = []
        for i in range(count):
            blobhash = pipeline.popleft()
            hashes.append(blobhash)
        return Blobs(hashes)     
    
    def write(self, port):
        vint.write_vuint(port, len(self.hashes)) # length
        for blobhash in self.hashes:
            blob = ''.join(vfs.cp().join(blobhash.encode('hex')))
            vint.write_bvec(port, blob)
    
    @staticmethod
    def read(port):
        count = vint.read_vuint(port)
        for i in range(count):
            blob = vint.read_bvec(port)
            yield blob
