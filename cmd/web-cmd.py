#!/usr/bin/env python
import sys, stat, urllib, mimetypes, posixpath, time, webbrowser

# WebDAV support
import calendar
from email.utils import formatdate

from httplib import responses as http_responses
from lxml import etree
from lxml.builder import ElementMaker

from bup import options, git, vfs, xstat
from bup.helpers import *
try:
    import tornado.httpserver
    import tornado.ioloop
    import tornado.web
    from tornado import web
except ImportError:
    log('error: cannot find the python "tornado" module; please install it\n')
    sys.exit(1)

handle_ctrl_c()

# Web Dav XML element types
DAV_VERSION='1,2'
DAV_NS="DAV:"
DAVElement = ElementMaker(namespace=DAV_NS, nsmap={'d' : DAV_NS })  

MultistatusElement = DAVElement.multistatus
ResponseElement = DAVElement.response
PropStatElement = DAVElement.propstat
PropElement = DAVElement.prop
StatusElement = DAVElement.status
HrefElement = DAVElement.href
CollectionElement = DAVElement.collection
ErrorElement = DAVElement.error

# We may need to monkey-patch http_responses to recognize 207 (multistatus)
try:
    http_responses[207]
except KeyError:
    http_responses[207] = "Multi-Status"

# This is not an exhaustive list - it's just the properties we can
# actually return properly.
DavProperties = [
        '{%s}getlastmodified' % DAV_NS, 
        '{%s}resourcetype'% DAV_NS ,
        '{%s}getetag'% DAV_NS, 
        '{%s}getcontentlength'% DAV_NS,
        '{%s}getcontenttype' % DAV_NS, # dead property
        ]

def _compute_breadcrumbs(path, show_hidden=False):
    """Returns a list of breadcrumb objects for a path."""
    breadcrumbs = []
    breadcrumbs.append(('[root]', '/'))
    path_parts = path.split('/')[1:-1]
    full_path = '/'
    for part in path_parts:
        full_path += part + "/"
        url_append = ""
        if show_hidden:
            url_append = '?hidden=1'
        breadcrumbs.append((part, full_path+url_append))
    return breadcrumbs


def _contains_hidden_files(n):
    """Return True if n contains files starting with a '.', False otherwise."""
    for sub in n:
        name = sub.name
        if len(name)>1 and name.startswith('.'):
            return True

    return False


def _compute_dir_contents(n, path, show_hidden=False):
    """Given a vfs node, returns an iterator for display info of all subs."""
    url_append = ""
    if show_hidden:
        url_append = "?hidden=1"

    if path != "/":
        yield('..', '../' + url_append, '')
    for sub in n:
        display = link = sub.name

        # link should be based on fully resolved type to avoid extra
        # HTTP redirect.
        if stat.S_ISDIR(sub.try_resolve().mode):
            link = sub.name + "/"

        if not show_hidden and len(display)>1 and display.startswith('.'):
            continue

        size = None
        if stat.S_ISDIR(sub.mode):
            display = sub.name + '/'
        elif stat.S_ISLNK(sub.mode):
            display = sub.name + '@'
        else:
            size = sub.size()
            size = (opt.human_readable and format_filesize(size)) or size

        yield (display, link + url_append, size)


class BupRequestHandler(tornado.web.RequestHandler):
    # This is the full range of methods we need to do something with.
    #SUPPORTED_METHODS = ("HEAD", "GET", "POST", "OPTIONS", 
    #        "PUT", "DELETE", "MKCOL", 
    #        "PROPFIND", "PROPPATCH", 
    #        "MOVE", "COPY", "LOCK", "UNLOCK")
    
    # These are the methods we currently support.
    SUPPORTED_METHODS = ("HEAD", "GET", "POST", "OPTIONS", "PROPFIND")
    
    # Implemented
    def head(self, path):
        return self._process_request(path)
    
    def get(self, path):
        return self._process_request(path)

    def post(self, path):
        return get(self, path)
    
    def options(self, path):
        return self._process_request(path)
    
    def propfind(self, path):
        return self._process_request(path)
    
    # Unimplemented (i.e. impossible with a bup repository at the moment)
    def put(self, name):
        raise web.HTTPError(501)  

    def mkcol(self, name):
        raise web.HTTPError(501)  

    def move(self, name):
        raise web.HTTPError(501)  

    def copy(self, name):
        raise web.HTTPError(501)  

    def delete(self, name):
        raise web.HTTPError(501)
    
    def proppatch(self, name):
        raise web.HTTPError(501)  

    def lock(self, name):
        raise web.HTTPError(501)  

    def unlock(self, name):
        raise web.HTTPError(501)
    
    @tornado.web.asynchronous
    def _process_request(self, path):
        path = urllib.unquote(path)
        debug1('Handling request for %s' % path)
        try:
            n = top.resolve(path)
        except vfs.NoSuchFile:
            self.send_error(404)
            return
        f = None
        
        if self.request.method == 'HEAD':
            self.set_header('Allow', ",".join(self.SUPPORTED_METHODS))
        elif self.request.method == 'OPTIONS':
            self.set_header('Allow', ",".join(self.SUPPORTED_METHODS))
            self.set_header('Dav', DAV_VERSION)
            self.finish()
            return
        elif self.request.method == 'PROPFIND':
            self._propfind(path, n)
            return
            
        if stat.S_ISDIR(n.mode):
            self._list_directory(path, n)
        else:
            self._get_file(path, n)

    def _propfind(self, path, n):
        """handle a PROPFIND request
        """
        # depth header
        depth = self.request.headers.get('depth')
        if depth:
            if depth == "infinity":
                depth = None
            else:
                try:
                    depth = int(depth)
                except:
                    depth = None
        
        # TODO: handle IF headers
        
        prop_list = None  # Properties that will be returned (none == all)
        prop_name = False   # Return only list of properties on element
        # If not a blank request (Win 7 sends those) then handle normally
        if not (self.request.body == None or self.request.body == ''):
            # Get root.
            root = etree.fromstring(self.request.body)
            # Root *must* be a propfind element if it exists
            if root.tag != '{DAV:}propfind':
                raise web.HTTPError(400)
            if len(root) > 0:
                if root[0].tag == '{DAV:}propname':
                    prop_name = True    # return names of properties, not values
                elif root[0].tag == '{DAV:}allprop': 
                    pass    # Validate, but pass prop_list == None for all props
                elif root[0].tag == '{DAV:}prop':
                    # return only the named properties
                    prop_list = [prop.tag for prop in list( root[0] )]
                else:
                    raise web.HTTPError(400)
            
        # recursively get as many properties as the client requested
        response = []
        self._do_propfind(path, n, prop_list, depth, response, prop_name)
        
        self.set_header("Content-Type", "text/xml; charset=UTF-8")
        self.set_status(207)
        response_str = etree.tostring( DAVElement.multistatus(*response),
                                     pretty_print=True,
                                     encoding='UTF-8',
                                     xml_declaration=True)
        self.write(response_str)
        self.finish()

    def _do_propfind(self, path, n, prop_list, depth, response, prop_name):
        """recursive worker for propfind. note that we can't depend on
        using VFS names, so we need to pass a path stack between all the
        layers.
        """
        propfound = []      # Found properties
        propnotfound = []   # Not found properties
        
        if prop_list is None:
            for name in DavProperties:
                propfound.append(self._get_property(name, n, prop_name))
        else:
            # Get the property results for this node
            propnotfound = [DAVElement(name) for name in prop_list \
                            if not name in DavProperties ]
            found = [ name for name in prop_list if name in DavProperties ]
            for name in found:
                propfound.append(self._get_property(name, n, prop_name))
        
        # Generate the 200/404 multistatus lists for XML conversion here
        prop_result = []
        if len(propfound) > 0:
            prop_result.append( (200, propfound) )
        if len(propnotfound) > 0:
            prop_result.append( (404, propnotfound) )  
        
        # Wrap it in a propstat tags
        prop_stat = []
        for code, items in prop_result:
            if len(items) > 0:
                prop_stat.append(DAVElement.propstat(DAVElement.prop(*items),
                    DAVElement.status("HTTP/1.1 %d %s" % (code,  http_responses[code]))))
        
        # append to the big response
        response.append(DAVElement.response(DAVElement.href(path), 
                                            *prop_stat))
        
        # descend into child nodes
        if depth > 0:
            for child in n.subs():
                self._do_propfind(eatslash(path) + '/' + child.name, child, 
                                  prop_list, depth-1, response, prop_name)
    
    def _get_property(self, property, n, prop_name):
        """helper to propfind to retrieve element property values"""
        meta = n.metadata()
        if meta is not None:
            n.mtime = xstat.fstime_to_timespec(meta.mtime)[0]
            n.ctime = xstat.fstime_to_timespec(meta.ctime)[0]
            n.atime = xstat.fstime_to_timespec(meta.atime)[0]
        
        # properties we understand (or care about)        
        if property == '{DAV:}getlastmodified':
            if prop_name:
                return DAVElement.getlastmodified()
            t = calendar.timegm(time.gmtime(n.mtime))
            return DAVElement.getlastmodified(
                formatdate(t, localtime=False, usegmt=True) )
        
        elif property == '{DAV:}getcontentlength':
            if prop_name:
                return DAVElement.getcontentlength()
            return DAVElement.getcontentlength( str(n.size()) )
        
        elif property == '{DAV:}resourcetype':
            if prop_name:
                return DAVElement.resourcetype()
            if stat.S_ISDIR(n.mode):
                return DAVElement.resourcetype(DAVElement.collection)
            else:
                # might still be a dir, check if it's a symlink
                if stat.S_ISLNK(n.mode):
                    # resolve it and see what it is
                    try:
                        if stat.S_ISDIR(n.dereference().mode):
                            return DAVElement.resourcetype(DAVElement.collection)
                    except:
                        pass
                return DAVElement.resourcetype()
        
        elif property == '{DAV:}getetag':
            if prop_name:
                return DAVElement.etag()
            return DAVElement.etag(n.hash.encode('hex'))
        
        elif property == '{DAV:}getcontenttype':
            if prop_name:
                DAVElement.getcontenttype()
            return DAVElement.getcontenttype(self._guess_type(n))            

    def _list_directory(self, path, n):
        """Helper to produce a directory listing.

        Return value is either a file object, or None (indicating an
        error).  In either case, the headers are sent.
        """
        if not path.endswith('/') and len(path) > 0:
            debug1('Redirecting from %s to %s' % (path, path + '/'))
            return self.redirect(path + '/', permanent=True)

        try:
            show_hidden = int(self.request.arguments.get('hidden', [0])[-1])
        except ValueError, e:
            show_hidden = False

        self.render(
            'list-directory.html',
            path=path,
            breadcrumbs=_compute_breadcrumbs(path, show_hidden),
            files_hidden=_contains_hidden_files(n),
            hidden_shown=show_hidden,
            dir_contents=_compute_dir_contents(n, path, show_hidden))

    def _get_file(self, path, n):
        """Process a request on a file.

        Return value is either a file object, or None (indicating an error).
        In either case, the headers are sent.
        """
        ctype = self._guess_type(n)

        self.set_header("Last-Modified", self.date_time_string(n.mtime))
        self.set_header("Content-Type", ctype)
        size = n.size()
        self.set_header("Content-Length", str(size))
        assert(len(n.hash) == 20)
        self.set_header("Etag", n.hash.encode('hex'))

        if self.request.method != 'HEAD':
            self.flush()
            f = n.open()
            it = chunkyreader(f)
            def write_more(me):
                try:
                    blob = it.next()
                except StopIteration:
                    f.close()
                    self.finish()
                    return
                self.request.connection.stream.write(blob,
                                                     callback=lambda: me(me))
            write_more(write_more)
        else:
            self.finish()

    def _guess_type(self, n):
        """Guess the type of a file.

        Argument is a PATH (a filename).

        Return value is a string of the form type/subtype,
        usable for a MIME Content-type header.

        The default implementation looks the file's extension
        up in the table self.extensions_map, using application/octet-stream
        as a default; however it would be permissible (if
        slow) to look inside the data to make a better guess.
        """
        l = n
        if stat.S_ISLNK(n.mode):
            try:
                l = n.resolve()
            except:
                return 'inode/symlink'
        # FIXME: the VFS should figure out mime-types. Not us.
        if stat.S_ISDIR(l.mode):
            # check if we're any of the special types
            if isinstance(l, vfs.CommitDir):
                return 'bup-commit-dir/directory'
            elif isinstance(l, vfs.TagDir):
                return 'bup-tag-dir/directory'
            elif isinstance(l, vfs.RefList):
                return 'bup-ref-list/directory'
            elif isinstance(l, vfs.BranchList):
                return 'bup-branch-list/directory'
            else:
                # this is a filesystem default, but does it make sense for vfs?
                return 'inode/directory'
        
        guessed_type = mimetypes.guess_type(l.name, strict=False)
        
        if guessed_type[0] == (None):
            return 'application/octet-stream'
        return guessed_type[0]

    def date_time_string(self, t):
        return time.strftime('%a, %d %b %Y %H:%M:%S', time.gmtime(t))


optspec = """
bup web [-s] [[hostname]:port|socketname]
--
human-readable    display human readable file sizes (i.e. 3.9K, 4.7M)
browser           open the site in the default browser
s,socket    connect bup-web to a unix socket. 
"""
o = options.Options(optspec)
(opt, flags, extra) = o.parse(sys.argv[1:])

if len(extra) > 1:
    o.fatal("at most one argument expected")

if opt.socket:
    socket_path = None
    if len(extra) > 0:
        socket_path = extra[0]
    else:
        o.fatal("socket mode requires a path.")
else:
    address = ('127.0.0.1', 8080)
    if len(extra) > 0:
        addressl = extra[0].split(':', 1)
        addressl[1] = int(addressl[1])
        address = tuple(addressl)

git.check_repo_or_die()
top = vfs.RefList(None)

settings = dict(
    debug = 1,
    template_path = resource_path('web'),
    static_path = resource_path('web/static')
)

# Disable buffering on stderr
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0) 

# Initialize mimetypes app-wide
if not mimetypes.inited:
    mimetypes.init() # try to read system mime.types
extensions_map = mimetypes.types_map.copy()
extensions_map.update({
    '': 'text/plain', # Default
    '.py': 'text/plain',
    '.c': 'text/plain',
    '.h': 'text/plain',
    })

application = tornado.web.Application([
    (r"(/.*)", BupRequestHandler),
], **settings)

http_server = tornado.httpserver.HTTPServer(application)

if opt.socket:  # listen on unix socket
    if socket_path is not None:
        sock = tornado.netutil.bind_unix_socket(socket_path)
    else:
        raise Exception("socket_path cannot be None.")
    http_server.add_socket(sock)
else:   # listen on ip address as normal
    http_server.listen(address[1], address=address[0])

    try:
        sock = http_server._socket # tornado < 2.0
    except AttributeError, e:
        sock = http_server._sockets.values()[0]

if opt.socket:
    debug1("Serving HTTP on %s..." % sock.getsockname())
else:
    debug1("Serving HTTP on %s:%d..." % sock.getsockname())

    loop = tornado.ioloop.IOLoop.instance()
    if opt.browser:
        browser_addr = 'http://' + address[0] + ':' + str(address[1])
        loop.add_callback(lambda : webbrowser.open(browser_addr))
    loop.start()
