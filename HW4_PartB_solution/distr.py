#!/usr/bin/env python

import logging, xmlrpclib, pickle

from xmlrpclib import Binary
from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

bsize = 8

class Memory(LoggingMixIn, Operations):
    """Implements a hierarchical file system by using FUSE virtual filesystem.
       The file structure and data are stored in local memory in variable.
       Data is lost when the filesystem is unmounted"""

    def __init__(self, mport, dports):
        self.fd = 0
        self.metaserv = xmlrpclib.ServerProxy("http://localhost:" + str(int(mport)))
        self.dataserv = [xmlrpclib.ServerProxy("http://localhost:" + str(int(i))) for i in dports]
        self.metaserv.clear()
        for i in self.dataserv:
            i.clear()
        now = time()
        self.putmeta('/', dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                st_mtime=now, st_atime=now, st_nlink=2, files=[]))
        # The key 'files' holds a dict of filenames(and their attributes
        #  and 'files' if it is a directory) under each level

    def hashpath(self, path):
        return sum(ord(i) for i in path)

    def getmeta(self, path):
        return pickle.loads(self.metaserv.get(Binary(path)).data)

    def putmeta(self, path, meta):
        return self.metaserv.put(Binary(path), Binary(pickle.dumps(meta)))

    def purgemeta(self, path):
        return self.metaserv.remove(Binary(path))

    def getdata(self, path, blks):
        phash = self.hashpath(path)
        return [self.dataserv[(phash + blk) % len(self.dataserv)].get(Binary(str(blk) + path)).data for blk in blks]

    def putdata(self, path, blks, datablks):
        phash = self.hashpath(path)
        for i in range(len(blks)):
            self.dataserv[(phash + blks[i]) % len(self.dataserv)].put(Binary(str(blks[i]) + path), Binary(datablks[i]))

    def purgedata(self, path, blks):
        phash = self.hashpath(path)
        return [self.dataserv[(phash + blk) % len(self.dataserv)].remove(Binary(str(blk) + path)) for blk in blks]
    
    def splitpath(self, path):
        childpath = path[path.rfind('/')+1:]
        parentpath = path[:path.rfind('/')]
        if parentpath == '':
            parentpath = '/'
        return parentpath, childpath

    def chmod(self, path, mode):
        p = self.getmeta(path)
        p['st_mode'] &= 0o770000
        p['st_mode'] |= mode
        self.putmeta(path, p)
        return 0

    def chown(self, path, uid, gid):
        p = self.getmeta(path)
        p['st_uid'] = uid
        p['st_gid'] = gid
        self.putmeta(path)

    def create(self, path, mode):
        ppath, cname = self.splitpath(path)
        p = self.getmeta(ppath)
        p['files'].append(cname)
        self.putmeta(ppath, p)
        self.putmeta(path, dict(st_mode=(S_IFREG | mode), st_nlink=1,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time()))
        self.fd += 1
        return self.fd

    def getattr(self, path, fh = None):
        try:
            p = self.getmeta(path)
        except:
            raise FuseOSError(ENOENT)
        return {attr:p[attr] for attr in p.keys() if attr != 'files'}

    def getxattr(self, path, name, position=0):
        p = self.getmeta(path)
        attrs = p.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        p = self.getmeta(path)
        attrs = p.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        ppath, cname = self.splitpath(path)
        p = self.getmeta(ppath)
        p['files'].append(cname)
        p['st_nlink'] += 1
        self.putmeta(ppath, p)
        self.putmeta(path, dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),files=[]))

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        p = self.getmeta(path)
        if offset + size > p['st_size']:
            size = p['st_size'] - offset
        dd = ''.join(self.getdata(path, range(offset//bsize, (offset + size - 1)//bsize + 1)))
        dd = dd[offset % bsize:offset % bsize + size]
        return dd

    def readdir(self, path, fh):
        p = self.getmeta(path)
        return ['.', '..'] + p['files']

    def readlink(self, path):
        p = self.getmeta(path)
        return ''.join(self.getdata(path, range(p['st_size']//bsize)))

    def removexattr(self, path, name):
        p = self.getmeta(path)
        attrs = p.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
        self.putmeta(path, p)

    def rename(self, old, new):
        po, po1 = self.traverseparent(old)
        pn, pn1 = self.traverseparent(new)
        if po['files'][po1]['st_mode'] & 0o770000 == S_IFDIR:
            po['st_nlink'] -= 1
            pn['st_nlink'] += 1
        pn['files'][pn1] = po['files'].pop(po1)
        do, do1 = self.traverseparent(old, True)
        dn, dn1 = self.traverseparent(new, True)
        dn[dn1] = do.pop(do1)

    def rmdir(self, path):
        p = self.getmeta(path)
        if len(p['files']) > 0:
            raise FuseOSError(ENOTEMPTY)
        self.purgemeta(path)
        ppath, cname = self.splitpath(path)
        p = self.getmeta(ppath)
        p['files'].remove(cname)
        p['st_nlink'] -= 1
        self.putmeta(ppath, p)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        p = self.getmeta(path)
        attrs = p.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        ppath, cname = self.splitpath(target)
        p = self.getmeta(ppath)
        p['files'].append(cname)
        self.putmeta(ppath, p)
        self.putmeta(target, dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source)))
        datablks = [source[i:i+bsize] for i in range(0, len(source), bsize)]
        self.putdata(target, range(len(datablks)), datablks)

    def truncate(self, path, length, fh=None):
    	p = self.getmeta(path)
        currblks = range((p['st_size'] - 1)//bsize + 1)
        newblks  = range((length - 1)//bsize + 1)
        # create new blocks as needed
        blks_to_create = list(set(newblks[:-1]) - set(currblks)) 
        self.putdata(path, blks_to_create, ['\x00'*bsize]*len(blks_to_create))
        # purge existing blocks as required
        blks_to_purge = list(set(currblks) - set(newblks))
        self.purgedata(path, blks_to_purge)
        # last block trunc
        if len(newblks) > 0:
            if newblks[-1] in currblks:
                self.putdata(path, [newblks[-1]], [self.getdata(path, [newblks[-1]])[0][:length % offset]])
            else:
                self.putdata(path, [newblks[-1]], ['\x00'*(length % bsize)])
        p = self.getmeta(path)
        p['st_size'] = length
        self.putmeta(path, p)

    def unlink(self, path):
        ppath, cname = self.splitpath(path)
        p = self.getmeta(ppath)
        p['files'].remove(cname)
        self.putmeta(ppath, p)
        p = self.getmeta(path)
        self.purgemeta(path)
        blks = range((p['st_size'] - 1)//bsize + 1)
        self.purgedata(path, blks)

    def utimens(self, path, times = None):
        now = time()
        atime, mtime = times if times else (now, now)
        p = self.getmeta(path)
        p['st_atime'] = atime
        p['st_mtime'] = mtime
        self.putmeta(path, p)

    def write(self, path, data, offset, fh):
    	p = self.getmeta(path)
        currblks = range((p['st_size'] - 1)//bsize + 1)
        if offset > p['st_size']:
            lfill = [(self.getdata(path, [i])[0] if i in currblks else '').ljust(bsize, '\x00') for i in range(offset//bsize)] \
                    + [(self.getdata(path, [offset//bsize])[0][:offset % bsize] if offset//bsize in currblks else '').ljust(offset % bsize, '\x00')]
            self.putdata(path, range(0, offset//bsize), lfill)
        size = len(data)
        sdata = [data[:bsize - (offset % bsize)]] + [data[i:i+bsize] for i in range(bsize - (offset % bsize), size, bsize)]
        blks = range(offset//bsize, (offset + size - 1)//bsize + 1)
        mod = blks[:]
        mod[0] = (self.getdata(path, [blks[0]])[0][:offset % bsize] if blks[0] in currblks else '').ljust(offset % bsize, '\x00') + sdata[0]
        if len(mod[0]) != bsize and blks[0] in currblks:
            mod[0] = mod[0] + self.getdata(path, [blks[0]])[0][len(mod[0]):]
        mod[1:-1] = sdata[1:-1]
        if len(blks) > 1:
            mod[-1] = sdata[-1] + (self.getdata(path, [blks[-1]])[0][len(sdata[-1]):] if blks[-1] in currblks else '')
        self.putdata(path, blks, mod)
        p['st_size']= offset + size if offset + size > p['st_size'] else p['st_size']
        self.putmeta(path, p)
        return size

if __name__ == '__main__':
    if len(argv) < 4:
        print('usage: %s <mountpoint> <metaport> <dataport1> <dataport2> ..' % argv[0])
        exit(1)
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(argv[2], argv[3:]), argv[1], foreground=True, debug=True)
