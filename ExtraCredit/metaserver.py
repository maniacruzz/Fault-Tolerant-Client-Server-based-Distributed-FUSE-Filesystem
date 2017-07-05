#!/usr/bin/env python

#################################
#Group Members:					#
#Amogh Rao						#
#Abhinandan Jyothishwara		#
#################################

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

ddbug=False


# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.files = {}
    self.data = defaultdict(bytes)
    self.fd = 0
    now = time()
    self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
            st_mtime=now, st_atime=now, st_nlink=2, files={})
    # The key 'files' holds a dict of filenames(and their attributes
    # # and 'files' if it is a directory) under each level
  
  def traverse(self, path, tdata = False, handle="open", update={}, bid=0):
    """Traverses the dict of dict(self.files) to get pointer
        to the location of the current file.
        Retuns the node from self.data if tdata else from self.files"""
    target = path[path.rfind('/')+1:]
    target = target if len(target)>0 else '/'
    #print("##############################printing targe tdata handle t####", target, tdata, handle )
    pp=self.data if tdata else self.files
    p = self.data if tdata else self.files['/']
    if tdata:
        for i in path.split('/') :
            pp=p if len(i) > 0 else p
            p = p[i] if len(i) > 0 else p
    else:
        for i in path.split('/') :
            try:
              pp=p if len(i) > 0 else p
              p = p['files'][i] if len(i) > 0 else p
            except KeyError:
              p = {}
    if tdata:
      if(ddbug):print("traverse reached target", target)
      if target in pp and len(pp[target])==0:
          pp[target]=[]  
      if(ddbug):print("traverse reached target", target)
      if(handle=="open"):  
        if(ddbug):print("############## last 5 pp, len(pp), target, bid", pp[target][-5:], len(pp[target]), target, bid)
        if(ddbug):print("bid", bid, "ppTar", pp[target][-5:], "pp tar bid ",pp[target][bid])
        return pp[target][bid]
      elif(handle=="close"):
        if(ddbug):print("pp:", pp)
        if(ddbug):print("update:", update)
        if len(pp[target])>bid: 
          pp[target][bid] = update 
        else: 
          pp[target].append(update)
        if(ddbug): print("self . data after traversal update", self.data)
    else:
      if(handle =="open"):
        return p
      else:
        pp['files'][target]=update
        if(ddbug): print("self . files after traversal update", self.files)
    if(ddbug):print("self.data:", self.data)


  def traverseparent(self, path, tdata = False, handle="open", update={}, bid=0):
    """Traverses the dict of dict(self.files) to get pointer
        to the parent directory of the current file.
        Also returns the child name as string"""
    #
    target = path[path.rfind('/')+1:]           ###obtain target'''
    path   = path[:path.rfind('/')]             ###to obtain parent into p'''
    pptar  = path[path.rfind('/')+1:]           ###to update p in pp'''
    pptar  = pptar if len(pptar) >0 else '/'
    if(ddbug):print("pptar", pptar)
    pp= self.data if tdata else self.files      ###always lags p by 1 node...'''
    p = self.data if tdata else self.files['/']    
    #pp=p
    if tdata:
      for i in path.split('/') :
        pp= p if len(i) > 0 else pp
        p = p[i] if len(i) > 0 else p
    else:
      for i in path.split('/') :
        pp= p if len(i) > 0 else pp  #p['files'][i]
        p = p['files'][i] if len(i) > 0 else p
    if(handle=="open"):
      if p=='':	
        return [defaultdict(bytes), target]
      return [p, target]
    elif(handle=="close"):
      if(ddbug):print("self. files at start of close\n", self.files)
      if(ddbug):print("self. data  at start of close\n", self.data)
      if(tdata):
        if bid<0:
            if pptar=='/':
                self.data=update
            else:
                p[target]=update[target]
        else:
            p[target][bid]=update[target][bid]
      else:
        if(pptar=='/'):
          pp['/']=update
        else:
          pp['files'][pptar]=update
      if(ddbug):print("self. files at end of close\n", self.files)
      if(ddbug):print("self. data  at end of close\n", self.data)
     
  def delete(self):
    self.data={}
    self.files = {}
    return True
    
  def count(self):
    return len(self.data)

  # Retrieve something from the HT
  def get(self, key):
    # Default return value
    rv = {}
    cmd=pickle.loads(key)
    if(ddbug):print("received get with cmd", cmd)
    if(cmd['function']=="traverseparent"):# and cmd['handle']=="open"):
      if(cmd['handle']=="open"):
        rv=self.traverseparent(cmd['path'], cmd['tdata'], cmd['handle'], cmd['update'], cmd['bid'])
    elif(cmd['function']=="traverse"):
      rv=self.traverse(cmd['path'], cmd['tdata'], cmd['handle'], cmd['update'], cmd['bid'])
    if(ddbug):print("sending DS", rv)
    return pickle.dumps(rv)

  # #Insert something into the HT
  def put(self, key, value):
    # Remove expired entries
    cmd={}
    cmd=pickle.loads(key)
    if(ddbug):print("received put with cmd :", cmd)
    if(cmd["function"]=="traverseparent"):
      self.traverseparent(cmd['path'], cmd['tdata'], cmd['handle'], cmd['update'], cmd['bid'])
    else:
      self.traverse(cmd['path'], cmd['tdata'], cmd['handle'], cmd['update'], cmd['bid'])
    return True

  # Load contents from a file
  def read_file(self, filename):
    f = open(filename, "rb") #f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.files
    return True
  
  def log_message(self, format, *args):
    return

def main():
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  for k,v in optlist:
    ol[k] = v

  port = int(args[0])
  if "--port" in ol:
    port = int(ol["--port"])
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  print("so the port is ", port)
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.delete)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.serve_forever()


if __name__ == "__main__":
  main()
