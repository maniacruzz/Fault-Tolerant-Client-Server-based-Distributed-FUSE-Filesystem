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
import os
import random

ddbug=False

filename=''
data=None
data_red=None

# Presents a HT interface
class SimpleHT:
    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.data_red = defaultdict(bytes)
        #print("filename :", filename)
        #print("status file exists:",os.path.exists("./"+filename))
        if os.path.exists("./"+filename):
            self.read_file(filename)
        else:
            if (data!=None and data_red!=None):
                self.data = data
                self.data_red = data_red
        self.fd = 0
        self.backup = {}
        self.write_file(filename)
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                st_mtime=now, st_atime=now, st_nlink=2, files={})
        # The key 'files' holds a dict of filenames(and their attributes
        # # and 'files' if it is a directory) under each level
  
    def traverse(self, path, tdata = False, handle="open", update={}, bid=0, tred = False, ver=-1):
        """Traverses the dict of dict(self.files) to get pointer
            to the location of the current file.
            Retuns the node from self.data if tdata else from self.files"""
        print("traverse started")
        target = path[path.rfind('/')+1:]
        target = target if len(target)>0 else '/'

        if tdata:
                pp= self.data_red if tred else self.data
                p = pp
        else:
            pp, p=self.files, p=self.files['/']

        if(ddbug): print("dict chosen")
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
        if(ddbug): print("chosen dict traversed")
        if tdata:
            if target in pp and len(pp[target])==0:
                pp[target]=[]  
            if(handle=="open"):  
                return pp[target][bid][ver]
            elif(handle=="close"):
                if len(pp[target])>bid: 
                    if len(pp[target][bid])>ver:
                        pp[target][bid][ver] = update
                    else:
                        pp[target][bid].append(update)
                else: 
                    pp[target].append([update])
                if(ddbug): print("tyring to write")    
                self.write_file(filename)
                if(ddbug): print("completed write")    
        else:
            if(handle =="open"):
                return p
            else:
                pp['files'][target]=update
        if(ddbug): print("traverse ended")
        


    def traverseparent(self, path, tdata = False, handle="open", update={}, bid=0,tred = False):
        """Traverses the dict of dict(self.files) to get pointer
            to the parent directory of the current file.
            Also returns the child name as string"""
        #
        target = path[path.rfind('/')+1:]           ###obtain target'''
        path   = path[:path.rfind('/')]             ###to obtain parent into p'''
        pptar  = path[path.rfind('/')+1:]           ###to update p in pp'''
        pptar  = pptar if len(pptar) >0 else '/'
        if(ddbug):print("pptar", pptar)
        if tdata:
                pp= self.data_red if tred else self.data
                p = pp
        else:
            pp, p=self.files, p=self.files['/']

        #pp= self.data if tdata else self.files      ###always lags p by 1 node...'''
        #p = self.data if tdata else self.files['/']    
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
            if(tdata):
                if bid<0:
                    if pptar=='/':
                        if tred:
                            self.data_red=update
                        else:
                            self.data=update
                    else:
                        p[target]=update[target]
                else:
                    p[target][bid]=update[target][bid]
                self.write_file(filename)     
            else:
                if(pptar=='/'):
                    pp['/']=update
                else:
                    pp['files'][pptar]=update
    
  
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
            rv=self.traverseparent(cmd['path'], cmd['tdata'], cmd['handle'], cmd['update'], cmd['bid'],cmd['tred'])
        elif(cmd['function']=="traverse"):
          rv=self.traverse(cmd['path'], cmd['tdata'], cmd['handle'], cmd['update'], cmd['bid'],cmd['tred'], cmd['current_version'])
        if(ddbug):print("sending DS", rv)
        return pickle.dumps(rv)

    def get_data(self, key):
        if key=='data':
            return pickle.dumps(self.data)
        elif key=='data_red':
            return pickle.dumps(self.data_red)
        else:
            pass

  # #Insert something into the HT
    def put(self, key, value):
        # Remove expired entries
        cmd={}
        cmd=pickle.loads(key)
        if(ddbug):print("received put with cmd :", cmd)
        if(cmd["function"]=="traverseparent"):
            self.traverseparent(cmd['path'], cmd['tdata'], cmd['handle'], cmd['update'], cmd['bid'],cmd['tred'])
        else:
            print("server traverse with cmd", cmd)
            self.traverse(cmd['path'], cmd['tdata'], cmd['handle'], cmd['update'], cmd['bid'],cmd['tred'],cmd['current_version'])
        return True

  # Load contents from a file
    def read_file(self, filename):
        f = open(filename, "rb") #f = open(filename.data, "rb")
        self.backup = pickle.load(f)
        self.data = self.backup['data']
        self.data_red = self.backup['data_red']
        f.close()
        return True

    # Write contents to a file
    def write_file(self, filename):
        f = open(filename, "wb")
        self.backup['data']=self.data
        self.backup['data_red']=self.data_red
        pickle.dump(self.backup, f)
        f.close()
        return True

    # Print the contents of the hashtable
    def print_content(self):
        print self.data
        return True

    def corrupt(self, path, bid=0, tred=False, cid=0, cchar='*'):
        print("corrupting file "+path+" at block "+str(bid)+" at char "+str(cid))
        try:
            d = self.traverseparent(path,True,"open", {},bid, tred)
            tar = d[1]
            d=d[0]
            
            print("fetched before corruption  ")
            print(d)
            print(tar)
            
            for block in d[tar][bid]:
		if d[0][cid]==cchar:
			cchar=chr(ord(cchar)+1)
                block[0]=block[0][:cid]+cchar+block[0][cid+1:]
            self.traverseparent(path,True,"close", d,bid, tred)
            
            print("re - fetched after corruption  ")
            print(d)
            print(tar)
        except IndexError:
            print("file "+path+"does not have that block")
        return True
  
    def log_message(self, format, *args):
        return

def main():
    optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
    ol={}
    print(args)
    for k,v in optlist:
        ol[k] = v
    ports=args[1:]
    port = int(ports[int(args[0])])
    next_port = int(ports[(int(args[0])+1)%len(ports)])
    prev_port = int(ports[(int(args[0])-1)%len(ports)])
    print("next port :", next_port)
    global filename,data,data_red
    filename ="backup_"+str(port)+".txt"
    #print("filename :" , filename)
    try: 
        ser = xmlrpclib.ServerProxy("http://localhost:"+str(next_port))
        #print("server :",ser)
        data = pickle.loads(ser.get_data('data_red'))
        ser = xmlrpclib.ServerProxy("http://localhost:"+str(prev_port))
        data_red = pickle.loads(ser.get_data('data'))
    except:
        pass
    print("port being served", port)
    serve(port)

# Start the xmlrpc server
def serve(port):
    file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
    file_server.register_introspection_functions()
    sht = SimpleHT()
    file_server.register_function(sht.get)
    file_server.register_function(sht.get_data)
    file_server.register_function(sht.delete)
    file_server.register_function(sht.put)
    file_server.register_function(sht.print_content)
    file_server.register_function(sht.corrupt)
    #file_server.register_function(sht.read_file)
    #file_server.register_function(sht.write_file)
    file_server.serve_forever()


if __name__ == "__main__":
    main()
