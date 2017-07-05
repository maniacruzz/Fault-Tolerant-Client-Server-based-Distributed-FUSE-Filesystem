#!/usr/bin/env python

#################################
#Group Members:					#
#Amogh Rao						#
#Abhinandan Jyothishwara		#
#################################

from __future__ import print_function, absolute_import, division

import logging
import math, random
import errno

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

import sys, SimpleXMLRPCServer, getopt, pickle, threading, xmlrpclib, unittest, socket
from datetime import datetime, timedelta
from xmlrpclib import Binary

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

gblock=512
with_server=True
ddbug=False


class Memory(LoggingMixIn, Operations):
     
    """Implements a hierarchical file system by using FUSE virtual filesystem.
       The file structure and data are stored in local memory in variable.
       Data is lost when the filesystem is unmounted"""

    def __init__(self,args):
        if(not with_server):
            if(ddbug): print("instantiating self.files and self.data in stand alone mode")
            self.files = {}
            self.data = defaultdict(bytes)
            now = time()
            self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                    st_mtime=now, st_atime=now, st_nlink=2, files={})
        self.fd = 0
        self.metas = args[0] if len(args) > 0 else 0
        self.datas = args[1:] if len(args[1:]) > 1 else [args[0]] if self.metas !=0 else 1
        self.num_D=len(self.datas) if len(args) > 0 else 1
        self.doCorrection=True
        print("metas",self.metas)
        print("datas",self.datas)
        print("num_D",self.num_D)
        self.start_d=0
        # The key 'files' holds a dict of filenames(and their attributes
        #  and 'files' if it is a directory) under each level

    def traverse(self, path, tdata = False, handle="open", update={}, bid=0):
        """Traverses the dict of dict(self.files) to get pointer
            to the location of the current file.
            Retuns the node from self.data if tdata else from self.files"""
        SBid = bid//self.num_D
        SerN = (bid+self.start_d)%self.num_D
        #str="".join(path.split("/"))
        #start_d=0
        #for i in range(len(str)): start_d+=ord(str[i])*(i+1)
        #start_d=start_d%self.num_D
        #SerN=(SerN+start_d)%self.num_D

        SerRed=(SerN+1)%self.num_D

        #if(ddbug): print("start_d is ", start_d)
        if(ddbug): print("SerN is ", SerN)
        if(ddbug): print("start_d is ", self.start_d)
        if(ddbug): print("SerRed is ", SerRed)
        if(ddbug): print("bid is ", bid)
        if(ddbug): print("Sbid is ",SBid)
        #bid = SBid
        ret1=[]
        ret2=[]
        cmd = {'function':'traverse','path':path,'tdata':tdata, 'handle':handle, 'update':update, 'bid':SBid}
        if(ddbug): print(" traverse with cmd", cmd)
        if(not with_server):
            target = path[path.rfind('/')+1:]
            target = target if len(target)>0 else '/'
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
              if target in pp and len(pp[target])==0:
                  pp[target]=[]  
              if(handle=="open"):  
                return pp[target][bid]
              elif(handle=="close"):
                if len(pp[target])>bid: 
                    pp[target][bid] = update 
                else: 
                    pp[target].append(update)
            else:
              if(handle =="open"):
                return p
              else:
                pp['files'][target]=update
        else:
            orig_correct=False
            redundant_correct=False
            if(handle=="open"):
                if(ddbug): print("server traverse cmd", cmd)                
                pcmd= pickle.dumps(cmd)            
                if tdata==False:
                    ser = xmlrpclib.ServerProxy("http://localhost:"+self.metas)
                    rv = ser.get(pcmd)
                    rd={}
                    rd=pickle.loads(rv)
                    if(ddbug): print("unpickled it ", rd)
                    self.start_d=rd['start_d'] if 'start_d' in rd.keys() else 0 
                    return rd
                else:
                    i=0
                    rd=[]
                    p = self.traverse(path)
                    if(ddbug):print("#############################################################################trying to get the version")
                    if(ddbug):print(p)
                    cmd['current_version']=p['current_version'][bid]
                    self.doCorrection=True
                    for sernum in [[SerN,False],[SerRed,True]]:
                        ser = xmlrpclib.ServerProxy("http://localhost:"+self.datas[sernum[0]])
                        cmd['tred']=sernum[1]
                        
                        pcmd= pickle.dumps(cmd)
                        if(ddbug):print("############################################")
                        rv =  self.blocking_get(ser, pcmd)
                        #rv = ser.get(pcmd)
                        if(ddbug): print("getting a string ")
                        rd.append(pickle.loads(rv))
                        if(ddbug): print("unpickled it rd", rd)
                        i+=1
                    if (self.checksum(rd[0][0])==rd[0][1]):
                        orig_correct=True
                    if (self.checksum(rd[1][0])==rd[1][1]):
                        redundant_correct=True  
                    if(ddbug):print("orig_correct ", orig_correct)
                    if(ddbug):print("redundant_correct ",redundant_correct)     
                    if orig_correct==False:
                        if redundant_correct==True:
                            ret=rd[1][0]
                            if self.doCorrection==True:
                                self.traverse( path, True, "close",ret,bid)
                                p['current_version'][bid]+=1
                                self.traverse( path, False, "close", p)
                                print("original corrected at block ", bid)
                        else:
                            if self.doCorrection:print("Incorrect data in both servers")
                            else: 
                                print("Incorrect data in 1 server and adjacent server down")
                                print("or 2 adjacent servers down")
                                raise FuseOSError(errno.ECONNREFUSED)
                    else:
                        if redundant_correct==False:
                            ret=rd[0][0]
                            if self.doCorrection==True:
                                self.traverse( path, True, "close",ret,bid)
                                p['current_version'][bid]+=1
                                self.traverse( path, False, "close", p)
                                print("redundant corrected at block", bid)
                        else:
                            ret=rd[0][0]
                    return ret
            if(with_server and handle=="close"):# and tdata == False):
                if(ddbug): print("server traverse in close ")
                if(ddbug): print("client sending cmd update ", cmd)
                if tdata==False:
                    pcmd= pickle.dumps(cmd)    
                    ser = xmlrpclib.ServerProxy("http://localhost:"+self.metas)
                    ser.put(pcmd, "")
                else:
                    p = self.traverse(path)
                    if(ddbug):print(p)
                    cmd['current_version']=p['current_version'][bid]+1 if len(p['current_version'])-1>=bid else 0
                    if(ddbug):print("############################################################################# version is ", cmd['current_version'])
                    cmd['update']=[update,self.checksum(update)]
                    for sernum in [[SerN,False],[SerRed,True]]:
                        ser = xmlrpclib.ServerProxy("http://localhost:"+self.datas[sernum[0]])
                        cmd['tred']=sernum[1]
                        pcmd= pickle.dumps(cmd)
                        self.blocking_put(ser, pcmd)
                        #ser.put(pcmd, "")
            else: ###d = self.traverse(path, True) is there only in read => no close required but will still be called
                pass           

    def blocking_get(self, ser, pcmd):
        success=False
        while not success:
            success=True
            try:
                rv = ser.get(pcmd)
            except socket.error as e:
                        #success=False
                        cmd=pickle.loads(pcmd)
                        print("cmd is ")
                        print(cmd)
                        if cmd['handle']=="open":
                            rv=['*',0]
                            self.doCorrection=False
                            print("no creection marked")
                            return pickle.dumps(rv)
                        else:
                            print("connection is refused")
                            raise FuseOSError(errno.ECONNREFUSED)
        return rv

    def blocking_put(self, ser, pcmd):
        success=False
        while not success:
            success=True
            try:
                ser.put(pcmd, "")
            except socket.error as e:
                        #success=False
                        raise FuseOSError(errno.ECONNREFUSED)
            

    def traverseparent(self, path, tdata = False, handle="open", update={}, bid=0, Dinit=False, forRen = False):
        """Traverses the dict of dict(self.files) to get pointer
            to the parent directory of the current file.
            Also returns the child name as string"""
        SBid = bid//self.num_D
        SerN = (bid+self.start_d)%self.num_D
        #SerN = (bid%self.num_D)
        #str="".join(path.split("/"))
        #start_d=0
        #for i in range(len(str)): start_d+=ord(str[i])*i
        #start_d=start_d%self.num_D
        #SerN=(SerN+start_d)%self.num_D
        
        SerRed=(SerN+1)%self.num_D
        ret1=[]
        ret2=[]
        if Dinit==True: 
            SBid=-1            
        #if(ddbug): print("start_d is ", start_d)
        if(ddbug): print("SerN is ", SerN)
        if(ddbug): print("start_d is ", self.start_d)
        if(ddbug): print("SerRed is ", SerRed)
        if(ddbug): print("bid is ", bid)
        if(ddbug): print("Sbid is ",SBid)
        bid = SBid
        if(not with_server):
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
        elif(with_server):          
            if(ddbug): print("server traverseparent in ", handle," mode")
            cmd = {'function':'traverseparent','path':path,'tdata':tdata,'handle':handle,'update':update, 'bid' : SBid}
            if(ddbug): print("client sending cmd update ", update)
            if tdata==False:
                ser = xmlrpclib.ServerProxy("http://localhost:"+self.metas)
                pcmd= pickle.dumps(cmd)                 
                if(handle=="open"):
                    ret = pickle.loads(ser.get(pcmd))
                    return ret[0],ret[1]
                elif(handle=="close"):
                        ser.put(pcmd,"")
            else:
                i=0
                for sernum in [[SerN,False],[SerRed,True]]:
                    ser = xmlrpclib.ServerProxy("http://localhost:"+self.datas[sernum[0]])                
                    cmd['tred']=sernum[1]
		    if forRen==True and handle=="close" :
			    #print("here i am --------------",update)
			    cmd['update']=update[i]
                    pcmd= pickle.dumps(cmd)      
                    if(handle=="open"):
                        ret1.append(pickle.loads(self.blocking_get(ser, pcmd)))
                        #ret1.append(pickle.loads(ser.get(pcmd)))
                        #return ret1[0][0],ret1[0][1]
                    elif(handle=="close"):
                        self.blocking_put(ser,pcmd)
                        #ser.put(pcmd,"")
                    i+=1
		if(handle=="open"):
			if (forRen==False):
				return ret1[0][0],ret1[0][1]
			else :
				return ret1[0],ret1[1]
                        

    def checksum(self, string):
        chksum=0
        if(ddbug):print("In checksum function")
        if(ddbug):print("string :",string)  
        for i in range(len(string)): 
            if(ddbug):print("character : ",string[i])
            chksum+=ord(string[i])*(i+1)
        return chksum
    
    def chmod(self, path, mode):
        p = self.traverse(path)   #traverse(self, path, tdata = False, handle="open", update={}, bid=0):
        p['st_mode'] &= 0o770000
        p['st_mode'] |= mode
        self.traverse(path, False, "close", p)
        return 0

    def chown(self, path, uid, gid):
        p = self.traverse(path)
        p['st_uid'] = uid
        p['st_gid'] = gid
        self.traverse(path, False, "close", p)

    def create(self, path, mode):
        p, tar = self.traverseparent(path)
        p['files'][tar] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time(), start_d=random.randrange(0,self.num_D), current_version=[])
        self.fd += 1
        self.traverseparent( path, False, "close",p)

        return self.fd

    def getattr(self, path, fh = None):    
        '''no change in p, hence traverse not closed'''
        try:
            p = self.traverse(path)
        except KeyError:
            raise FuseOSError(ENOENT)
        if(p=={}):
            raise FuseOSError(ENOENT)
        return {attr:p[attr] for attr in p.keys() if(attr != 'files' and attr != 'start_d')}

    def getxattr(self, path, name, position=0):  
        '''no change in p, hence traverse not closed'''
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       ####### Should return ENOATTR

    def listxattr(self, path):    
        '''no change in p, hence traverse not closed'''
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        p, tar = self.traverseparent(path)
        if(ddbug): print("mkdir prints ", p)
        p['files'][tar] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),files={})
        p['st_nlink'] += 1
        
        self.traverseparent(path, False, "close",p) #(self, path, tdata = False, handle="open", update={}, bid=0)
        #traverseparent(self, path, tdata = False, handle="open", update={}, bid=0, Dinit=False, tred=False):
        for i in range(self.num_D):
            #for self.data
            d, d1 = self.traverseparent(path, True,"open", {}, i)
            d[d1] = defaultdict(bytes)
            self.traverseparent( path, True, "close",d, i,True)
        

    def open(self, path, flags):    
        ''' no p no d so no traversal and no close   :D '''
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh): 
        '''both d and p are traversed but not written to so no close'''
        d = []
        
        p = self.traverse(path)
        if(offset>p['st_size'] or p['st_size']==0):
            return ""
        fab= int(math.floor(offset/gblock))
        fabc= offset%gblock
        #fabc=0
        lab=int(math.floor((offset+size-1)/gblock))
        labc=(offset+size-1)%gblock+1
        if(offset+size>p['st_size']):
            lab=int(math.floor((p['st_size']-1)/gblock))
            labc=(p['st_size']-1)%gblock+1

        if(ddbug):print("fab", fab)
        if(ddbug):print("fabc", fabc)
        if(ddbug):print("lab", lab)
        if(ddbug):print("labc", labc)
        for bid in range(fab,lab+1):
            d = d+[self.traverse(path,True,"open", {},bid)]
 
        lab=lab-fab
        fab=0
        if(ddbug): print("lab", lab)
        if(ddbug): print("fab", fab)
        if(ddbug):print("d", d)  
        p1= [d[fab][fabc:]] 
        if(ddbug):print(p1)
        p2= d[fab+1:lab]
        if(ddbug):print(p2)
        p3= [d[lab][0:labc]] if lab>fab else []
        if(ddbug):print(p3)
        return "".join(p1+p2+p3)
        #return "".join(d)[offset:offset + size]




    def readdir(self, path, fh):  
        '''p not modified, no traversal close'''
        p = self.traverse(path)['files']
        return ['.', '..'] + [x for x in p ]




    def readlink(self, path):   
        '''no modification, so no traversal close '''
        return self.traverse(path, True)

    def removexattr(self, path, name):
        '''part of contents of p deleted, hence traverse should be closed'''
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        try:
            del attrs[name]      
        except KeyError:
            pass        # Should return ENOATTR
        self.traverse(path, False, "close", p)

    def rename(self, old, new):
        error=False
        po, po1 = self.traverseparent(old)
        condition=(po['files'][po1]['st_mode'] & 0o770000 == S_IFDIR)
        popped = po['files'].pop(po1)
        popped1=None
        if condition:
            po['st_nlink'] -= 1
        self.traverseparent( old, False, "close",po)
    
    
        pn, pn1 = self.traverseparent(new)
        if(ddbug):print("----------------old",old)
        if(ddbug):print("----------------new",new)
        if(ddbug):print("----------------po",po)
        if(ddbug):print("----------------po1",po1)
        if(ddbug):print("----------------pn",pn)
        if(ddbug):print("----------------pn1",pn1)
    
        pn['files'][pn1] = popped
        if condition:
            pn['st_nlink'] += 1
        self.traverseparent( new, False, "close",pn)

        if(ddbug):print("-------after push---------pn",pn)
        if(ddbug):print("-------after pop---------po",po)
        #(self, path, tdata = False, handle="open", update={}, bid=0, Dinit=False):
	#traverseparent(self, path, tdata = False, handle="open", update={}, bid=0, Dinit=False, forRen = False):
        for i in range(self.num_D):
            do1, do2 = self.traverseparent(old, True, "open",{},i,True,True)
            if(ddbug):print("----------------do1",do1)
            if(ddbug):print("----------------do2",do2)
            try:
                popped1=do1[0].pop(do1[1])
		popped2=do2[0].pop(do2[1])
            except KeyError:
                    error=True
            if(ddbug):print("popped1=====",popped1)
            if(ddbug):print("popped2=====",popped2)
            if(ddbug):print("error=======",error)
            self.traverseparent( old, True, "close",[do1[0],do2[0]],i,True,True)

            dn1, dn2 = self.traverseparent(new, True, "open",{},i,True,True)
            if(ddbug):print("----------------dn1",dn1)
            if(ddbug):print("----------------dn2",dn2)
            if not error:
                dn1[0][dn1[1]] = popped1
                dn2[0][dn2[1]] = popped2
            if(ddbug):print("----------------dn1",dn1)
            if(ddbug):print("----------------dn2",dn2)
            self.traverseparent( new, True, "close",[dn1[0],dn2[0]],i,True,True)
    
    def rmdir(self, path):
        p, tar = self.traverseparent(path)
        if len(p['files'][tar]['files']) > 0:
            raise FuseOSError(ENOTEMPTY)
        p['files'].pop(tar)
        p['st_nlink'] -= 1
        self.traverseparent( path, False, "close",p)
        #traverseparent(self, path, tdata = False, handle="open", update={}, bid=0, Dinit=False, tred=False):
        for i in range(self.num_D):
            #for self.data
            p, tar = self.traverseparent(path, True, "open", {}, i)
            p.pop(tar)
            self.traverseparent( path, True, "close", p , i,True)

    def setxattr(self, path, name, value, options, position=0):
        '''part of p is set to default hence traverse should be closed'''
        p = self.traverse(path)
        attrs = p.setdefault('attrs', {})  
        attrs[name] = value
        self.traverse(path, False, "close", p)


    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    #traverseparent(self, path, tdata = False, handle="open", update={}, bid=0, Dinit=False, tred=False):
    def symlink(self, target, source):
        p, tar = self.traverseparent(target)
        p['files'][tar] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
    
        self.traverseparent( path, False, "close",p)
    #for self.data
        d, d1 = self.traverseparent(target, True)
        d[d1] = source
        self.traverseparent( path, True, "close",d)

    def truncate(self, path, length, fh = None):
        '''see ###'''
        p = self.traverse(path)        
        ab=int(math.ceil(length/gblock)-1)
        abc=(length)%gblock
        if(length<p['st_size']):
            if(abc>0):
                AB=self.traverse(path,True,"open", {},ab)
                AB=AB[:abc]
                self.traverse( path, True, "close",AB,ab)
            #p['current_version']=p['current_version'][:ab+1]
	    for bid in range(ab,len(p['current_version'])):
	    	if len(p['current_version'])!=0:
	    		p['current_version'].pop()
            p['st_size'] = length                       ###length changed so traverse should be closed'''
            self.traverse(path, False, "close", p)
        
    def unlink(self, path):
        p, tar = self.traverseparent(path)
        p['files'].pop(tar)        
        self.traverseparent( path, False, "close",p)
        
        for i in range(self.num_D):
            d , d1 = self.traverseparent(path, True, "open", {}, i)
            try:
                del d[d1]
            except KeyError:
                if(ddbug): print("key error ignored")
                pass
            self.traverseparent( path, True, "close",d, i,True)

    def utimens(self, path, times = None):
        now = time()
        atime, mtime = times if times else (now, now)
        p = self.traverse(path)
        p['st_atime'] = atime
        p['st_mtime'] = mtime
        self.traverse(path, False, "close", p) 

    def write(self, path, data, offset, fh):
        p = self.traverse(path)
        
        fab= int(math.floor((offset)/gblock))
        fabc= offset%gblock
        lab= int(math.floor((offset+len(data)-1)/gblock))
        labc=((offset+len(data))%gblock)*-1
        lb=int(math.ceil(p['st_size']/gblock))
        lbc=(p['st_size']-1)%gblock+1 if p['st_size']!=0 else 0
        fbn=0
        nb=0
        nulls=0

        if(offset>p['st_size']):
            nulls=offset-p['st_size']
            fbn=gblock-lbc if gblock-lbc<nulls else nulls
            nb=int((nulls-fbn-fabc)/gblock)
            
        #print("d[d1]", d[d1])
        #print("len(d[d1])", len(d[d1]))
        if(ddbug):print("fab", fab)
        if(ddbug):print("fabc", fabc)
        if(ddbug):print("lab", lab)
        if(ddbug):print("labc", labc)
        if(ddbug):print("p['st_size']", p['st_size']) 
        if(ddbug):print("lb",lb)
        if(ddbug):print("lbc",lbc)
        if(ddbug):print("nulls",nulls)
        if(ddbug):print("fbn",fbn)    
        if(ddbug):print("nb", nb)
        
        p2=[]
        #p2=['error']
        FB=['']
        bfetch=lb    
        if(offset>p['st_size']):
            if(p['st_size']%gblock>0 or fbn==gblock):
                #bfetch = bfetch if fab+1==lb else lb-1 if lb > 0 else 0
                if fbn < 5:
                    bfetch = lb-1 if lb > 0 else 0 
                    FB[-1]=self.traverse(path,True,"open", {},bfetch)
                if(ddbug):print("FB", FB)
                FB[0]=FB[0][0:]+'\x00'*fbn
                if(ddbug):print("FB", FB)
                p2+=FB
                if(ddbug):print("p2", p2)
            else:
                #bfetch = lb-1
                p2+=['\x00'*fbn]            
            if(len(p2)>0):
                if p2[0]=="": p2.pop(0)
            p2+=['\x00'*gblock]*nb
            if nulls-fbn-gblock*nb>0: p2+=['\x00'*fabc]
            if(len(p2[-1])==0):
                p2=p2[:-1]
            #disabledprint("nulls d[d1]", d[d1])
        else:
            #if(p['st_size']%gblock>0 or fbn==gblock):
            #   bfetch = lb if fab==lb else lb-1 if lb > 0 else 0 
            #else:
            bfetch=fab
            #bfetch = lb-1 if lb > 0 else 0 
        if(ddbug):print("bfetch", bfetch)
    
        #if(not with_server):
        #    if(len(d[d1])==fab and fabc==0 and len(d[d1][-1])==gblock):
        #            d[d1].append('')            
        
            
        #p1=d[d1][0:fab]
        #disabledprint('p1', p1)
        if fabc>0:
            if offset<=p['st_size']:
                FB=self.traverse(path,True,"open", {},fab)
            else:
                FB=p2.pop(-1)
            p2+=[FB[0:fabc]+data[0:gblock-fabc]]
        else:
            p2+=[data[:gblock]]
            if(ddbug):print('p2', p2)

        if(lab-fab>1 or (labc == 0 and lab==fab+1)):
            p2=p2+[data[i:i+gblock] for i in range(gblock-fabc,gblock-fabc+(lab-fab)*gblock, gblock)]
            if(ddbug): print('lab-fab>1, p2:', p2)

        if(labc<0):
            if(lab==fab and p['st_size']!=0 and lab<lb):
                if(ddbug): print("lab, ",lab,"len(p2))", len(p2))
                LB=self.traverse(path,True,"open", {},lab)
                p2[-1]=p2[-1]+LB[-labc:-labc+(gblock+labc)*1]
                if(ddbug): print('labc>-5, lab==fab, p2:', p2)
            elif(lab==fab+1 and labc < 0):
                p2=p2+[data[labc:]]
                if(ddbug): print('labc>-5, lab>fab, p2:', p2)
                if(offset+len(data)<p['st_size']):
                    LB=self.traverse(path,True,"open", {},lab)
                    if(ddbug): print("LB",LB)
                    p2[-1]=p2[-1]+LB[-labc:]
                    if(ddbug): print('labc>-5, write end < f_size, p2:', p2)
            elif(lab>fab+1 and offset+len(data)<p['st_size']):
                LB=self.traverse(path,True,"open", {},lab)
                p2[-1]=p2[-1]+LB[-labc:]
                if(ddbug): print('lab>fab+1, write end > f_size, p2:', p2)
                

        #if(offset+len(data)< p['st_size']-1):
        #    p2=p2+d[d1][lab+1:]
            #disabledprint('write end < f_size, blocks, p2:', p2)
        WB=p2
        
        if(ddbug): print('WB', WB)
        
        
        for bid in range(0,len(WB)):
            self.traverse( path, True, "close",WB[bid],bid+bfetch)
        
        if(offset+len(data)>p['st_size']):
            p['st_size']=offset+len(data)
            for bid in range(lb,lab+1):
                p['current_version'].append(-1)
        for bid in range(fab,lab+1):
            p['current_version'][bid]+=1
        self.traverse(path, False, "close", p)

        return len(data)


if __name__ == '__main__':
    if len(argv) < 2:
        if(ddbug): print('usage: %s <mountpoint> metaserver_port ' % argv[0])
        exit(1)

    if(ddbug):
        logging.basicConfig(level=logging.DEBUG)
    if (with_server):
        fuse = FUSE(Memory(argv[2:]), argv[1], foreground=True, debug=ddbug)
    else:
        fuse = FUSE(Memory(argv[2:]), argv[1], foreground=True, debug=ddbug)
