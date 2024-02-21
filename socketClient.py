#-*- coding: utf-8 -*-
'''
Created on 2014. 4. 16.

@author: Administrator
'''
import threading, time
import socket
import os
import common
import ast
import glob
import ConfigParser
import random
from ibrm_logger import ibrm_logger

o_log = ibrm_logger().logger('ibrm_agent_log')
class SocketSender():
    def __init__(self,**kwargs):
        #o_log = ibrm_logger().logger('ibrm_agent_log')
        
        self.filieName=kwargs['FILENAME']
        self.dir = kwargs['DIR']
        self.endCheck = kwargs['ENDCHECK']

        self.dec = common.Decode()
        self.cfg = self.getInfo()
    
    
    def getInfo(self):
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','config.cfg')
        cfg.read(cfgFile)
        
        self.HOST=cfg.get('file_server','ip')
        self.PORT=cfg.get('file_server','port')
        return cfg
    def getNow(self,format='%Y-%m-%d %H:%M:%S'):
        return time.strftime(format)
    
    def getReTryCnt(self):
        try:
            cnt= int(self.cfg.get('fleta_agent','sock_retry_cnt'))
        except:
            cnt = 5
        return cnt
    
    def send(self):
        HOST = self.HOST
        try:
            PORT = int(self.PORT)
        except:
            PORT=54001
        
        fname = self.filieName
        info={}
        info['FLETA_PASS']='kes2719!'
        info['FILENAME']=os.path.basename(fname)
        info['DIR']=self.dir
        info['FILESIZE']=os.path.getsize(fname)
        info['ENDCHECK']=self.endCheck
        
        o_log.info("recv server info : HOST : {} , PORT : {}".format(HOST,str(PORT)))

        dec=common.Decode()
        data=dec.fenc(str(info))

        # Create a socket (SOCK_STREAM means a TCP socket)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sBit=False
        
        try:
            # Connect to server and send data
            sock.connect((HOST, PORT))
            sock.sendall(data + "\n")
        
            # Receive data from the server and shut down
            received = sock.recv(1024)
            
            if received=='READY' or received == b'READY':
                with open(fname,'rb') as f:
                    data=f.read()
                    if len(data) > 0:
                        sock.sendall(data)
                        o_log.info("send all data [%s] completed" %(fname))
                    else:
                        o_log.error("send file data size 0 %s" %(str(e)))
                sBit = True
            else:
                o_log.error("received data Not 'READY' ERROR ==> %s" %(str(received)))
        except socket.error as e:
            sBit = False
            o_log.error("recv SEND EXCEPTION ERROR %s" %(str(e)))
        finally:
            o_log.info("""
###########################################
# DATE       : %s
# RECEIVER   : %s
# FILE NAME  : %s
# FILE SIZE  : %s
# DIR        : %s
# CHECK      : %s
###########################################
            """%(self.getNow(), self.HOST+':'+str(self.PORT), fname, str(os.path.getsize(fname)),self.dir, str(sBit))
            )
            sock.close()
        
        return sBit
    
    def main(self):
        reCnt=self.getReTryCnt()
        cnt=0
        
        while 1:
            sBit=self.send()
            if sBit :
                break
            else:
                o_log.error('FLETA SERVER : %s , PORT : %s SEND FILE ERROR RETRY (%d/%d) '%(self.HOST,self.PORT,cnt+1,reCnt))
            cnt += 1
            if reCnt == cnt:
                break 
            
            time.sleep(random.randint(5,10))
