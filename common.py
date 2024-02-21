'''
Created on 2012. 10. 12.

@author: Administrator
'''
'''
Created on 2012. 9. 27.

@author: Administrator
'''
import sys
import os
import datetime
import time
import ConfigParser
from ftplib import FTP
import re
import logging
import glob
import logging.handlers
import base64
import hashlib
import pprint
class hangul_dict(pprint.PrettyPrinter):
    def format(self, _object, context, maxlevels, level):
        if isinstance(_object, unicode):
            return "'%s'" % _object.encode('utf8'), True, False
        elif isinstance(_object, str):
            _object = unicode(_object,'utf8')
            return "'%s'" % _object.encode('utf8'), True, False
        return pprint.PrettyPrinter.format(self, _object, context, maxlevels, level)

class Decode():

    def _en(self, _in):
        return base64.b64encode(_in)

    def _de(self, _in):
        return base64.b64decode(_in)

    def fenc(self, str):
        e0 = self._en(str)
        e1 = self._en(e0)
        m = hashlib.md5(e0).hexdigest()
        e1 = e1.replace('=', '@')
        e = e1 + '@' + m
        return e

    def fdec(self, e):
        r = e.rfind('@')
        if r == -1:
            pass
        d1 = e[:r]
        d1 = d1.replace('@', '=')
        d0 = self._de(d1);
        d = self._de(d0);
        return d

    def decBit(self, fileName):
        with open(fileName) as f:
            tmp = f.read()
        if re.search('###\*\*\*', tmp):
            return True
        else:
            return False

    def fileDec(self, fileName):
        if self.decBit(fileName):
            return None
        with open(fileName) as f:
            str = f.read()
        with open(fileName, 'w') as f:
            f.write(self.fdec(str))
        return self.decBit(fileName)

    def fileDecReText(self, fileName):
        if self.decBit(fileName):
            with open(fileName) as f:
                reList = f.read()

        else:
            with open(fileName) as f:
                str = f.read()
            reList = self.fdec(str)

        return reList

    def fileEncDec(self, fileName):
        if self.decBit(fileName) == False:
            return None
        with open(fileName) as f:
            str = f.read()
        with open(fileName, 'w') as f:
            f.write(self.fenc(str))
        return self.decBit(fileName)

class Common():
    def __init__(self):
        self.cfg = self.getCfg()
        self.s_agent_path = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])))
        self.s_sql_path = os.path.join(self.s_agent_path, 'sql')
        
        if os.path.isdir(self.s_sql_path) :
            if oct(os.stat(self.s_sql_path).st_mode)[-3:] != '707':
                os.chmod(self.s_sql_path, 0o0707)
        
    def exec_command(self, s_commmand):
        return os.popen(s_commmand).read().strip()

    def ora_list_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config','list.cfg')
        cfg.read(cfg_file)
        return cfg

    def get_ora_home(self,ora_sid):
        ora_home = ''
        try:
            ora_cfg=self.ora_list_cfg()
            ora_home = ora_cfg.get(ora_sid,'ORACLE_HOME')
            print(ora_home)
        except Exception as e:
            print('EEEEEEEEEEEEEEE')
            print(str(e))
            print('EEEEEEEEEEEEEEE')
            pass
        return ora_home

    def ora_home_str(self):
        return self.cfg.get('common','ora_home_str')
    
    def getCfg(self):
        config = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','config.cfg')
        config.read(cfgFile)
#         self.fletaRecv=config.get('fletaRecv','ip')
#         self.user=config.get('fletaRecv','user')
#         self.passwd=Decode().fdec(config.get('fletaRecv','passwd'))
        return config

    def getNow(self,format='%Y-%m-%d %H:%M:%S'):
        return time.strftime(format)

    def getHeadMsg(self,title='FLETA BATCH LAOD'):
        now = self.getNow()
        msg = '\n'
        msg += '#'*79+'\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#### '+('TITLE     : %s'%title).ljust(71)+'###\n'
        msg += '#### '+('DATA TIME : %s'%now).ljust(71)+'###\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#'*79+'\n'
        return msg

    def getEndMsg(self):
        now = self.getNow()
        msg = '\n'
        msg += '#'*79+'\n'
        msg += '####  '+('END  -  DATA TIME : %s'%now).ljust(71)+'###\n'
        msg += '#'*79+'\n'
        return msg

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config', 'config.cfg')
        cfg.read(cfg_file)
        return cfg

if __name__=='__main__':
#    logTest()
#    fname=os.path.join('data','fs_ibk-test05.tmp')
#    print os.path.isfile(fname)
#    print Common().fletaPutFtp(fname,'diskinfo.SCH')
    pass

