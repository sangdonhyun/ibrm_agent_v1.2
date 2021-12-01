"""
* *******************************************************
* Copyright (c) Fleta Communications, Inc. 2020. All Rights Reserved.
* *******************************************************
* create date :
* modipy date : 2021-12-01
* python version : 2.7
"""
__author__ = 'TODO: muse@fletacom.com'
__ibrm_agent_version__ = 'TODO: v1.2'
import os
import ConfigParser
import glob
import time
import datetime
import json
import ast
import common

class shell_contorl():
    def __init__(self):
        self.cfg = self.get_cfg()
        self.shell_path = self.cfg.get('common','shell_path')
        self.hangul_dic = common.hangul_dict()

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config', 'config.cfg')
        cfg.read(cfgFile)
        return cfg


    def shell_detail(self,db_name,shell_name,s_path):
        if s_path == '':
            shell_path = self.shell_path.replace('{DB_NAME}',db_name)
        else:
            shell_path = s_path
        print 'shell_name :',shell_name,os.path.isfile(os.path.join(shell_path, shell_name))

        with open(os.path.join(shell_path, shell_name)) as f:
            shell_detail = f.read()
        return_data = {"shell_detail": shell_detail}
        #return_data = self.hangul_dic.pprint(return_data)
        input = json.dumps(return_data,ensure_ascii=False)
        print return_data
        return str(input)

    def shell_list(self,db_name,shell_path=None):
        """
        shFileNm
shPath
owner
fsCreDt
fsModDt
prmsn
:return:
        """

        if not shell_path == None:
            self.shell_path = shell_path
        else:
            shell_path  = self.cfg.get('common','shell_path')

        shell_path = self.shell_path.replace('{DB_NAME}',db_name)
        print shell_path
        pylist = glob.glob(os.path.join(self.shell_path, '*.sh'))
        #print pylist
        shell_list = []
        for s in pylist:
            shell_dict = {}
            name = os.path.basename(s)
            path, filename = os.path.split(s)
            a_time = time.ctime(os.path.getatime(s))
            c_time = time.ctime(os.path.getatime(s))
            m_time = time.ctime(os.path.getatime(s))
            atime_obj = datetime.datetime.strptime(a_time, '%a %b %d %H:%M:%S %Y').strftime('%Y%m%d%H%M%S')
            ctime_obj = datetime.datetime.strptime(c_time, '%a %b %d %H:%M:%S %Y').strftime('%Y%m%d%H%M%S')
            mtime_obj = datetime.datetime.strptime(m_time, '%a %b %d %H:%M:%S %Y').strftime('%Y%m%d%H%M%S')
            shell_dict["shFileNm"] = filename
            shell_dict["shPath"] = path
            # shell_dict["fsAccDt"] = atime_obj
            shell_dict["fsCreDt"] = ctime_obj
            shell_dict["fsModDt"] = mtime_obj
            cmd = "ls -alt %s | awk '{print $1}'" % (s)
            os.popen(cmd).read()
            permission = os.popen(cmd).read().strip()
            cmd = "ls -alt %s | awk '{print $3}'" % (s)
            owner = os.popen(cmd).read().strip()
            shell_dict["prmsn"] = permission
            shell_dict["owner"] = owner
            shell_list.append(shell_dict)

        return_data = {"shell_list": shell_list}
        return_string = json.dumps(return_data)



        return str(return_data)


if __name__=='__main__':
    shell_contorl().shell_detail('IBRM','IBRM_Incr_Level0.sh')
