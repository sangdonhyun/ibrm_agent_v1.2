#-*- coding: utf-8 -*-
"""
* *******************************************************
* Copyright (c) Fleta Communications, Inc. 2020. All Rights Reserved.
* *******************************************************
* create date :
* modipy date : 2021-12-01
"""
__author__ = 'TODO: muse@fletacom.com'
__ibrm_agent_version__ = 'TODO: v1.2'

import SocketServer
import subprocess
import sys
from threading import Thread
import time
import re
import ast
import os
import common
import shutil
import ConfigParser
import datetime
import glob
import shell_control
import threading
import subprocess
import shlex
import job_submit
import log_clear
import fbrm_nfs
import fs_nfs

com = common.Common()


############################################################################
def getInfo():
    cfg=ConfigParser.RawConfigParser()
    cfgFile=os.path.join('config','config.cfg')
    cfg.read(cfgFile)
    HOST,PORT=cfg.get('socket','HOST'),cfg.get('socket','PORT')
    return HOST,int(PORT)


def shell_path():
    return "/u01/SCRIPTS/Database/IBRM/RMAN/SCHEDULE"



class bg_thread(object):
    """ Threading example class
    The run() method will be started and it will run in the background
    until the application exits.
    """

    def __init__(self,args, interval=1):
        """ Constructor
        :type interval: int
        :param interval: Check interval, in seconds
        """
        self.interval = interval

        self.args = args
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution
        time.sleep(interval)


    def run(self):

        """ Method that runs forever """

        # Do something
        print('Doing something impotant in the background')
        #subprocess.call(shlex.split(self.cmd), stdout=subprocess.PIPE, shell=True)
        #print os.popen(self.cmd).read()
        #subprocess.Popen(shlex.split(self.cmd), stdout=subprocess.PIPE)
        job_submit.job_submit(job_id=self.args['job_id'], shell_name=self.args['shell_name'], shell_type=self.args['shell_type'], shell_path=self.args['shell_path'],db_name=self.args['db_name'], ora_sid=self.args['ora_sid'],tg_job_dtl_id=self.args['tg_job_dtl_id']).main()
        time.sleep(self.interval)
        print (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print ('job excute OK')



    #proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    #return proc

############################################################################
'''  One instance per connection.
     Override handle(self) to customize action. '''

class TCPConnectionHandler(SocketServer.BaseRequestHandler):

    def getCfg(self):
        
        cfg = ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','config.cfg')
        cfg.read(cfgFile)
        return cfg

    def get_list(self):
        pylist=glob.glob(os.path.join('SHELL','*.sh'))
        shell_list=[]
        for s in pylist:

            shell_dict={}
            name = os.path.basename(s)
            a_time = time.ctime(os.path.getatime(s))
            c_time = time.ctime(os.path.getctime(s))
            m_time = time.ctime(os.path.getmtime(s))
            atime_obj = datetime.datetime.strptime(a_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            ctime_obj = datetime.datetime.strptime(c_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            mtime_obj = datetime.datetime.strptime(m_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            shell_dict['name'] = name
            shell_dict['a_time'] = atime_obj
            shell_dict['c_time'] = ctime_obj
            shell_dict['m_time'] = mtime_obj
            shell_list.append(shell_dict)

        return {'shell_list':shell_list}

    def handle(self):
        print (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print ('='*50)
        shell_controler = shell_control.shell_contorl()

        # self.request is the client connection
        data = self.request.recv(1024)  # clip input at 1Kb

        cip,cport= self.client_address
#         print type(self.request),self.request,dir(self.request)
        msg='[%s] CONNECT PORT %s'%(cip,cport)

        f_list_str = str(self.get_list())



        try:

            info=ast.literal_eval(data)

            if info['FLETA_PASS'] != 'kes2719!':
                self.request.close()
                msg=self.client_address + ' PASSWORD INCOREECT'

            else:

                cmd=info['CMD']
                print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print data
                print 'CMD :',cmd
                if cmd == 'AGENT_SHELL_LIST':

                    if data is not None:
                        args = info['ARG']
                        db_name = args['db_name']
                        use_path = args['use_path']
                        path = args['path']

                        if use_path =='N':
                            path = self.getCfg().get('common','shell_path')
                        if '{DB_NAME}' in path:
                            path = path.replace('{DB_NAME}',db_name)

                        if not os.path.isdir(path):
                            shell_data = {'status':'{} path not exist '.format(path)}
                        else:
                            shell_data = shell_controler.shell_list(db_name,path)
                        self.request.send(str(shell_data))

                elif cmd == 'AGENT_SHELL_DETAIL':
                    shell_args = info['ARG']
                    if data is not None:

                        db_name = shell_args['db_name']
                        shell_name = shell_args['shell_name']
                        shell_path = shell_args['shell_path']
                        print('shell_name :',shell_name)
                        print('db_name :', db_name)
                        print('shell_path :', shell_path)
                        shell_detail_data = shell_controler.shell_detail(db_name,shell_name,shell_path)
                        self.request.send(str(shell_detail_data))

                elif cmd == 'AGENT_JOB_EXCUTE':

                    args = info['ARG']
                    job_id = args['job_id']
                    shell_name = args['shell_name']
                    shell_type = args['shell_type']
                    shell_path = args['shell_path']
                    db_name = args['db_name']
                    ora_sid  = args['ora_sid']
                    tg_job_dtl_id = args['tg_job_dtl_id']


                    #ibrm_excute.ibrm_excute(job_id=job_id, shell_name=shell_name, shell_type=shell_type, db_name=db_name,ora_sid=ora_sid).job_exec()
                    """
                    job_id = arg[1]
                    shell_name = arg[2]
                    shell_type = arg[3]
                    db_name = arg[4]
                    ora_sid = arg[5]
                    """
                    cmd = 'python ./job_submit.py {} {} {} {} {} {}'.format(job_id,shell_name,shell_type,db_name,ora_sid,tg_job_dtl_id)

                    print 'cmd :',cmd

                    job_info={}
                    job_info['job_id'] = job_id
                    #job_submit(job_id=job_id,shell_name=shell_name,shell_type=shell_type,db_name=db_name,ora_sid=ora_sid).main()
                    bg_thread(args=args, interval=1)
                    time.sleep(0.5)
                    print('JOB SUBMIT ====>')
                    time.sleep(0.5)


                    #t = threading.Thread(target=job_excute, args=(cmd,))
                    #t.start()
                    #import job_submit_test
                    #pid, mpid = job_submit_test.job_submit_test(ora_sid=ora_sid, db_name=db_name, shell_type=shell_type,  shell_name=shell_name, job_id=job_id).main()
                    #print pid, mpid


                    ret_data = {}
                    ret_data['FLETA_PASS'] = 'kes2719!'
                    ret_data['CMD'] = 'JOB_STATUS'
                    ret_data['ARG'] = {}
                    ret_data['ARG'] = {'JOB_STATUS':'Running'}
                    print 'return data :',ret_data
                    self.request.send(str(ret_data))

                elif cmd == 'ORACLE_JOB_STATUS':
                    args = info['ARG']
                    session_stamp = args['sessioin_stamp']
                elif cmd == 'AGENT_JOB_TEST':
                    args = info['ARG']
                    if data is not None:
                        self.request.send(str(args))
                elif cmd == 'AGENT_NFS_INFO':
                    args = info['ARG']
                    if data is not None:
                        json_data = fbrm_nfs.fs().main()
                        print 'json_data :',json_data
                        self.request.send(str(json_data))



                elif cmd == 'AGENT_FBRM_FS_INFO':
                    args = info['ARG']
                    if data is not None:
                        json_data=dict()

                        bg_thread(cmd='fbrm_fs', interval=1)
                        json_data['fbrm_fs'] = 'Run-OK'
                        self.request.send(str(json_data))


                elif cmd == 'AGENT_HEALTH_CHECK':
                    ret_data = {}
                    ret_data['FLETA_PASS'] = 'kes2719!'
                    ret_data['CMD'] = 'AGENT_HEALTH_CHECK'
                    ibrm_status = {}
                    ret_data['ARG'] = {}
                    ibrm_status['CHECKING']='OK'
                    #ret_data['ARG'] = {'CHECKING': 'OK'}
                    try:
                        cfg=ConfigParser.RawConfigParser()
                        cfg_file = os.path.join('config','config.cfg')
                        cfg.read(cfg_file)
                        ibrm_path=cfg.get('common','ibrm_path')
                        print 'ibrm_path :',ibrm_path
                        ibrm_status['IBRM_PATH'] = ibrm_path
                        with open(os.path.join('config','version.txt')) as f:
                            lineset=f.readlines()
                        vline=lineset[-1]
                        version='unkown'
                        patch_dt='unkown'
                        if ',' in vline:
                            version=vline.split(',')[0]
                            patch_dt = vline.split(',')[1]
                        ibrm_status['IBRM_AGENT_VERSION'] = version
                        ibrm_status['IBRM_AGENT_PATCH_DT'] = patch_dt
                        ret_data['ARG'] = ibrm_status
                    except Exception as e:
                        print str(e)

                    print 'ret_data :',ret_data
                    self.request.send(str(ret_data))
                    log_clear.clear().main()
                elif cmd == 'FS_NFS':
                    ret_data = {}
                    ret_data['FLETA_PASS'] = 'kes2719!'
                    ret_data['CMD'] = 'FS_NFS'
                    ibrm_status = {}
                    ret_data['ARG'] = {}
                    ibrm_status['CHECKING'] = 'OK'
                    # ret_data['ARG'] = {'CHECKING': 'OK'}
                    try:
                        nfs_list=fs_nfs.fs().get_nfs_list()
                    except Exception as e:
                        print str(e)
                        nfs_list = []





                self.request.close()
        except:
            self.request.close()
############################################################################

class Server(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    # Ctrl-C will cleanly kill all spawned threads
    daemon_threads = True
    # much faster rebinding
    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass):
        SocketServer.TCPServer.__init__(\
        self,\
        server_address,\
        RequestHandlerClass)
        sip,sport= server_address




if __name__ == "__main__":
    HOST,PORT=getInfo()
    import common
    com = common.Common()
    msg= com.getHeadMsg('START iBRM Agent Socket Daemon(%s,%s)'%(HOST,PORT))
    print datetime.datetime.now().strftime('%Y-%m-%h %H:%M:%S')
    print (msg)
    server = Server((HOST, PORT), TCPConnectionHandler)
    # terminate with Ctrl-C
    server.serve_forever()

