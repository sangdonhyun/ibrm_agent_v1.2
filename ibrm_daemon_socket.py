#-*- coding: utf-8 -*-
#!/usr/bin/python

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
import job_monitor
import log_control
import log_clear
import fbrm_nfs
import ibrm_agent_info

com = common.Common()
log = log_control.LogControl()
#log = ibrm_logger.ibrm_logger().logger('ibrm_agent_log')
############################################################################
def thead_agent_init():
    ibrm_agent_info.AgentInitialization().main()
    
def getInfo():
    cfg=ConfigParser.RawConfigParser()
    cfgFile=os.path.join('config','config.cfg')
    cfg.read(cfgFile)

    print 'HOST',cfg.get('socket','HOST')
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
        #log = ibrm_logger.ibrm_logger().logger('ibrm_agent_log')
        """ Method that runs forever """

        # Do something
        print('Doing something imporant in the background')
       
        if 'session_key' in self.args:
            a_args = self.args
            ora_sid = a_args['ora_sid']
            pid = str(a_args['pid'])
            session_key = str(a_args['session_key'])
            job_id = str(a_args['job_id'])
            shell_name = str(a_args['shell_name'])
            shell_type = a_args['shell_type']
            db_name = a_args['db_name']
            sysdate_str = a_args['sysdate']
            tg_job_dtl_id = str(a_args['tg_job_dtl_id'])
            job_monitor.job_mon(ora_sid, pid, session_key,job_id,shell_name,shell_type,db_name,sysdate_str,tg_job_dtl_id).main()
        else:
            job_submit.job_submit(job_id=self.args['job_id']
                              , shell_name=self.args['shell_name']
                              , shell_type=self.args['shell_type']
                              , shell_path=self.args['shell_path']
                              , db_name=self.args['db_name']
                              , ora_sid=self.args['ora_sid']
                              , tg_job_dtl_id=self.args['tg_job_dtl_id']).main()
        time.sleep(self.interval)
        print ('job excute OK')
        

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

        shell_controler = shell_control.shell_contorl()

        # self.request is the client connection
        data = self.request.recv(1024)  # clip input at 1Kb
        #log.info(data)
        cip,cport= self.client_address
        msg='[%s] CONNECT PORT %s'%(cip,cport)
        #f_list_str = str(self.get_list())

        try:

            info=ast.literal_eval(data)

            if info['FLETA_PASS'] != 'kes2719!':
                self.request.close()
                msg=self.client_address + ' PASSWORD INCOREECT'
                #log.info(msg)
            else:

                cmd=info['CMD']
              
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
                        #20220916 hyun
                        #pedwdb75 arg shell_path 포함.
                        #shell_path 는 shell_controler.py 에서 가져오는걸로
                        #shell_detail_data = shell_controler.shell_detail(db_name,shell_name,shell_path)
                        shell_detail_data = shell_controler.shell_detail(db_name,shell_name)
                        print '-'*40
                        print shell_detail_data
                        self.request.send(str(shell_detail_data))

                elif cmd == 'AGENT_JOB_EXCUTE':
                    print '#'*50
                    args = info['ARG']
                    print args
                    job_id = args['job_id']
                    shell_name = args['shell_name']
                    shell_type = args['shell_type']
                    shell_path = args['shell_path']
                    db_name = args['db_name']
                    ora_sid  = args['ora_sid']
                    tg_job_dtl_id = args['tg_job_dtl_id']

                    cmd = 'python ./job_submit.py {} {} {} {} {} {} {}'.format(job_id,shell_name,shell_type,db_name,ora_sid,tg_job_dtl_id, shell_path)

                    job_info={}
                    job_info['job_id'] = job_id

                    bg_thread(args=args, interval=1)
                    time.sleep(0.5)
                    
                    time.sleep(0.5)

                    ret_data = {}
                    ret_data['FLETA_PASS'] = 'kes2719!'
                    ret_data['CMD'] = 'JOB_STATUS'
                    ret_data['ARG'] = {}
                    ret_data['ARG'] = {'JOB_STATUS':'Running'}
                    self.request.send(str(ret_data))
                
                elif cmd == 'AGENT_NFS_INFO':
                    args = info['ARG']
                    if data is not None:

                        json_data = fbrm_nfs.fs().main()
                        self.request.send(str(json_data))

                elif cmd == 'AGENT_INFO_UPDATE':
                    args = info['ARG']
                    if data is not None:
                        try:
                            ibrm_agent_info.AgentInitialization().main()
                            self.request.send("OK")
                        except:
                            pass

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

                        ibrm_status['IBRM_PATH'] = ibrm_path

                        v_file = os.path.join('config', 'version.txt')
                        if os.path.isfile(v_file):
                            with open(v_file) as f:
                                lineset=f.readlines()
                            vline=lineset[-1]
                            version='unkown'
                            patch_dt='unkown'
                            if ',' in vline:
                                version=vline.split(',')[0]
                                patch_dt = vline.split(',')[1]
                        else:
                            version = 'unkown'
                            patch_dt = 'unkown'
                        ibrm_status['IBRM_AGENT_VERSION'] = version
                        ibrm_status['IBRM_AGENT_PATCH_DT'] = patch_dt
                        ret_data['ARG'] = ibrm_status
                    except Exception as e:
                        log.logdata('AGENT', 'ERROR', '90000', 'ibrm_path error or get version error')
                    self.request.send(str(ret_data))
                    log_clear.clear().main()
                
                elif cmd == 'JOB_MON_CHECK':
                    try:
                        if data is not None:
                            a_args = info['ARG']
                            if len(a_args) > 0:
                                for a_running_job_proc in a_args:
                                    bg_thread(args = a_running_job_proc, interval=1)
                                    time.sleep(2)
                    except:
                        pass
                
                elif cmd == 'JOB_MONITOR':
                
                    if data is not None:
                        a_args = info['ARG']
                        a_index_key = [ 
                                         'sysdate'
                                       , 'job_id'
                                       , 'pid'
                                       , 'host'
                                       , 'db_name'
                                       , 'tg_job_dtl_id'
                                       , 'session_key'
                                       , 'ora_sid'
                                       , 'shell_type'
                                    ]
                        a_difference = list(set(a_index_key) - set(a_args))
                        
                        if len(a_difference) > 0:
                            s_diff = ','.join(a_difference)
                            a_args['result'] = 'Fail'
                            a_args['message'] = 'difference key - %s' %(s_diff)

                        elif not all(a_args.values()):
                            a_args['result'] = 'Fail'
                            a_args['message'] = 'Exception error null data value'

                        else:
                            bg_thread(args=a_args, interval=1)
                            a_args['result'] = 'Success'
                            a_args['message'] = 'OK'
                    self.request.send(str(a_args))
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
    try:
       
        t = Thread(target=thead_agent_init)
        t.daemon = True  
        t.start()
        #t.join(timeout=5)
    except Exception as e:
        print(e)
        pass
    inst_path= os.path.dirname(os.path.abspath(__file__))

    if not os.path.isdir(os.path.join(inst_path, 'data')):
        os.mkdir('data')
    if not os.path.isdir(os.path.join(inst_path, 'log')):
        os.mkdir('log')
    if not os.path.isdir(os.path.join(inst_path, 'logs')):
        os.mkdir('logs')

    HOST,PORT=getInfo()
    msg= com.getHeadMsg('START iBRM Agent Socket Daemon(%s,%s)'%(HOST,PORT))
    print (msg)
    log.logdata('AGENT', 'INFO', '50001', str(HOST))
    server = Server((HOST, PORT), TCPConnectionHandler)
    # terminate with Ctrl-C
    server.serve_forever()