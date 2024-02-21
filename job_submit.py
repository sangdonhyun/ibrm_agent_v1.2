# -*- encoding:utf-8*-
import threading
import subprocess
import shlex
import os
import time
import threading
import re
import v_rman_control
import sys
import ConfigParser
#import ibrm_logger
import ibrm_server_daemon_send
import job_monitor
import datetime
import job_monitor_ndb
#from subprocess import check_output
import log_control
import platform

print("Main Thread")
log = log_control.LogControl()


def job_submit(cmd):
    subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, shell=True)


class bg_thread(object):
    """ Threading example class
    The run() method will be started and it will run in the background
    until the application exits.
    """

    def __init__(self, cmd, interval=1):
        """ Constructor
        :type interval: int
        :param interval: Check interval, in seconds
        """
        self.interval = interval
        self.cmd = cmd
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution
        time.sleep(interval)
        log.logdata('AGENT', 'INFO', '60001', str(cmd))

    def run(self):
        """ Method that runs forever """
        # Do something
        print('Doing JOB_SUBMIT in the background')
        subprocess.call(shlex.split(self.cmd), stdout=subprocess.PIPE, shell=False)
        time.sleep(self.interval)
       

class job_submit():
    def __init__(self, **kwargs):

        self.cfg = self.get_cfg()
        self.ora_sid = kwargs['ora_sid']
        self.db_name = kwargs['db_name']
        self.shell_type = kwargs['shell_type']
        self.job_id = kwargs['job_id']
        self.shell_name = kwargs['shell_name']

        self.shell_path = kwargs['shell_path']
        self.tg_job_dtl_id = kwargs['tg_job_dtl_id']
        self.rman_control = v_rman_control.rman_control(shell_type=self.shell_type
                                                        ,db_name=self.db_name
                                                        ,ora_sid=self.ora_sid
                                                        ,job_id=self.job_id
                                                        ,tg_job_dtl_id=self.tg_job_dtl_id
                                                    )
        self.name = os.path.basename(sys.argv[0])
        self.HOST = self.cfg.get('ibrm_server', 'ip')
        self.PORT = self.cfg.get('ibrm_server', 'socket_port')
        self.owner = self.get_owner()
        self.check_bit = self.check_shell()

    def get_owner(self):
        try:
            cmd = "ps -ef | grep ora_pmon_%s | grep -v grep | awk '{print $1}'"%self.ora_sid
            owner = os.popen(cmd).read().strip()
        except:
            owner = 'oracle'
        print(cmd)
        print('owner : ', owner)
        return owner

    def check_shell(self):
        if platform.system() == 'Linux':
            cmd = 'ps -ef | grep {SHELL_NAME} | grep "{ORA_SID};" | grep -v grep |grep -v su | grep -v python | wc -l'.format(
                SHELL_NAME=self.shell_name, ORA_SID=self.ora_sid)
        else:
            cmd = 'ps -ef | grep {SHELL_NAME} | grep "{ORA_SID}" | grep -v grep |grep -v su | grep -v python | wc -l'.format(
                SHELL_NAME=self.shell_name, ORA_SID=self.ora_sid)
        ret = os.popen(cmd).read()
       
        check_bit = False
        if int(ret) > 0:
            job_fail_info = {}
            job_fail_info['job_id'] = self.job_id
            job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
            job_fail_info['memo'] = 'this job ({})  is already running'.format(self.shell_name)
           
            ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).job_already_exist_run(job_fail_info)
            log.logdata('AGENT', 'ERROR', '60001', str(job_fail_info['memo']))
            check_bit = False
        else:
            check_bit = True

        #2022.11.15 삼성카드 적용 shell_path = self.shell_path @jhbae
        shell_path = self.shell_path
        if not os.path.isfile(os.path.join(shell_path, self.shell_name)):
            job_fail_info = {}
            job_fail_info['job_id'] = self.job_id
            job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
            job_fail_info['memo'] = 'shell_file {} is not exist as path({})'.format(self.shell_name, shell_path)
          
            log.logdata('AGENT', 'ERROR', '60001', str(job_fail_info['memo']))
            ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).job_submit_fail(job_fail_info)
            check_bit = False
        return check_bit

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config', 'config.cfg')
        cfg.read(cfg_file)
        return cfg

    def process_status(self, pid):
        ps_list = []
        cmd = "ps -ef | grep pid |grep %s| grep -v grep" %(self.ora_sid)
        ret = os.popen(cmd).read()
        print(cmd)
        print('ret : ', ret)
        for line in ret.spltilines():
            lineset = line.split()
            if len(lineset) > 1:
                ps = lineset[1]
                if ps not in ps_list:
                    ps_list.append(ps)
    '''
    def get_pid_gid(self, shell_name):
        try:
            return check_output(["pidof", "-s", shell_name])
        except:
            return None
    '''
    def get_pid_old(self, shell_name):
        cnt = 0
        s_pid = None
        while True:
            cnt = cnt + 1
            time.sleep(3)
            if platform.system() == 'Linux':
                cmd = "ps -ef | grep root | grep %s | grep %s | egrep -v \"grep|python\" | awk '{print $2}'" % (self.owner, shell_name)
            else:
                cmd = "ps -ef | grep %s | grep %s  | egrep -v \"grep|python\" | awk '{print $2}'" % (self.owner, shell_name)

            ret = os.popen(cmd).read().strip()

            a_ret_pid = ret.splitlines()
            if len(a_ret_pid) > 0 :
                if len(a_ret_pid) == 1:
                    s_pid = a_ret_pid[0]
                    if re.findall('^[0-9]+$', s_pid):
                        break
                elif len(a_ret_pid) > 1:
                    job_fail_info = {}
                    job_fail_info['job_id'] = self.job_id
                    job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
                    job_fail_info['memo'] = '1 more process'
                    ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).submit_fail(job_fail_info)
                    break
            else:
                time.sleep(5)
            if cnt > 10:
                job_fail_info = dict()
                job_fail_info['job_id'] = self.job_id
                job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
                job_fail_info['memo'] = 'this job ({})  submit fail, count over'.format(self.shell_name)
                ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).submit_fail(job_fail_info)
                break
            print(cnt)
        return s_pid

    def get_pid(self, shell_name):
        cnt = 0
        pid = None
        time.sleep(0.5)
        cnt = 0
        while True:
            if os.path.isfile('pid.txt'):
                with open('pid.txt') as f:
                    pid = f.read()
            time.sleep(1)
            cnt = cnt + 1
            if cnt == 20:
                break
        if pid == None:
            job_fail_info = {}
            job_fail_info['job_id'] = self.job_id
            job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
            job_fail_info['memo'] = 'this job ({})  submit fail'.format(self.shell_name)
            ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).submit_fail(job_fail_info)
        # os.remove('pid.txt')
        return pid

    def send_job_monitor(self, monitor_info):
        try:
            print 'MONITOR :', monitor_info
            ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).job_monitor(monitor_info)
        except:
            pass

    def main(self):
        old_session_id = self.rman_control.get_last_session_id()
        log.logdata('AGENT', 'INFO', '80000', '#'*50)
        log.logdata('AGENT', 'INFO', '80000', 'JOB_SUBMT START')
        # cmd = "python ./ibrm_excute.py {JOB_ID} {SHELL_NAME} {SHELL_TYPE} {DB_NAME} {ORA_SID}".format(JOB_ID=self.job_id,SHELL_NAME=self.shell_name,SHELL_TYPE=self.shell_type,DB_NAME=self.db_name,ORA_SID=self.ora_sid)

        #20220915 sdhyun
        #pcordb24 self.shell_path 로 통일.
        #shell_name = os.path.join(self.get_shell_apth(),self.shell_name)
        shell_name = os.path.join(self.shell_path,self.shell_name)
    
        #2022.11.15 삼성카드 적용 ORACLE_SID 추가 @jhbae
        cmd = '/bin/su - {OWNER} -c "export ORACLE_SID={ORA_SID};sh {SHELL_NAME}"'.format(OWNER=self.owner, ORA_SID=self.ora_sid, SHELL_NAME=shell_name)

        if self.check_bit:
            # log.info(cmd)
            sys_date_str = self.rman_control.ora_datetime()
            if self.shell_type in ['ARCH', 'INCR_L0', 'INCR_L1', 'FULL_L0', 'MRG', 'INCR_MRG']:
                log.logdata('AGENT', 'INFO', '80000', 'shell_type: {}'.format(self.shell_type))
                bg_thread(cmd=cmd, interval=1)
                time.sleep(1)
               
                pid = self.get_pid_old(self.shell_name)
                log.logdata('AGENT', 'INFO', '80000', 'PID: {}'.format(pid))
                if pid is None:
                    print 'pid is None'
                    job_fail_info = {}
                    job_fail_info['job_id'] = self.job_id
                    job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
                    job_fail_info['memo'] = 'Backup Process check Fail - %s'%(os.path.join(self.shell_path, self.shell_name))
                    ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).submit_fail(job_fail_info)
                    sys.exit()
                else: #running 추가 해야할부분 @jhbae
                    monitor_info = {
                        'job_st' : 'Running'
                        ,'tg_job_dtl_id' : self.tg_job_dtl_id
                    }
                    self.send_job_monitor(monitor_info)

                session_id = self.rman_control.get_session_id(old_session_id)
                log.logdata('AGENT', 'INFO', '80000', 'SESSION_KEY: {}'.format(session_id))

                if 'no' in session_id:
                    job_fail_info = {}
                    job_fail_info['job_id'] = self.job_id
                    job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
                    job_fail_info['memo'] = "this job ({})  can't get a oracle session_key ".format(self.shell_name)
                    ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).job_already_exist_run(job_fail_info)
                else:
                    monitor_info = {}
                    monitor_info['ora_sid'] = self.ora_sid
                    monitor_info['pid'] = pid
                    monitor_info['session_id'] = session_id
                    monitor_info['job_id'] = self.job_id
                    monitor_info['shell_name'] = self.shell_name
                    monitor_info['shell_type'] = self.shell_type
                    monitor_info['db_name'] = self.db_name
                    monitor_info['sys_date_str'] = sys_date_str
                    monitor_info['tg_job_dtl_id'] = self.tg_job_dtl_id
                 
                    cmd = """python27/bin/python ./job_monitor.py {} {} {} {} {} {} {} "{}" {} """.format(
                        self.ora_sid, pid, session_id, self.job_id, self.shell_name, self.shell_type, self.db_name,
                        sys_date_str, self.tg_job_dtl_id)
                   
                    with open('job_submit.txt', 'a') as f:
                        f.write(cmd + '\n')
                    log.logdata('AGENT', 'INFO', '80000', '-' * 50)
                    log.logdata('AGENT', 'INFO', '80000', 'ora_sid: {}'.format(self.ora_sid))
                    log.logdata('AGENT', 'INFO', '80000', 'pid: {}'.format(pid))
                    log.logdata('AGENT', 'INFO', '80000', 'session_id: {}'.format(session_id))
                    log.logdata('AGENT', 'INFO', '80000', 'job_id: {}'.format(self.job_id))
                    log.logdata('AGENT', 'INFO', '80000', 'shell_name: {}'.format(self.shell_name))
                    log.logdata('AGENT', 'INFO', '80000', 'shell_type: {}'.format(self.shell_type))
                    log.logdata('AGENT', 'INFO', '80000', 'db_name: {}'.format(self.db_name))
                    log.logdata('AGENT', 'INFO', '80000', 'start_time : {}'.format(sys_date_str))
                    log.logdata('AGENT', 'INFO', '80000', 'tg_job_dtl_id : {}'.format(self.tg_job_dtl_id))
                    log.logdata('AGENT', 'INFO', '80000', '=' * 50)
                    log.logdata('AGENT', 'INFO', '80000', 'JOB MONITOR START')
                    job_monitor.job_mon(self.ora_sid, pid, session_id, self.job_id, self.shell_name, self.shell_type,
                                        self.db_name, sys_date_str, self.tg_job_dtl_id).main()
            else:
                """
                if job is not db shell file
                """
                mon_cmd = """{} {} {} {} {} {} """.format(cmd, self.job_id, self.shell_name, self.shell_type,
                                                          self.db_name, self.tg_job_dtl_id)
               
                with open('job_submit.txt', 'a') as f:
                    f.write(mon_cmd + '\n')

                job_monitor_ndb.job_mon(cmd=cmd, job_id=self.job_id, shell_name=self.shell_name,
                                        shell_type=self.shell_type, db_name=self.db_name,
                                        tg_job_dtl_id=self.tg_job_dtl_id).main()
                # os.wait()[1]


if __name__ == '__main__':
    arg = sys.argv
    if len(arg) == 8:
        '''
        @2022.09.28 jhbae check
        ./job_submit.py 2 RMAN_Archivelog.sh ARCH BAE19C bae19c 49

        '''
        job_id = arg[1]
        shell_name = arg[2]
        shell_type = arg[3]
        db_name = arg[4]
        ora_sid = arg[5]
        tg_job_dtl_id = arg[6]
        shell_path = arg[7]
        job_submit(job_id=job_id, shell_name=shell_name, shell_type=shell_type, db_name=db_name, ora_sid=ora_sid,
                   tg_job_dtl_id=tg_job_dtl_id,shell_path=shell_path).main()
