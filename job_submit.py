# -*- encoding:utf-8*-
"""
* *******************************************************
* Copyright (c) Fleta Communications, Inc. 2020. All Rights Reserved.
* *******************************************************
* create date :
* modipy date : 2021-12-01
"""
__author__ = 'TODO: muse@fletacom.com'
__ibrm_agent_version__ = 'TODO: v1.2'
import threading
import subprocess
import shlex
import os
import time
import threading
import v_rman_control
import sys
import ConfigParser
import ibrm_server_daemon_send
import job_monitor
import datetime
import job_monitor_ndb
from subprocess import check_output

print("Main Thread JOB submit START")



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


    def run(self):
        """ Method that runs forever """
        # Do something
        print('JOB START')
        print 'START TIME :',datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # subprocess.call(shlex.split(self.cmd), stdout=subprocess.PIPE, shell=True)

        #ret = os.popen(self.cmd)
        #output = ret.read()
        print 'cmd :', self.cmd
        time.sleep(self.interval)
        start_date=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        os.popen('echo "#[{}]" >> ./logs/daemon_session.log'.format(start_date))
        os.popen('echo "#[{}]" >> ./logs/daemon_error.log'.format(start_date))
        os.popen('echo "#[{}]" >> ./logs/stdout.log'.format(start_date))
        os.popen('echo "#[{}]" >> ./logs/stderr.log'.format(start_date))
        back_cmd = 'nohup {} &'.format(self.cmd)
        p = subprocess.Popen(shlex.split(back_cmd)
                             , stdout=open('./logs/stdout.log', 'a')
                             , stderr=open('./logs/stderr.log', 'a')
                             , preexec_fn=os.setpgrp)

        print back_cmd
        print 'PID :', p.pid
        with open('now_pid.txt','w') as fw:
            fw.write(str(p.pid))
        p.communicate()
        print datetime.datetime.now().strftime('%H-%m-%d %H:%M:%S'),' #SCRIPT END'
        print 'STDOUT :'
        print '-'*50
        print p.stdout
        print 'STDERR :'
        print p.stderr
        print 'subprocess job-end'


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
        # (shell_type=shell_type,db_name =db_name,ora_sid=ora_sid)
        self.rman_control = v_rman_control.rman_control(shell_type=self.shell_type, db_name=self.db_name,
                                                        ora_sid=self.ora_sid)
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

        return owner

    def check_shell(self):
        cmd = 'ps -ef | grep {SHELL_NAME} | grep {DB_NAME} | grep -v grep |grep -v su | grep -v python | wc -l'.format(
            SHELL_NAME=self.shell_name,DB_NAME=self.db_name)

        ret = os.popen(cmd).read()
        check_bit = False
        # print 'check_shell cmd:', cmd
        if int(ret) > 0:
            job_fail_info = {}
            job_fail_info['job_id'] = self.job_id
            job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
            job_fail_info['memo'] = 'this job ({})  is already running'.format(self.shell_name)

            print job_fail_info
            ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).job_already_exist_run(job_fail_info)
            check_bit = False
        else:
            check_bit = True
        shell_path = self.get_shell_apth()
        shell_path = self.shell_path
        if not os.path.isfile(os.path.join(shell_path, self.shell_name)):
            job_fail_info = {}
            job_fail_info['job_id'] = self.job_id
            job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
            job_fail_info['memo'] = 'shell_file {} is not exist as path({})'.format(self.shell_name, shell_path)
            print job_fail_info
            ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).job_submit_fial(job_fail_info)
            check_bit = False
        return check_bit

    def get_shell_apth(self):
        try:
            shell_path = self.cfg.get('common', 'shell_path')
            if '{DB_NAME}' in shell_path:
                shell_path = shell_path.replace('{DB_NAME}', self.db_name)
        except:
            shell_path = '/ZFS/SCRIPTS/Database/{DB_NAME}/RMAN/SCHEDULE'
            shell_path = shell_path.replace('{DB_NAME}', self.db_name)
        return shell_path

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config', 'config.cfg')
        cfg.read(cfg_file)
        return cfg

    def process_status(self, pid):
        ps_list = []
        cmd = "ps -ef | grep pid | grep -v grep"
        ret = os.popen(cmd).read()
        for line in ret.spltilines():
            lineset = line.split()
            if len(lineset) > 1:
                ps = lineset[1]
                if ps not in ps_list:
                    ps_list.append(ps)

    def get_pid_gid(self, shell_name):
        try:
            return check_output(["pidof", "-s", shell_name])
        except:
            return None

    def get_pid_old(self, shell_name):

        cnt = 0
        pid = None
        while True:
            cmd = "ps -ef |  grep %s | grep -v grep | grep root | wc -l" % (shell_name)
            print cmd
            ret = os.popen(cmd).read().strip()
            print ret, ret == '1'

            cmd1 = "ps -ef | grep %s | grep -v grep " % (shell_name)
            print 'shell name :', shell_name
            print '-' * 40
            ps_msg = os.popen(cmd1).read()
            print ps_msg
            if ret == '0':
                time.sleep(5)
            elif ret == '1':
                print '-' * 40
                cmd1 = "ps -ef |  grep %s  | grep -v grep | grep root | awk '{print $2}' " % (shell_name)
                print cmd1
                pid = os.popen(cmd1).read().strip()
                if not pid is None:
                    break
                if pid == '':
                    print 'pid get fail'

            else:
                print 'pid :',pid
                time.sleep(2)
                job_fail_info = {}
                job_fail_info['job_id'] = self.job_id
                job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
                job_fail_info['memo'] = '1 more process({})'.format(ps_msg)
                ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).submit_fial(job_fail_info)

            print 'cnt :', cnt
            if cnt > 10:
                """
                job_fail_info = {}
                job_fail_info['job_id'] = self.job_id
                job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
                job_fail_info['memo'] = 'this job ({})  submit fail'.format(self.shell_name)
                            if max_ps_cnt == 0 and cnt == 10:
                print 'max_ps_count : ',max_ps_cnt
                print 'count        : ',cnt
                self.job_st = 'Fail'
                return_data = self.job_monitor(session_id=self.session_key)
                return_data['job_st'] = self.job_st
                return_data['prg_cnt'] = prg_cnt
                return_data['tot_cnt'] = tot_cnt
                return_data['progress'] = progress
                
                self.socket_server.job_status(return_data)
                break
                """
                # log.logdata('AGENT', 'ERROR', '50101', str(shell_name))
                job_fail_info = dict()
                job_fail_info['job_id'] = self.job_id
                job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
                job_fail_info['memo'] = 'this job ({})  submit fail'.format(self.shell_name)
                ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).submit_fial(job_fail_info)
                # log.logdata('AGENT', 'ERROR', '60007', str(job_fail_info['memo']))
                check_bit = False
                break
            cnt = cnt + 1
            print 'cnt :', cnt
        if pid is None:
            if os.path.isfile('pid.txt'):
                with open('pid.txt') as f:
                    p_pid = f.read()
                print p_pid
                cmd = 'ps -ef | grep {} | grep -v grep | grep -v child '.format(p_pid)
                print cmd

                ret = os.popen(cmd).read()
                print ret
                line_set = ret.splitlines()
                if len(line_set) == 1:
                    pid = line_set[0].split()[1]
                else:
                    for line in line_set:
                        if shell_name in line:
                            pid = line.split()[1]

        if pid is None:
            with open('now_pid.txt','r') as f:
                tmp_pid = f.read().strip()
            cmd = 'ps -ef | grep {} | grep -v grep'.format(self.shell_name)
            print 'shell process :',cmd
            ret = os.popen(cmd).read()
            for line in ret.splitlines():
                if tmp_pid in line :
                    pid = tmp_pid
        if pid is None:
            job_fail_info = dict()
            job_fail_info['job_id'] = self.job_id
            job_fail_info['tg_job_dtl_id'] = self.tg_job_dtl_id
            job_fail_info['memo'] = '[PID GET ERROR] this job ({})  submit fail - '.format(self.shell_name)
            # log.logdata('AGENT', 'ERROR', '50101', str(job_fail_info))
            # log.logdata('AGENT', 'ERROR', '50101', str(job_fail_info))
            ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).submit_fial(job_fail_info)
        return pid

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
            ibrm_server_daemon_send.SocketSender(self.HOST, int(self.PORT)).submit_fial(job_fail_info)
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
        print '-' * 50
        print 'OLD SESSION_KEY :', old_session_id
        # cmd = "python ./ibrm_excute.py {JOB_ID} {SHELL_NAME} {SHELL_TYPE} {DB_NAME} {ORA_SID}".format(JOB_ID=self.job_id,SHELL_NAME=self.shell_name,SHELL_TYPE=self.shell_type,DB_NAME=self.db_name,ORA_SID=self.ora_sid)
        # shell_name = os.path.join(self.get_shell_apth(),self.shell_name)
        shell_name = os.path.join(self.shell_path, self.shell_name)
        print 'shell_name :', shell_name

        cmd = 'su - {} -c "sh {}"'.format(self.owner, shell_name)
        print 'cmd :', cmd
        print 'check_bit :', self.check_bit
        """
        "ARCH" "Archive" 1 "?꾩뭅?대툕諛깆뾽"
        "INCR_L0" "INCR_LEV0" 2 "Incremental Level 0 諛깆뾽"
        "INCR_L1" "INCR_LEV1" 3 "Incremental Level 1 諛깆뾽"
        "FULL_L0" "FULL_LEV0" 4 "Full Level 0 諛깆뾽"
        "MRG" "MERGE" 5 "MERGE"
        "INCR_MRG" "INCR+MERGE" 6 "Incremental + Merge"
        "DSC" "DATA SNAP Create" 7 "Data Snapshot Create"
        "DSD" "DATA SNAP DEL" 8 "Data Snapshot Delete"
        "ASC" "ARCH SNAP CREATE" 9 "Archieve Snapshot Create"
        "ASD" "ARCH SNAP DEL" 10 "Archieve Snapshot Delete"
                        """
        if self.check_bit:
            # log.info(cmd)
            print ['ARCH', 'INCR_L0', 'INCR_L1', 'FULL_L0', 'MRG', 'INCR_MRG']
            print 'SHELL_TYPE :', self.shell_type, self.shell_type in ['ARCH', 'INCR_L0', 'INCR_L1', 'FULL_L0', 'MRG',
                                                                       'INCR_MRG']
            if self.shell_type in ['ARCH', 'INCR_L0', 'INCR_L1', 'FULL_L0', 'MRG', 'INCR_MRG']:
                # t = threading.Thread(target=job_submit, args=(cmd,))
                # t.start()
                bg_thread(cmd=cmd, interval=1)
                time.sleep(0.5)
                # pid =self.get_pid(self.shell_name)
                pid = self.get_pid_old(self.shell_name)
                print 'shell pid :', pid
                if pid is None:
                    print 'pid is None'
                    sys.exit()
                sys_date_str = self.rman_control.ora_datetime()
                session_id = self.rman_control.get_session_id(old_session_id)
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
                    self.send_job_monitor(monitor_info)
                    cmd = """python ./job_monitor.py {} {} {} {} {} {} {} "{}" {} """.format(
                        self.ora_sid, pid, session_id, self.job_id, self.shell_name, self.shell_type, self.db_name,
                        sys_date_str, self.tg_job_dtl_id)
                    # print 'mon cmd :',cmd
                    # log.info(cmd)
                    # log.logdata('AGENT', 'INFO', '60001', str(cmd))
                    with open('job_submit.txt', 'a') as f:
                        f.write(cmd + '\n')
                    job_monitor.job_mon(self.ora_sid, pid, session_id, self.job_id, self.shell_name, self.shell_type,
                                        self.db_name, sys_date_str, self.tg_job_dtl_id).main()
            else:
                """
                if job is not db shell file  
                """
                mon_cmd = """{} {} {} {} {} {} """.format(cmd, self.job_id, self.shell_name, self.shell_type,
                                                          self.db_name, self.tg_job_dtl_id)
                # print 'mon cmd :',mon_cmd
                # log.info(cmd)
                # log.logdata('AGENT', 'INFO', '60001', str(mon_cmd))
                with open('job_submit.txt', 'a') as f:
                    f.write(cmd + '\n')
                ret_data = job_monitor_ndb.job_mon(cmd=cmd, job_id=self.job_id, shell_name=self.shell_name,
                                                   shell_type=self.shell_type, db_name=self.db_name,
                                                   tg_job_dtl_id=self.tg_job_dtl_id).main()
                # os.wait()[1]


if __name__ == '__main__':
    arg = sys.argv
    if len(arg) == 7:
        job_id = arg[1]
        shell_name = arg[2]
        shell_type = arg[3]
        db_name = arg[4]
        ora_sid = arg[5]
        tg_job_dtl_id = arg[6]
        job_submit(job_id=job_id, shell_name=shell_name, shell_type=shell_type, db_name=db_name, ora_sid=ora_sid,
                   tg_job_dtl_id=tg_job_dtl_id).main()
