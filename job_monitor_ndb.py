"""
self.ora_sid, pid, session_id, self.job_id, self.shell_name, self.shell_type, self.db_name, sys_date_str,self.tg_job_dtl_id

"""
import os
import sys
import ibrm_server_daemon_send
import time
import datetime
import ConfigParser
import subprocess

class job_mon():
    def __init__(self,**kwargs):
        """
        cmd=cmd,job_id= self.job_id,shell_name= self.shell_name, shell_type= self.shell_type, db_name=self.db_name, tg_job_dtl_id=self.tg_job_dtl_id
        :param kwargs:
        """
        self.cfg = self.get_cfg()
        self.cmd = kwargs['cmd']
        self.job_id = kwargs['job_id']
        #self.pid = kwargs['pid']
        self.shell_name = kwargs['shell_name']
        self.shell_type = kwargs['shell_type']
        self.db_name = kwargs['db_name']
        self.tg_job_dtl_id = kwargs['tg_job_dtl_id']
        self.socket_server = self.get_server()


    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config','config.cfg')
        cfg.read(cfg_file)
        return cfg

    def server_info(self):
        HOST=self.cfg.get('ibrm_server','ip')
        PORT=self.cfg.get('ibrm_server','socket_port')
        return HOST,PORT

    def get_server(self):
        HOST,PORT = self.server_info()
        socket_server=ibrm_server_daemon_send.SocketSender(HOST, int(PORT))
        return socket_server


    def runcommand(self):
        proc = subprocess.Popen(self.cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True,
                                universal_newlines=True)
        std_out, std_err = proc.communicate()

        return proc.returncode, std_out, std_err,proc.pid





    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config','config.cfg')
        cfg.read(cfg_file)
        return cfg



    def server_info(self):
        HOST=self.cfg.get('ibrm_server','ip')
        PORT=self.cfg.get('ibrm_server','socket_port')
        return HOST,PORT

    def get_server(self):
        HOST,PORT = self.server_info()
        socket_server=ibrm_server_daemon_send.SocketSender(HOST, int(PORT))
        return socket_server




    def main(self):

        self.job_st = 'Running'
        start_time = time.time()
        start_time_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        pid = os.getpid()
        data = {}

        data['job_id'] = self.job_id
        data['job_st'] = self.job_st
        data['tg_job_dtl_id'] = self.tg_job_dtl_id
        data['start_time'] = start_time_str
        data['end_time'] = ''

        self.socket_server.job_status_shell_only(data)

        returncode, std_out, std_err,pid =self.runcommand()
        print '-'*50
        print self.cmd
        print 'return code : ',returncode,returncode ==0
        print 'pid : ',pid
        print os.popen('ps -ef | grep {}'.format(pid)).read()


        if returncode ==0:
            self.job_st = 'End-OK'
        else:
            self.job_st = 'Fail'
        print 'job_st :',self.job_st

        elapsed_seconds = time.time() - start_time
        end_time_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')


        data['pid'] = pid
        data['job_id'] = self.job_id

        data['job_st'] = self.job_st
        data['tg_job_dtl_id'] = self.tg_job_dtl_id
        data['elapsed_seconds'] = int(elapsed_seconds)
        data['start_time'] = start_time_str
        data['end_time'] = end_time_str
        print data
        self.socket_server.job_status_shell_only(data)
        print std_out

if __name__=='__main__':
    arg = sys.argv
    cmd = arg[1]
    job_id = arg[2]
    shell_name = arg[3]
    shell_type = arg[4]
    db_name = arg[5]
    tg_job_dtl_id = arg[6]
    cmd = """su - oracle -c "sh /u01/SCRIPTS/Database/ORCL/RMAN/SCHEDULE/ORCL_Arch_snapshot.sh"
    """
    job_id = "219"
    shell_name = "ORCL_Arch_snapshot.sh"
    tg_job_dtl_id = '11170780'
    db_name = 'ORCL'
    shell_type = 'DSC'

    job_mon(cmd=cmd, job_id=job_id, shell_name=shell_name, shell_type=shell_type, db_name=db_name,tg_job_dtl_id=tg_job_dtl_id).main()
