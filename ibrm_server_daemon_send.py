'''
Created on 2014. 4. 16.

@author: Administrator
'''
import threading, time
import socket
import os
import ast
import glob
import ConfigParser
import random
import json
#import ibrm_dbms

class SocketSender():
    def __init__(self,HOST,PORT):
        #self.dbms = ibrm_dbms.fbrm_db()
        self.HOST = HOST
        self.PORT = int(PORT)

    def shell_list(self):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_SHELL_LIST'
        data['ARG'] = {}
        data['ARG']['db_name'] = 'UPGR'
        self.socket_send(data)

    def shell_detail(self):

        HOST, PORT = "121.170.193.196", 53001

        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_STATUS'
        data['ARG'] = {}
        data['ARG']['db_nme'] = 'UPGR'
        data['ARG']['job_id'] = '13'

        print data
        # Create a socket (SOCK_STREAM means a TCP socket)
        self.socket_send(data)


    def socket_send(self,data):
        sBit = False
        received = ''
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Connect to server and send data
            sock.connect((self.HOST, self.PORT))
            sock.sendall(str(data) + "\n")

            # Receive data from the server and shut down
            received = sock.recv(1024)
            print 'recv :', received
            # if received == 'READY':
            #     cmd = 'agent_shell_list'
            #     sock.sendall(cmd)
            print 'ok'

            # recv_data=sock.recv(1024)
            # print 'recv_data :',recv_data

            sBit = True
        except socket.error as e:
            sBit = False
            print e
        finally:
            sock.close()

        return received



    def job_already_exist_run(self,job_data):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_ALREADY_RUN'
        data['ARG'] = {}
        data['ARG'] = job_data
        print data
        self.socket_send(data)

    def agent_health_check(self,job_data):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_HEALTH_CHECK'
        data['ARG'] = {}
        data['ARG'] = job_data
        print data


        self.socket_send(data)


    # def job_monitor (self,job_data):
    #     data = {}
    #     data['FLETA_PASS'] = 'kes2719!'
    #     data['CMD'] = 'JOB_MONITOR'
    #     data['ARG'] = {}
    #     data['ARG'] = job_data
    #     print data
    #     self.socket_send(data)



    def submit_fial(self,job_data):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_SUBMIT_FAIL'
        data['ARG'] = {}
        data['ARG'] = job_data
        print data
        self.socket_send(data)


    def log_update(self,log_data):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'LOG_UPDATE'
        data['ARG'] = {}
        data['ARG'] = log_data
        self.socket_send(data)



    def job_error(self,job_status):
        pass

    def job_status(self,job_status):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_STATUS'
        data['ARG'] = {}
        data['ARG'] = job_status
        print job_status
        self.socket_send(data)

    def job_submit_fial(self,job_status):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_SUBMIT_FAIL'
        data['ARG'] = {}
        data['ARG'] = job_status
        print job_status
        self.socket_send(data)



    def job_status_shell_only(self,job_status):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_STATUS_SHELL_ONLY'
        data['ARG'] = {}
        data['ARG'] = job_status
        print job_status
        self.socket_send(data)

    def job_complete(self,job_status):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_COMPLETE'
        data['ARG'] = {}
        data['ARG'] = job_status

        self.socket_send(data)

    def get_rman_tag(self,job_id):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'GET_RMAN_TAG'
        data['ARG'] = {}
        data['ARG']['job_id'] = job_id
        job_status = self.socket_send(data)
        job_status = job_status.replace("'",'"')
        print type(job_status)
        json_data = json.loads(job_status)
        # json_data = literal_eval(job_status)
        rman_tag = json_data['ARG']['rman_tag']
        print rman_tag
        return rman_tag

    def rman_progress(self,job_status):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'RMAN_PROGRESS'
        data['ARG'] = {}
        data['ARG'] = job_status
        self.socket_send(data)

    def send(self):

        HOST, PORT = "121.170.193.196", 53001


        data={}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'AGENT_JOB_EXCUTE'
        data['ARG'] = {}
        data['ARG']['job_id'] = '00001'
        data['ARG']['db_nme'] = 'UPGR'
        data['ARG']['shell_name'] = 'UPGR_Incr_Level0.sh'
        data['ARG']['shell_type'] = 'level0'


        # Create a socket (SOCK_STREAM means a TCP socket)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sBit = False

        try:
            # Connect to server and send data
            sock.connect((HOST, PORT))
            sock.sendall(str(data) + "\n")

            # Receive data from the server and shut down
            received = sock.recv(1024)
            print 'recv :',received
            # if received == 'READY':
            #     cmd = 'agent_shell_list'
            #     sock.sendall(cmd)
            print 'ok'

            # recv_data=sock.recv(1024)
            # print 'recv_data :',recv_data



            sBit = True
        except socket.error as e:
            sBit = False
            print e
        finally:
            sock.close()

        return sBit

    def main(self):

        sBit = self.send()
        print 'sBit' , sBit

if __name__ == '__main__':

    HOST='121.170.193.207'
    PORT=53002

#    job_status={'status': 'RUNNING', 'input_type': 'DB INCR', 'start_time': '2020-11-02 16:44:29', 'session_id': '4384', 'elapsed_seconds': '42', 'end_time': '2020-11-02 16:45:11', 'write_bps': '23.62M'}
#    shell_name = 'IBRM_Incr_Level0.sh'
#    job_fail_info = {}
#    job_fail_info['job_id'] = '13'
#    job_fail_info['tg_job_dtl_id'] = '177'
#    job_fail_info['memo'] = '{} job is allready running'.format(shell_name)

    #job_status =  {'status': 'RUNNING', 'job_id': 52, 'input_type': 'DB INCR', 'start_time': '2020-11-09 19:50:30', 'tg_job_dtl_id': 195, 'pid': '27895', 'session_id': '2272', 'elapsed_seconds': '5', 'end_time': '2020-11-09 19:50:35', 'job_st': 'Running', 'write_bps': '88.80M', 'session_stamp': '1056052228', 'ora_sid': 'ibrm', 'session_recid': '2272'}
    #SocketSender(HOST,PORT).job_status(job_status)


    """
    ibrm 8517 3743 37 IBRM_Incr_Level1_Weekly.sh INCR IBRM "2020-11-19 09:15:22" 554
    """

    monitor_info= {}

    monitor_info['ora_sid'] = 'ibrm'
    monitor_info['pid'] = '8517'
    monitor_info['session_id'] = '3743'
    monitor_info['job_id'] = '37'
    monitor_info['shell_name'] = 'IBRM_Incr_Level1_Weekly'
    monitor_info['shell_type'] = 'INCR'
    monitor_info['db_name'] = 'IBRM'
    monitor_info['sys_date_str'] = "2020-11-19 09:15:22"
    monitor_info['tg_job_dtl_id'] = '554'

    job_check={'memo': 'this job (IBRM_Archive.sh)  is already running', 'job_id': 71, 'tg_job_dtl_id': 577}
    job_check={'end_time': '', 'start_time': '20201211200223', 'tg_job_dtl_id': '201836', 'job_id': '93', 'job_st': 'Running'}
    job_check={'job_id': 119, 'start_time': '20201213171022', 'job_st': 'End-OK', 'pid': 3954, 'elapsed_seconds': 60, 'end_time': '20201213171122', 'tg_job_dtl_id': 201988}
    #SocketSender(HOST,PORT).job_status_shell_only(job_check)
    job_id=monitor_info['job_id']
    SocketSender(HOST, PORT).get_rman_tag(job_id)
#    SocketSender(HOST,PORT).job_status(job_check)



