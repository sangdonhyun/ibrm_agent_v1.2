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
            sock.sendall(str(data))
            # Receive data from the server and shut down
            received = sock.recv(1024)
          
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


    def job_monitor (self,job_data):
         data = {}
         data['FLETA_PASS'] = 'kes2719!'
         data['CMD'] = 'JOB_MONITOR'
         data['ARG'] = {}
         data['ARG'] = job_data
         self.socket_send(data)

    def submit_fail(self,job_data):
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

    def job_submit_fail(self,job_status):
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
        rman_tag = ''
        if job_status != '':
            job_status = job_status.replace("'",'"')
            json_data = json.loads(job_status)
        # json_data = literal_eval(job_status)
            rman_tag = json_data['ARG']['rman_tag']
        return rman_tag

    def rman_progress(self,job_status):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'RMAN_PROGRESS'
        data['ARG'] = {}
        data['ARG'] = job_status
        self.socket_send(data)
        
    def job_set_monitor(self, job_status):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_SET_MONITOR'
        data['ARG'] = {}
        data['ARG'] = job_status
        
        self.socket_send(data)
        
    def job_get_monitor(self, a_arg):
        data = {}
        data['FLETA_PASS'] = 'kes2719!'
        data['CMD'] = 'JOB_GET_MONITOR'
        data['ARG'] = {}
        data['ARG'] = a_arg
        s_monitor_status = self.socket_send('', data)
        s_monitor_status = s_monitor_status.replace("'",'"')
        a_monitor_status = json.loads(s_monitor_status)
        return a_monitor_status
    
    def agent_info(self, a_agent_info):
        self.socket_send(a_agent_info)

    def send(self):
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



