import datetime
import os
import sys
import time
import log_clear
import subprocess
import ConfigParser
import shutil

log_clear
ibrm_path= os.path.dirname(os.path.abspath(__file__))
platform = sys.platform
class menu():
    def __init__(self):
        self.log_clear()
        self.cfg = self.get_cfg()
        self.HOST = self.cfg.get('socket','HOST')
        self.PORT = self.cfg.get('socket', 'PORT')

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config','config.cfg')
        cfg.read(cfg_file)
        return cfg

    def debug_mode(self):
        debug_bit = False
        try:
            mode=self.cfg.get('common','debug_mode')
            if mode == 'True':
                debug_bit = True
        except:
            debug_bit = False
        return debug_bit

    def get_pid(self):
        pid = None
        txt_pid = os.popen('cat pid.txt').read()
        cmd='ps -ef | grep {} | grep -v grep '.format(txt_pid)
        lines = os.popen(cmd).read().splitlines()
        for line in lines:
            if 'ibrm_daemon_socket' in line:
                pid=line.split()[1]
                break
        return pid


    def log_clear(self):
        print("\033c")
        print 'old log clearing.....'
        remove_cnt=log_clear.clear().main()
        if remove_cnt > 0:
            print 'REMOVE FILE :',remove_cnt


    def get_version(self):
        pass

    def getHeadMsg(self,title='IBRM MENU'):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = '\n'
        msg += '#'*79+'\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#### '+('TITLE     : %s'%title).ljust(71)+'###\n'
        msg += '#### '+('DATA TIME : %s'%now).ljust(71)+'###\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#'*79+'\n'
        return msg

    def process_status(self):

        cmd='ps -ef | grep ibrm_daemon_socket.py |grep -v grep | wc -l'
        ret=os.popen(cmd).read().strip()
        # print cmd
        # print ret,ret=='1'
        bit=False
        if ret=='1' :
            bit=True
        elif ret > '1':
            bit = False
            print '-'*20
            print 'TOO MANU PROCESS'
            print os.popen('ps -ef | grep ibrm_daemon_socket | grep -v grep').read()
            print '-'*20
        else :
            bit = False
        return bit

    def ps_msg(self):
        ps=self.process_status()
        if ps:
            ps_msg='Alive'
        else:
            ps_msg ='Dead'
        port_statu=self.port_status()
        msg=    'PROCESS STATUS        : %s\n'%ps_msg
        msg=msg+"PORT({}) STATUS    : {}".format(self.PORT,str(port_statu))
        return msg



    def process_stop(self):
        if not self.process_status():
            print self.ps_msg()
            return None
        else:
            pid = self.get_pid()
            if pid == None:
                print 'pid not found '
                print 'check process or port'
                raw_input('>ENTER')
            else:
                if 'aix' in platform:
                    f_str = '{}.{}'.format(self.HOST, self.PORT)
                    cmd = 'ps -ef | grep {} | grep -v grep'.format(pid)
                    ret= os.popen(cmd).read()
                    lines = ret.splitlines()
                    if len(lines) >0:
                        cmd='kill -9 %s'%pid
                        print cmd
                        ret=raw_input('DO YOU WANT TO KILL THIS PROCESS (Y/N) ?')
                        if ret in ['Y','y']:
                            print os.popen(cmd).read()
                            cnt=1
                    else:
                        print 'cant stop process !!'
                        print 'check process or port'
                        raw_input('>ENTER')


                else:
                    cmd='netstat -alp | grep {}'.format(self.PORT)
                    print cmd
                    ret= os.popen(cmd).read().split()[-1]
                    print ret
                    if '/' in ret:
                        port_pid = ret.split('/')[0]
                        ps= ret.split('/')[-1]
                        print '{} port pid  : {}'.format(self.PORT,port_pid)
                        cmd='kill -9 %s'%pid
                        print cmd
                        ret=raw_input('DO YOU WANT TO KILL THIS PROCESS (Y/N) ?')
                        if ret in ['Y','y']:
                            print os.popen(cmd).read()
                            cnt=1
                            while True:
                                print self.process_status()
                                if not self.process_status():
                                    break
                                else:
                                    print 'process killing ... %s'%cnt
                                    time.sleep(1)
                                cnt = cnt+1

                        else:
                            print 'cant stop process !!'
                            print 'check process or port'
                            raw_input('>ENTER')


    def port_status(self):
        port_status = None
        if 'aix' in platform:
            cmd='netstat -an | grep 53001'
            lines = os.popen(cmd).read().splitlines()
            for line in lines:
                f_str = '{}.{}'.format(self.HOST, self.PORT)
                if f_str in line:
                    port_status= line.split()[-1]
        else:
            cmd='netstat -alp | grep 53001'
            ret= os.popen(cmd).read().splitlines()
            for line in ret:
                ret_set= line.split()
                if ':53001' in ret_set[3]:
                    if len(ret_set) > 2:
                        port_status = ret_set[-2].strip()
        return port_status



    def log_backup(self):
        d_log = os.path.join('./logs','daemon_session.log')
        d_err = os.path.join('./logs', 'daemon_error.log')
        std_log = os.path.join('./logs','stdout.log')
        std_err = os.path.join('./logs', 'stderr.log')
        now_date = datetime.datetime.now().strftime('%Y%m%d%H%M')
        if os.path.isfile(d_log):
            d_backup_log = os.path.join('./logs','daemon_session_{}.log'.format(now_date))
            os.rename(d_log,d_backup_log)
        if os.path.isfile(d_err):
            d_backup_err = os.path.join('./logs','daemon_error_{}.log'.format(now_date))
            os.rename(d_err,d_backup_err)
        if os.path.isfile(std_log):
            std_backup_log = os.path.join('./logs','stdout_{}.log'.format(now_date))
            os.rename(std_log,std_backup_log)
        if os.path.isfile(std_err):
            std_backup_err = os.path.join('./logs','stderr_{}.log'.format(now_date))
            os.rename(std_err,d_backup_log)




    def process_start(self,check='NO'):
        self.log_backup()
        if self.process_status():
            msg='IBRM AGENT PROCESS ALREADY STARTED !'
            print msg
            if not check == 'YES':
                raw_input('Enter>')
        else:
            print sys.executable
            os.chdir(ibrm_path)

            trace_file = os.path.join('logs','daemon_session.log')
            error_log_file = os.path.join('logs','daemon_error.log')
            print 'DEBUG MODE :',self.debug_mode()
            if self.debug_mode():
                write_bit = 'a'
                d = datetime.datetime.now().strftime('%Y%m%d%H%m')
                if os.path.isfile(trace_file):
                    backup_file = os.path.join('logs', 'daemon_session_{}.log'.format(d))
                    os.rename(trace_file, backup_file)
                if os.path.isfile(error_log_file):
                    backup_file = os.path.join('logs', 'daemon_error_{}.log'.format(d))
                    os.rename(error_log_file, backup_file)
            else:
                write_bit = 'w'
            self.log_clear()
            p = subprocess.Popen(['nohup', sys.executable, os.path.join(ibrm_path,'ibrm_daemon_socket.py') , "child"]
                                 , stdout=open(trace_file, write_bit)
                                 , stderr=open(error_log_file, write_bit)
                                 , close_fds=True)

            print 'ibrm process start!'
            # print 'PID :',p.pid
            # print p.stdout

            cmd = 'ps -ef | grep {} | grep -v grep '.format(str(p.pid))
            print os.popen(cmd).read()
            #print os.popen('ps -ef | grep {} | grep -v grep'.format(str(proc.pid)))

            with open('pid.txt','w') as fw:
                fw.write(str(p.pid))
            # print p.stdout
            #print check, check == 'YES'
            if not check == 'YES':
                raw_input('Enter>')



    def main(self):


        while True:
            print("\033c")
            print self.getHeadMsg()
            print self.ps_msg()
            print ''
            print 'MENU >>'
            print '1) PROCESS START'
            print '2) PROCESS STOP'
            print '3) EXIT'
            ret=raw_input('>>')
            if ret=='1':
                self.process_start()
            elif ret=='2':
                self.process_stop()
            if ret=='3':
                print 'GOOD BYE!'
                sys.exit(0)
            os.popen('clear').read()





if __name__=='__main__':
    arg = sys.argv
    if len(arg) == 1:
        menu().main()
    else:
        if arg[1].upper() == 'START':
            menu().process_start('YES')
        elif arg[1].upper() == 'STOP':
            menu().process_stop()
        else:
            print 'GOOT BYE'


