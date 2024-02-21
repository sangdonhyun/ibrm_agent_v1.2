import datetime
import os
#pcordb24
import pstats
import sys
import time
import log_control
import log_clear
import subprocess
import common
import ConfigParser

log=log_control.LogControl()
ibrm_path= os.path.dirname(os.path.abspath(__file__))

class menu():
    def __init__(self):
        self.o_common = common.Common()
        self.log_clear()
        cfg = ConfigParser.RawConfigParser()
        cfg.read(os.path.join('config','config.cfg'))
        try:
            self.port = cfg.get('socket','PORT')
        except:
            self.port = '53001'

    def get_pid(self):
        if os.path.isfile(os.path.join(ibrm_path,'pid.txt')):
            with open('pid.txt') as f:
                pid=f.read()
            cmd='ps -ef | grep {} | grep -v grep | wc -l'.format(pid)
            cnt=int(self.o_common.exec_command(cmd))
            if cnt == 1:
                return pid
            else:
                return None
        else:
            return None

    def log_clear(self):
        print("\033c")
        print 'old log clearing.....'
        remove_cnt=log_clear.clear().main()
        if remove_cnt > 0:
            log.logdata('AGENT', 'INFO', '50011', str('remove_log : {}'.format(remove_cnt)))

    def get_version(self):
        pass

    def getHeadMsg(self,title='IBRM MENU'):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = '\n'
        msg += '#'*79+'\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#### '+('TITLE     : %s'%title).ljust(71)+'###\n'
        msg += '#### '+('DATA TIME : %s'%now).ljust(71)+'###\n'
        msg += '#### '+('PORT : %s'%self.port).ljust(71)+'###\n'
        msg += '#### '+' '*71+'###\n'
        msg += '#'*79+'\n'
        return msg

    def process_status(self):
        cmd='ps -ef | grep ibrm_daemon_socket.py |grep -v grep | wc -l'
        ret=self.o_common.exec_command(cmd)

        bit=False
        if ret=='1' :
            bit=True
        elif ret > '1':
            bit = False
            print '-'*20
            print 'TOO MANU PROCESS'
            print self.o_common.exec_command('ps -ef | grep ibrm_daemon_socket | grep -v grep')
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
        msg=msg+"PORT(%s) STATUS    : %s"%(self.port,port_statu)
        return msg

    def process_stop(self):
        if not self.process_status():
            #pcrodb23
            print self.ps_msg()
            return None

        if os.path.isfile(os.path.join(ibrm_path,'pid.txt')):
            pid= self.get_pid()

            if pid == None:
                cmd='ps -ef | grep {} | grep -v grep | wc -l'.format(pid)

                ret=self.o_common.exec_command(cmd)
                if not int(ret) == 1:
                    cmd = """ps -ef | grep ibrm_daemon_socket.py | grep -v grep | awk '{print $2}'"""
                    ps_pid = self.o_common.exec_command(cmd)
                    print('ibrm_daemon_socket pid :', ps_pid)

            else:
                ps_pid=pid

            if int(ps_pid) > 1:
                cmd='kill -9 %s'%str(ps_pid)

                ret=raw_input('DO YOU WANT TO KILL THIS PROCESS (Y/N) ?')

                if ret in ['Y','y']:
                    cnt=1
                    os.popen(cmd).read()
                    while True:
                        print(self.process_status())
                        if not self.process_status():
                            break
                        else:
                            print('process killing ... %s'%cnt)
                            time.sleep(1)
                        cnt = cnt+1

            else:
                print('cant stop process !!')
                print('check process or port')
                raw_input('>ENTER')

    def port_status(self):
        port_status = None

        cmd='netstat -an | grep %s' %(self.port)
        ret = self.o_common.exec_command(cmd).splitlines()
        for line in ret:
            ret_set= line.split()
            if  self.port in ret_set[3]:
                if len(ret_set) > 2:
                    port_status = ret_set[-1].strip()
        return port_status

    def process_start(self,check='NO'):
        if self.process_status():
            msg='IBRM AGENT PROCESS ALREADY STARTED !'

            raw_input('Enter>')
        else:
            os.chdir(ibrm_path)
            p = subprocess.Popen(['nohup', sys.executable, os.path.join(ibrm_path,'ibrm_daemon_socket.py') , "child"]
                                 , stdout=open('/dev/null', 'w')
                                 , stderr=open('/dev/null', 'w')
                                 , preexec_fn=os.setsid
                                 , close_fds=True)

            print 'ibrm process start!'

            cmd = 'ps -ef | grep {} | grep -v grep '.format(str(p.pid))

            with open('pid.txt','w') as fw:
                fw.write(str(p.pid))

            with open('pid.txt','w') as fw:
                fw.write(str(p.pid))
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
            self.o_common.exec_command('clear')

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
            print 'GOOD BYE'