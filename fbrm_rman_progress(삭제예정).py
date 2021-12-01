'''
"""
* *******************************************************
* Copyright (c) Fleta Communications, Inc. 2020. All Rights Reserved.
* *******************************************************
* create date :
* modipy date : 2021-12-01
"""

__author__ = 'TODO: muse@fletacom.com'
__ibrm_agent_version__ = 'TODO: v1.2'
'''
import ConfigParser
import os
import sys
import shutil
import glob
import socket
import time
import datetime
import common
import socketClient
import glob
class fbrm_ora():

    def __init__(self):
        self.cfg=self.get_cfg()
        self.hostname = os.popen('hostname').read().strip()
        self.check_config()
        self.com=common.Common()
        self.now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.file_name=self.get_filename()

    def get_filename(self):

        fname='rman_%s_%s.tmp'%(self.hostname,self.now)
        return os.path.join('data',fname)
    def get_cfg(self):
        cfg= ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config','config.cfg')
        cfg.read(cfg_file)
        return cfg
    
    def check_config(self):
        uid=os.getuid()
        print uid,uid==0
        if not uid == 0:
            print 'root only'
            print 'program exit'
            sys.exit()
       	fbrm_dir='/tmp/fbrm'
        if not os.path.isdir(fbrm_dir):
            os.mkdir(fbrm_dir)
        data_path=os.path.join('/tmp','fbrm','data')
        if not os.path.isdir(data_path):
            os.mkdir(data_path)
        sql_path=os.path.join('/tmp','fbrm','sql')
        print sql_path,os.path.isdir(sql_path)
        if not os.path.isdir(sql_path):
            os.mkdir(sql_path)
        print os.path.isdir(sql_path)
        sql_path = os.path.join('/tmp', 'fbrm', 'rman_sql')
        print sql_path, os.path.isdir(sql_path)
        if not os.path.isdir(sql_path):
            os.mkdir(sql_path)

        sql_path = os.path.join('/tmp', 'fbrm', 'rman_data')
        print sql_path, os.path.isdir(sql_path)
        if not os.path.isdir(sql_path):
            os.mkdir(sql_path)

        os.popen('chmod -R 666 /tmp/fbrm').read()
        rman_data='/tmp/fbrm/rman_data'
        if not os.path.isdir(rman_data):
            os.makedirs(rman_data)


    def get_db_run(self):


        target_dir = os.path.join('/tmp',  'rman_sql')
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)

        src_file = os.path.join('sql', 'rman', 'rman_today_job.sql')
        tar_file = os.path.join(target_dir, 'fbrm_rman.sql')
        cat_ps = os.popen('ps -ef | grep ora_pmon | grep catdb | grep -v grep').read().split()
        # shutil.copy(src_file,tar_file)
        with open(src_file) as f:
            lineset = f.read()
        print lineset
        print '{HOSTNAME}' in lineset
        n_lineset = lineset.replace('{HOSTNAME}', self.hostname)
        print n_lineset
        with open(tar_file, 'w') as fw:
            fw.write(n_lineset)

        print os.popen('chmod -R 777 /tmp/fbrm').read()
        print os.popen('chmod -R 777 /tmp/fbrm/rman_sql').read()
        if len(cat_ps) > 4:
            cat_owner = cat_ps[0]
            print cat_owner
        os.popen('chmod 777 %s' % tar_file)
        user = self.cfg.get('rmancat', 'user')
        passwd = self.cfg.get('rmancat', 'passwd')
        dbname = self.cfg.get('rmancat', 'dbname')

        cmd = """su - oracle -c "export ORACLE_SID=catdb;sqlplus -s '%s/%s@%s '< '%s'" """ % (
        user, passwd, dbname, tar_file)
        print cmd
        print os.popen(cmd).read()


    
    def rman(self,db_name,start_time):
        """
        [rmancat]
        user = rmancat
        passwd = welcome1
        dbname = catdb
        """

        src_file=os.path.join('sql','rman','fbrm_rman.sql')
        tar_file=os.path.join('.','data','fbrm_rman.sql')
        cat_ps = os.popen('ps -ef | grep ora_pmon | grep catdb | grep -v grep').read().split()
        #shutil.copy(src_file,tar_file)
        rman_user=self.cfg.get('rmancat','user')
        rman_passwd=self.cfg.get('rmancat','passwd')
        rman_sid=self.cfg.get('rmancat','dbname')
        """
        rman target / catalog rmancat/welcome1@catd << EOF
        list copy tag 'ZFS_UPGR' completed after "to_date('2020/08/25 15:00:00','yyyy/mm/dd hh24:mi:ss')";
        EOF
        """
        start_time ='2020/08/25 15:00:00'
        rman_cmd = """rman target / catalog {RMAN_USER}/{RMAN_PASS}@{RMAN_SID} << EOF 
list copy tag 'ZFS_{DB_NAME}' completed after \"to_date('{START_TIME}','yyyy/mm/dd hh24:mi:ss')\";
EOF
""".format(RMAN_USER=rman_user,RMAN_PASS=rman_passwd,RMAN_SID=rman_sid,DB_NAME=db_name,START_TIME=start_time)

        print rman_cmd
        with open('/tmp/rman_progress.sh','w') as fw:
            fw.write(rman_cmd)
        cmd="""su - oracle -c "sh /tmp/rman_progress.sh" """
        print cmd

        ret= os.popen(cmd).read()
        print ret



    def set_sql_file(self,sid):
        src_files=os.path.join('sql','rman','fbrm_rman.sql')
        print src_files
        tar_dir='/tmp/fbrm/rman_sql/fbrm_rman.sql'
        print src_files

        with open(src_file) as f:
            lineset=f.read()
        lineset=lineset.replace('{HOSTNAME}',self.hostname)
        lineset=lineset.replace('{DBNAME}',sid)
        #print lineset
        print 'tar_file :',tar_file,os.path.isfile(tar_file)
        print os.popen('chmod -R 777 /tmp/fbrm').read()
        with open(tar_file,'w') as fw:
            fw.write(lineset)
        os.popen('chmod 666 %s'%(tar_file))

        return tar_file








    


    def get_data_list(self):
        data_list = glob.glob(os.path.join('/tmp', 'fbrm', 'data', '*.tmp'))
        return data_list

    def get_ret(self):
        data_file='/tmp/fbrm/rman_data/%s_catdb_list_backup_detail.tmp'%self.hostname
        print data_file
        print os.path.isfile(data_file)
        with open(data_file) as f:
            lineset=f.read()
        lineset=lineset.replace('{HOSTNAME}',self.hostname)
        oracle_home = os.popen('env | grep ORACLE_HOME').read().strip()

        with open(self.file_name,'w') as fw:
            msg=self.com.getHeadMsg('RMANCATDB_%s'%self.hostname)
            fw.write(msg+'\n')
            fw.write('***###datetime###***\n')
            fw.write(self.now+'\n')
            fw.write('***###hostname###***\n')
            fw.write(self.cfg.get('fbrm_agent','hostname') + '\n')
            fw.write('***###agent_ip###***\n')
            fw.write(self.cfg.get('fbrm_agent','agent_ip') + '\n')
            fw.write('***###ORACLE_HOME###***\n')
            fw.write(oracle_home + '\n')
            fw.write('***###rman_catalog###***\n')
            fw.write(lineset)
            end_msg=self.com.getEndMsg()
            fw.write(end_msg)



        return data_file

    def getHeadMsg(self, title='FLETA BATCH LAOD'):
        now = self.getNow()
        msg = '\n'
        msg += '#' * 79 + '\n'
        msg += '#### ' + ' ' * 71 + '###\n'
        msg += '#### ' + ('TITLE     : %s' % title).ljust(71) + '###\n'
        msg += '#### ' + ('DATA TIME : %s' % now).ljust(71) + '###\n'
        msg += '#### ' + ' ' * 71 + '###\n'
        msg += '#' * 79 + '\n'
        return msg

    def getEndMsg(self):
        now = self.getNow()
        msg = '\n'
        msg += '#' * 79 + '\n'
        msg += '####  ' + ('END  -  DATA TIME : %s' % now).ljust(71) + '###\n'
        msg += '#' * 79 + '\n'
        return msg

    def get_job_list(self):
        td= datetime.datetime.now().strftime('%Y%m%d')
        print td
        print os.path.join('data', 'rman_*%s*' % td)
        job_file_list=glob.glob(os.path.join('data','rman_*%s*'%td))
        return sorted(set(job_file_list))[-1]


    def get_old_session_list(self):

        ss_file = os.path.join('config', 'session_list.txt')
        if os.path.isfile(ss_file):
            with open(ss_file) as f:
                lineset = f.read()
                old_session_list= lineset.split('\n')

        else:
            old_session_list = []
        return old_session_list


    def set_old_sseion_list(self,dt):
        ss_file = os.path.join('config', 'session_list.txt')
        ftime = time.strftime('%Y-%m-%d', time.gmtime(os.path.getmtime(ss_file)))
        dtime = datetime.datetime.now().strftime('%Y-%m-%d')
        print ftime,dtime,ftime==dtime
        if not ftime == dtime:
            os.remove(ss_file)
        with open(ss_file,'a') as f:
            f.write(dt+'\n')



    def get_progres(self,f_name):

        with open(f_name) as f:
            lineset=f.read()
        for line in lineset:
            print line

    def rman_daily_backup_status(self,db_name):
        """
        [rmancat]
        user = rmancat
        passwd = welcome1
        dbname = catdb
        """
        sql_file = '/tmp/{DB_NAME}_rman_daily.sql'.format(DB_NAME=db_name)
        sql="""
        
set ECHO OFF
set pages 0
set HEADING OFF
set LINESIZE 2048
set PAGESIZE 2000
set TERM OFF
set TRIMS ON
set colsep ,
spool /tmp/{DB_NAME}_rman_daily.tmp


SELECT DB_KEY,
DB_NAME,
SESSION_KEY,
SESSION_RECID,
SESSION_STAMP,
TO_CHAR(START_TIME,'yyyy/mm/dd hh24:mi:ss') START_TIME,
TO_CHAR(END_TIME,'yyyy/mm/dd hh24:mi:ss') END_TIME,
STATUS,
INPUT_TYPE
FROM RC_RMAN_BACKUP_JOB_DETAILS 
WHERE DB_NAME = '{DB_NAME}'
--and start_time >= sysdate - 5 
ORDER BY START_TIME desc
;  
SPOOL OFF
""".format(DB_NAME=db_name)

        with open(sql_file, 'w') as fw:
            fw.write(sql)


        with open(sql_file) as f:
            print f.read()
        os.popen('chmod 777 %s' % sql_file)
        user = self.cfg.get('rmancat', 'user')
        passwd = self.cfg.get('rmancat', 'passwd')
        dbname = self.cfg.get('rmancat', 'dbname')

        cmd = """su - oracle -c "export ORACLE_SID=catdb;sqlplus -s '%s/%s@%s' < %s" """ % (
        user, passwd, dbname, sql_file)
        print cmd
        ret= os.popen(cmd).read()


        for line in ret.splitlines():
            if ',' in line:
                lineset=line.split(',')

                if len(lineset) == 9 :

                    db_key=lineset[0].strip()
                    db_name = lineset[1].strip()
                    session_key = lineset[2].strip()
                    session_recid = lineset[3].strip()
                    session_stamp  = lineset[4].strip()
                    start_time = lineset[5].strip()
                    end_time = lineset[6].strip()
                    status = lineset[7].strip()
                    backup_type = lineset[8].strip()

                    dt = '%s %s %s %s'%(db_name, session_key, session_recid, session_stamp)

                    if not end_time == '':

                        print 'START_TIME :',start_time,end_time,status,backup_type

                        shell_cmd = """
rman target / catalog rmancat/welcome1@catdb << EOF 
LIST COPY tag 'ZFS_{DB_NAME}' completed between "to_date('{START_TIME}','yyyy/mm/dd hh24:mi:ss')" and "to_date('{END_TIME}','yyyy/mm/dd hh24:mi:ss')";
EOF
""".format(DB_NAME=db_name, START_TIME=start_time, END_TIME=end_time)

                    else:
                        if status=='RUNNING':

                            shell_cmd = """
rman target / catalog rmancat/welcome1@catdb << EOF 
LIST COPY tag 'ZFS_{DB_NAME}' completed after "to_date('{START_TIME}','yyyy/mm/dd hh24:mi:ss')" ;
EOF
    """.format(DB_NAME=db_name, START_TIME=start_time)
                        else:
                            shell_cmd=''
                    old_session_list = self.get_old_session_list()

                    if dt not in old_session_list :

                        shell_file = '/tmp/{DB_NAME}_rman.sh'.format(DB_NAME=db_name)
                        if status == 'COMPLETED':
                            self.set_old_sseion_list(dt)

                        rman_cmd = """su - oracle -c "sh {SHELL_FILE} " """.format(SHELL_FILE=shell_file)
                        print rman_cmd
                        with open(os.path.join('config', 'session_list.txt'), 'a') as fw:
                            fw.write(dt + '\n')
                        rman_ret= os.popen(rman_cmd).read()
                        if 'Name:' in rman_ret:
                            print rman_ret
                            print db_name, session_key, session_recid, session_stamp
                            ret_file = 'rman_progress_{DB_NAME}_{SESSION_KEY}_{SESSION_RECID}_{SESSION_STAMP}.txt'.format(DB_NAME=db_name,SESSION_KEY=session_key,SESSION_RECID=session_recid,SESSION_STAMP=session_stamp)
                            print ret_file
                            with open(ret_file,'w') as fw:
                                head_msg=self.com.getHeadMsg('RMAN PROGRESS_%s'%db_name)
                                fw.write(head_msg+'\n')
                                fw.write('###***DB_NAME***###'+'\n')
                                fw.write(db_name+'\n')
                                fw.write('###***START_TIME***###' + '\n')
                                fw.write(start_time + '\n')
                                fw.write('###***END_TIME***###' + '\n')
                                fw.write(end_time + '\n')
                                fw.write('###***SESSION_KEY***###' + '\n')
                                fw.write(session_key + '\n')
                                fw.write('###***SESSION_RECID***###' + '\n')
                                fw.write(session_recid + '\n')
                                fw.write('###***SESSION_STAMP***###' + '\n')
                                fw.write(session_stamp + '\n')
                                fw.write('###***RMAN_PROGRESS***###' + '\n')
                                fw.write(rman_ret)
                                end_msg = self.com.getEndMsg()
                                fw.write(end_msg)

                            socketClient.SocketSender(FILENAME=ret_file, DIR='FBRM_RMAN_PROGRESS', ENDCHECK='YES').main()

                            try:
                                os.remove(ret_file)
                            except:
                                pass
                            try:
                                os.remove(ret_file)
                            except:
                                pass

    def get_db_list(self):
        user = self.cfg.get('rmancat', 'user')
        passwd = self.cfg.get('rmancat', 'passwd')
        dbname = self.cfg.get('rmancat', 'dbname')
        sql_cmd = "select db_name from rc_rman_backup_job_details group by db_name;"
        cmd = """su - oracle -c "export ORACLE_SID=UPGR;sqlplus -s '%s/%s@%s' <<< '%s' " """ % (
            user, passwd, dbname, sql_cmd)
        print cmd
        ret = os.popen(cmd).read()
        #print ret
        lineset = ret.splitlines()
        line_bit = False
        db_list=[]
        for i in range(len(lineset)):
            line = lineset[i]

            if '-' in line:
                line_bit = True
            if line_bit:
                print line
                if not '-' in line:
                    if len(line) >0:
                        db_list.append(line.strip())
        return db_list


    def main(self):

        db_list=self.get_db_list()
        for db_name in db_list:
            print db_name
            self.rman_daily_backup_status(db_name)


if __name__=='__main__':
    fbrm_ora().main()
#    while True:
#        fbrm_ora().main()
#        time.sleep(60)

