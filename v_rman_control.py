"""
* *******************************************************
* Copyright (c) Fleta Communications, Inc. 2020. All Rights Reserved.
* *******************************************************
* create date :
* modipy date : 2021-12-01
"""
__author__ = 'TODO: muse@fletacom.com'
__ibrm_agent_version__ = 'TODO: v1.2'
import os
import datetime
import time
import ConfigParser
import common
"""
[rmancat]
user = rmancat
passwd = welcome1
dbname = catdb
"""
user="rmancat"
passwd = "welcome1"
dbname = 'catdb'


class rman_control():
    def __init__(self,**kwargs):
        self.ora_sid = kwargs['ora_sid']
        self.shell_type = kwargs['shell_type']
        self.db_name  =kwargs['db_name']
        self.owner = self.get_owner()
        # print 'ora sid :',self.ora_sid
        # print 'shell_type :', self.shell_type
        # print 'db_name :', self.db_name
        self.com = common.Common()
        self.cfg = self.com.cfg
        self.ora_home = ''







    def get_ora_home_bit(self):
        if self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.db_name)

    def get_owner(self):
        cmd = 'ps -ef | grep ora_pmon_{ORA_SID} | grep -v grep '.format(ORA_SID=self.ora_sid)
        print cmd
        try:
            owner=os.popen(cmd).read().split()[0]
        except:
            owner = None
        return owner




    def make_sql(self,**kwargs):
        print 'kwargs : ',kwargs
        sysdate = kwargs['sysdate']
        session_id = kwargs['session_id']
        print 'session_id :' ,session_id
        if self.shell_type == 'level0' or self.shell_type =='level1':
            input_type = 'DB INCR'
        elif self.shell_type == 'arch':
            input_type = 'ARCHIVELOG'

        rman_time = datetime.datetime.now().strftime('%Y-%m-%d %H')


        sql="""set ECHO OFF 
set pages 0 
set HEADING OFF
set LINESIZE 2048
set PAGESIZE 2000
set HEADING OFF
SET TIMING OFF
set colsep ,
SELECT STATUS,to_char(START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,OUTPUT_BYTES_PER_SEC_DISPLAY
 FROM V_$RMAN_BACKUP_JOB_DETAILS WHERE SESSION_KEY={SESSION_ID}
""".format(SESSION_ID=session_id)

        print sql
        sfile = '/tmp/{SID}_rman.sql'.format(self.ora_sid)
        with open(sfile,'w') as f:
            f.write(sql)
        os.popen('chmod 777 {}'.format(sfile))



    def get_session_id(self, old_session_id):
        if self.shell_type == 'level0':
            input_type = 'DB INCR'
        elif self.shell_type == 'INCR':
            input_type = 'DB INCR'
        elif self.shell_type == 'ARCH':
            input_type = 'ARCHIVELOG'
        elif self.shell_type == 'FULL_L0':
            input_type = 'DB FULL'
        else:
            input_type='DB INCR'
        time.sleep(3)

        sql = """set ECHO OFF 
set pages 0 
set HEADING OFF
SET TIMING OFF
set colsep ,
select  'SESSION_KEY',
SESSION_KEY from V_$RMAN_BACKUP_JOB_DETAILS WHERE SESSION_KEY > '{OLD_SESSION_ID}' and INPUT_TYPE = '{INPUT_TYPE}';""".format(OLD_SESSION_ID=old_session_id,INPUT_TYPE=input_type)
        qfile = os.path.join('/tmp','{}_get_session_id.sql'.format(self.ora_sid))
        print sql
        with open(qfile,'w') as fw:
            fw.write(sql)
        os.popen('chmod 777 {}'.format(qfile))

        if self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.db_name)
            print 'oracle_home :', oracle_home
            print 'ora_home_bit :', self.cfg.get('common','ora_home_bit')
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba'  <{QEURY_FILE}"
            """.format(OWNER=self.owner,ORA_SID=self.ora_sid, QEURY_FILE=qfile, ORACLE_HOME=oracle_home)

        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba'  <{QEURY_FILE}"
            """.format(OWNER=self.owner,ORA_SID=self.ora_sid, QEURY_FILE=qfile)
        print cmd
        cnt = 1
        while True:

            ret = os.popen(cmd).read()
            print ret
            if not 'no rows selected' in ret:
                break
            if self.shell_type in ['INCR_L0', 'INCR_L1', 'FULL_L0', 'MRG', 'INCR_MRG']:
                time.sleep(30)
                if cnt == 30*2*60*6:
                    break
                cnt = cnt+1
            else:
                time.sleep(10)
                if cnt == 360:
                    break
                cnt = cnt + 1


        session_id=""
        for line in ret.splitlines():
            if 'SESSION_KEY' in line:
                session_id=line.split(",")[-1].strip()
        return session_id

    def ora_datetime(self):
        sql_file = os.path.join('/tmp','ora_sysdate.sql')
        with open(sql_file,'w') as fw:
            fw.write("set HEADING OFF\n")
            fw.write("SET TIMING OFF\n")
            fw.write("SELECT 'SYSDATE,',TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') ORA_SYS_DATE FROM DUAL;")
        os.popen('chmod 777 {}'.format(sql_file))
        if self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.db_name)
            print 'oracle_home :',oracle_home
            self.cfg.get('common', 'ora_home_bit')
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner,ORA_SID=self.ora_sid,SQL_FILE=sql_file,ORACLE_HOME=oracle_home)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner,ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        print cmd
        ret = os.popen(cmd).read()
        print ret
        sysdate=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for line in ret.splitlines():
            if 'SYSDATE' in line:
                sysdate = line.split(',')[-1].strip()

        # try:
        #     sysdate = ret.strip().splitlines()[-1]
        # except:
        #     sysdate =''

        return sysdate





    def get_last_session_id(self):

        sql = """SET HEADING OFF
        SET TIMING OFF 
        SELECT 'SESSION_KEY,',SESSION_KEY FROM (
        SELECT SESSION_KEY FROM V_$RMAN_BACKUP_JOB_DETAILS  ORDER BY SESSION_KEY DESC
        ) WHERE ROWNUM <=1 ; """
        sql_file = '/tmp/{}_last_session_key.sql'.format(self.ora_sid)
        if not os.path.isfile(sql_file):
            with open(sql_file,'w') as fw:
                fw.write(sql)
        print 'sql',sql
        os.popen('chmod 777 {}'.format(sql_file))
        if self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.db_name)
            print 'oracle_home :',oracle_home
            self.cfg.get('common', 'ora_home_bit')
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner ,ORA_SID=self.ora_sid,SQL_FILE=sql_file,ORACLE_HOME=oracle_home)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"

            """.format(OWNER=self.owner ,ORA_SID=self.ora_sid,SQL_FILE=sql_file)
        print 'cmd :',cmd
        ret=os.popen(cmd).read().strip()
        print 'ret : ',ret
        session_id = ''
        for line in ret.splitlines():
            if 'SESSION_KEY' in line:
                session_id = line.split(',')[-1].strip()
        # session_id = ret.strip()
        print 'session id:',session_id
        return session_id

    def job_monitor(self,**kwargs):
        #sysdate = kwargs['sysdate']
        session_id= kwargs['session_id']
        #self.make_sql(sysdate=sysdate,session_id=session_id)
        sql = """col OUTPUT_BYTES_PER_SEC_DISPLAY for a20
col START_TIME for a21
col END_TIME for a21

SET PAGES 0 
SET HEADING ON
SET LINESIZE 2048
SET PAGESIZE 2000
SET TIMING ON
SET COLSEP ,
SELECT 'SESSION_MON,',
OUTPUT_BYTES_PER_SEC_DISPLAY,
TO_CHAR(START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,
TO_CHAR(END_TIME,'yyyy-mm-dd hh24:mi:ss') END_TIME,
ELAPSED_SECONDS,
STATUS,
INPUT_TYPE
 FROM V_$RMAN_BACKUP_JOB_DETAILS WHERE SESSION_KEY='{}'; 
""".format(session_id)

        sql_file = '/tmp/{}_session_monitor.sql'.format(self.ora_sid)
        with open(sql_file,'w') as f:
            f.write(sql)
        os.popen('chmod 777 {}'.format(sql_file))


        cmd = """
        su - oracle -c "export ORACLE_SID=catdb;sqlplus -s 'rmancat/welcome1@catdb '< %s" 
        """%sql_file
        if self.cfg.get('common', 'ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.db_name)
            cmd = """su - {OWNER} -c "ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner,ORACLE_HOME=oracle_home,ORA_SID=self.ora_sid,SQL_FILE=sql_file)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner,ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        print cmd
        ret =os.popen(cmd).read()


        print ret
        print '*'*50
        """
        ret :
        10.75M	    ,2020-10-13 17:30:26  ,2020-10-13 17:31:18	,	      52,COMPLETED		,DB INCR
        """
        data={}
        for line in ret.splitlines():
            if 'SESSION_MON' in line:
                if ','in ret:
                    line_set = ret.split(',')
                    data['session_id'] = session_id
                    data['write_bps'] = line_set[1].strip()
                    data['start_time'] = line_set[2].strip()
                    data['end_time'] = line_set[3].strip()
                    data['elapsed_seconds'] = line_set[4].strip()
                    data['status'] = line_set[5].strip()
                    data['input_type'] = line_set[6].strip()
                print 'data :',data
        return data




if __name__=='__main__':
    shell_type='Level0'
    db_name ='ORCL'
    ora_sid = 'ORCL'
    rman = rman_control(shell_type=shell_type,db_name =db_name,ora_sid=ora_sid)


    sysdate=rman.ora_datetime()
    print '-'*30

    print 'sysdate :',sysdate
    print '#'*50
    last_session_key = rman.get_last_session_id()
    print 'last session key :',last_session_key

    #session_id=  rman.get_session_id(sysdate,last_session_key)
    #print 'new session_id', session_id
    monitor_data=rman.job_monitor(session_id=last_session_key)
    print 'monitor_data :',monitor_data

#
#    while True:
#        data=rman.job_monitor(sysdate=rtime,session_id=session_id)
#        if not 'RUNNING' in data['status']:
#            break
#        print data
#        time.sleep(5)

