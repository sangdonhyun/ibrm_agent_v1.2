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
        self.com = common.Common()
        self.shell_type = kwargs['shell_type']
        self.db_name  =kwargs['db_name']
        self.ora_sid = kwargs['ora_sid']
        self.owner = self.get_owner(self.ora_sid)

        self.cfg = self.com.cfg
        self.ora_home = ''
        self.to_day = datetime.datetime.now().strftime('%Y-%m-%d')







    def get_ora_home_bit(self):
        if self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.db_name)

    def get_owner(self):

        cmd = 'ps -ef | grep ora_pmon_{ORA_SID} | grep -v grep '.format(ORA_SID=self.ora_sid)

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

        to_day = datetime.datetime.now().strftime('%Y-%m-%d')
        sql="""set ECHO OFF 
set pages 0 
set HEADING OFF
set LINESIZE 2048
set PAGESIZE 2000
set HEADING OFF
set colsep ,
SELECT STATUS,to_char(START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,OUTPUT_BYTES_PER_SEC_DISPLAY
 FROM V_$RMAN_BACKUP_JOB_DETAILS WHERE to_char(START_TIME,'yyyy-mm-dd') = '{TO_DAY}'
""".format(TO_DAY=to_day)

        print sql
        with open('/tmp/rman.sql','w') as f:
            f.write(sql)
        os.popen('chmod 777 /tmp/rman.sql')

    def get_session_id(self, old_session_id):
        if self.shell_type == 'level0':
            input_type = 'DB INCR'
        elif self.shell_type == 'arch':
            input_type = 'ARCHIVELOG'
        else:
            input_type='DB INCR'
        time.sleep(3)

        sql = """set ECHO OFF 
set pages 0 
set HEADING OFF
set TIMING OFF
select 
SESSION_KEY from V_$RMAN_BACKUP_JOB_DETAILS WHERE SESSION_KEY > '{OLD_SESSION_ID}' and INPUT_TYPE = '{INPUT_TYPE}';""".format(OLD_SESSION_ID=old_session_id,INPUT_TYPE=input_type)
        qfile = os.path.join('/tmp','get_session_id.sql')
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
            time.sleep(3)
            if cnt == 60:
                break
            cnt = cnt+1
        print ret

        session_id = ret.strip()

        return session_id

    def ora_datetime(self):
        sql_file = os.path.join('/tmp','ora_sysdate.sql')
        with open(sql_file,'w') as fw:
            fw.write("set HEADING OFF\n")
            fw.write("set TIMING OFF\n")
            fw.write("SELECT TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') ORA_SYS_DATE FROM DUAL;")
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
        sysdate=''
        sysdate = ret.strip().splitlines()[-1]

        return sysdate

    def get_owner(self,sid):
        cmd = 'ps -ef | grep ora_pmon_ | grep -v grep'
        ret=os.popen(cmd).read()
        owner=''
        for line in ret.splitlines():
            if sid.upper() in line.upper():
                owner = line.split()[0]
        return owner




    def get_last_session_id(self):

        sql = """SET HEADING OFF
        SELECT SESSION_KEY FROM (
        SELECT SESSION_KEY FROM V_$RMAN_BACKUP_JOB_DETAILS  ORDER BY SESSION_KEY DESC
        ) WHERE ROWNUM <=1 ; """
        sql_file = '/tmp/last_session_key.sql'
        with open(sql_file,'w') as fw:
            fw.write(sql)
        os.popen('chmod 777 %s'%sql_file).read()


        if self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.db_name)
            print 'oracle_home :',oracle_home
            self.cfg.get('common', 'ora_home_bit')
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOEM};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner ,ORA_SID=self.ora_sid,SQL_FILE=sql_file,ORACLE_HOEM=oracle_home)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"

            """.format(OWNER=self.owner ,ORA_SID=self.ora_sid,SQL_FILE=sql_file)

        ret=os.popen(cmd).read().strip()
        print ret
        session_id = ret.strip()
        return session_id

    def job_monitor(self,**kwargs):
        #sysdate = kwargs['sysdate']
        session_id= kwargs['session_id']
        #self.make_sql(sysdate=sysdate,session_id=session_id)
        sql = """col OUTPUT_BYTES_PER_SEC_DISPLAY for a20
col START_TIME for a21
col END_TIME for a21

SET PAGES 0 
SET HEADING OFF
SET LINESIZE 2048
SET PAGESIZE 2000
SET TIMING OFF
SET COLSEP ,
SELECT 
OUTPUT_BYTES_PER_SEC_DISPLAY,
TO_CHAR(START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,
TO_CHAR(END_TIME,'yyyy-mm-dd hh24:mi:ss') END_TIME,
ELAPSED_SECONDS,
STATUS,
INPUT_TYPE
 FROM V_$RMAN_BACKUP_JOB_DETAILS WHERE to_char(START_TIME,'yyyy-mm-dd') = '{TO_DAY}';
""".format(TO_DAY=self.to_day)

        sql_file = '/tmp/session_monitor.sql'
        print sql
        with open(sql_file,'w') as f:
            f.write(sql)

        os.popen('chmod 777 %s' % sql_file).read()
        cmd = """su - oracle -c "export ORACLE_SID=catdb;sqlplus -s 'rmancat/welcome1@catdb '< %s" 
        """%sql_file


        if self.cfg.get('common', 'ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.db_name)
            cmd = """su - oracle -c "ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(ORACLE_HOME=oracle_home,ORA_SID=self.ora_sid,SQL_FILE=sql_file)
        else:
            cmd = """su - oracle -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        print cmd
        ret =os.popen(cmd).read()


        print ret
        print '*'*50
        """
        ret :
        10.75M	    ,2020-10-13 17:30:26  ,2020-10-13 17:31:18	,	      52,COMPLETED		,DB INCR
        """
        data={}
        if ','in ret:
            line_set = ret.split(',')
            data['session_id'] = session_id
            data['write_bps'] = line_set[0].strip()
            data['start_time'] = line_set[1].strip()
            data['end_time'] = line_set[2].strip()
            data['elapsed_seconds'] = line_set[3].strip()
            data['status'] = line_set[4].strip()
            data['input_type'] = line_set[5].strip()
        print 'data :',data
        return data


    def get_pid(self):
        pid = None
        if pid is None:
            with open('now_pid.txt','r') as f:
                tmp_pid = f.read().strip()
            cmd = 'ps -ef | grep ZFS | grep -v grep'.format()
            print 'shell process :',cmd
            ret = os.popen(cmd).read()
            for line in ret.splitlines():
                if tmp_pid in line :
                    pid = tmp_pid
        return pid


    def process_monitor(self):
        try:
            print 'process check...'
            print ''
            print 'daemon process check..'
            daemon_pid = os.popen('cat pid.txt').read().strip()
            cmd = 'ps -ef | grep {} | grep -v grep | grep -v job_monitor'.format(daemon_pid)
            print cmd
            print os.popen(cmd).read()
            print '-' * 50
            print 'shell process...'
            n_pid = os.popen('cat now_pid.txt').read().strip()
            cmd = 'ps -ef | grep {} | grep -v grep | grep -v job_monitor'.format(n_pid)
            print cmd
            print os.popen(cmd).read()
            print '-'*50

        except Exception as e:
            print str(e)
if __name__=='__main__':
    shell_type='Level0'
    db_name ='ORCL'
    ora_sid = 'ORCL'

    rman = rman_control(shell_type=shell_type,db_name =db_name,ora_sid=ora_sid)
    last_session_key = rman.get_last_session_id()
    sysdate=rman.ora_datetime()
    print '-'*30
    print 'sysdate :',sysdate
    print 'owner :',rman.get_owner(ora_sid)
    print 'last session key :',last_session_key

    #session_id=  rman.get_session_id(sysdate,last_session_key)
    #print 'new session_id', session_id
    monitor_data=rman.job_monitor(session_id=last_session_key)
    print monitor_data
    rman.process_monitor()
    rman.get_pid()

#
#    while True:
#        data=rman.job_monitor(sysdate=rtime,session_id=session_id)
#        if not 'RUNNING' in data['status']:
#            break
#        print data
#        time.sleep(5)

