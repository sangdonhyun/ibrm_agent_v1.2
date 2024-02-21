'''
Created on 2014. 4. 16.

@author: muse
@ patch 20210906 sql query split lines

'''

import os
import datetime
import time
import ConfigParser
import common
import re
import platform
import getpass

class rman_control():
    def __init__(self,**kwargs):
        print("#"*100)
        print(kwargs)
        print("#"*100)
        self.ora_sid = kwargs['ora_sid']
        self.shell_type = kwargs['shell_type']
        self.db_name = kwargs['db_name']
        self.owner = self.get_owner()
        self.job_id = kwargs['job_id']
        self.tg_job_dtl_id = kwargs['tg_job_dtl_id']

        self.o_common = common.Common()
        self.cfg = self.o_common.cfg
        self.ora_home = ''


    def get_uname(self):
        return getpass.getuser()

    def get_home_path(self):
        return os.path.expanduser('~')

    def get_ora_home_bit(self):
        if self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.o_common.get_ora_home(self.db_name)

    def get_owner(self):
        cmd = 'ps -ef | grep ora_pmon_{ORA_SID} | grep -v grep '.format(ORA_SID=self.ora_sid)
        print cmd
        try:
            owner=os.popen(cmd).read().split()[0]
        except:
            owner = None
        return owner

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
select
'SESSION_KEY,',SESSION_KEY from V_$RMAN_BACKUP_JOB_DETAILS WHERE SESSION_KEY > '{OLD_SESSION_ID}' and INPUT_TYPE = '{INPUT_TYPE}';""".format(OLD_SESSION_ID=old_session_id,INPUT_TYPE=input_type)
        qfile = "{}_{}_{}_{}_get_session_id.sql".format(self.tg_job_dtl_id,self.db_name, self.job_id, self.shell_type)
        qfile = os.path.join(self.o_common.s_sql_path, qfile)

        with open(qfile,'w') as fw:
            fw.write(sql)

        os.chmod(qfile, 0644)

        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            if self.get_owner() == 'root':
                cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORA_SID};sqlplus \$rman_user/\$rman_passwd@\$rman_SID as sysdba < {SQL_FILE}" """.format(OWNER=self.owner,ORA_SID=self.ora_sid, SQL_FILE=qfile)
            else:
                ora_home_path = self.get_home_path()
                cmd = """. {ORA_HOME_PATH}/.ibrm_profile;export ORACLE_SID={ORA_SID};sqlplus $rman_user/$rman_passwd@$rman_SID as sysdba < {SQL_FILE} """.format(
                    ORA_HOME_PATH=ora_home_path, ORA_SID=self.ora_sid, SQL_FILE=qfile)

        elif self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.o_common.get_ora_home(self.db_name)
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba'  <{QEURY_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid, QEURY_FILE=qfile, ORACLE_HOME=oracle_home)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba'  <{QEURY_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid, QEURY_FILE=qfile)

        cnt = 1
        while True:
            session_id = ''
            try:
                ret=os.popen(cmd).read().strip()
            except Exception as e:
                print(e)


            for line in ret.splitlines():
                if 'SESSION_KEY' in line:
                    session_id = line.split(',')[-1].strip()
                    break

            if session_id.isdigit():
                print ('session key : ', session_id)
                break

            if self.shell_type in ['INCR_L0', 'INCR_L1', 'FULL_L0', 'MRG', 'INCR_MRG']:
                time.sleep(30)
                if cnt == 30*2*60*6:
                    break
                cnt = cnt+1
            else:
                time.sleep(10)
                if cnt == 10*60*6:
                    break
                cnt = cnt + 1
        os.remove(qfile)
        return session_id

    def ora_datetime(self):
        sql_file ="{}_{}_{}_{}_ora_sysdate.sql".format(self.tg_job_dtl_id,self.db_name, self.job_id, self.shell_type)
        sql_file = os.path.join(self.o_common.s_sql_path, sql_file)
        with open(sql_file,'w') as fw:
            fw.write("set HEADING OFF\n")
            fw.write("SET TIMING OFF\n")
            fw.write("SELECT 'SYSDATE,',TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS') ORA_SYS_DATE FROM DUAL;")
        os.chmod(sql_file, 0644)

        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            if self.get_owner() == 'root':
                cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORA_SID};sqlplus \$rman_user/\$rman_passwd@\$rman_SID as sysdba < {SQL_FILE}" """.format(OWNER=self.owner, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
            else:
                ora_home_path = self.get_home_path()
                cmd = """. {ORA_HOME_PATH}/.ibrm_profile;export ORACLE_SID={ORA_SID};sqlplus $rman_user/$rman_passwd@$rman_SID as sysdba < {SQL_FILE} """.format(
                    ORA_HOME_PATH=ora_home_path, ORA_SID=self.ora_sid, SQL_FILE=sql_file)

        elif self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.o_common.get_ora_home(self.db_name)

            self.cfg.get('common', 'ora_home_bit')
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid,SQL_FILE=sql_file,ORACLE_HOME=oracle_home)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print(e)
        # finally:
        #     os.remove(sql_file)

        try:
            find_date_time = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}', ret)
            sysdate = find_date_time.group()
        except:

            sysdate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("sysdate :",sysdate)
        return sysdate

    def get_last_session_id(self):

        sql = """SET HEADING OFF
        SET TIMING OFF
        SELECT 'SESSION_KEY,',SESSION_KEY FROM (
        SELECT SESSION_KEY FROM V_$RMAN_BACKUP_JOB_DETAILS  ORDER BY SESSION_KEY DESC
        ) WHERE ROWNUM <=1 ; """
        sql_file = "{}_{}_{}_{}_last_session_key.sql".format(self.tg_job_dtl_id,self.db_name, self.job_id, self.shell_type)
        sql_file = os.path.join(self.o_common.s_sql_path, sql_file)
        with open(sql_file,'w') as fw:
            fw.write(sql)
        os.chmod(sql_file, 0644)
        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            if self.get_owner() == 'root':
                cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORA_SID};sqlplus \$rman_user/\$rman_passwd@\$rman_SID as sysdba < {SQL_FILE}" """.format(OWNER=self.owner, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
            else:
                ora_home_path = self.get_home_path()
                cmd = """. {ORA_HOME_PATH}/.ibrm_profile;export ORACLE_SID={ORA_SID};sqlplus $rman_user/$rman_passwd@$rman_SID as sysdba < {SQL_FILE} """.format(
                    ORA_HOME_PATH=ora_home_path, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        elif self.cfg.get('common','ora_home_bit') == 'True':
            oracle_home = self.o_common.get_ora_home(self.db_name)

            self.cfg.get('common', 'ora_home_bit')
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid,SQL_FILE=sql_file,ORACLE_HOME=oracle_home)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid,SQL_FILE=sql_file)
        try:
            ret=os.popen(cmd).read().strip()
        except Exception as e:
            print(e)
        finally:
            os.remove(sql_file)
        session_id = ''

        for line in ret.splitlines():
            if 'SESSION_KEY' in line:
                session_id = line.split(',')[-1].strip()
        print 'session id:',session_id
        return session_id
