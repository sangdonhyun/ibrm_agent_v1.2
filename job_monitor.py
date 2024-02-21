# -*- encoding:utf-8*-
import os
import ConfigParser
import common
import sys
import time
import datetime
import ibrm_server_daemon_send
import socketClient
import log_control
import re
import platform

log = log_control.LogControl()
class job_mon():
    def __init__(self,ora_sid,pid,session_key,job_id,shell_name,shell_type,db_name,sysdate_str,tg_job_dtl_id):
        self.cfg = self.get_cfg()
        self.session_key = session_key
        self.pid = pid
        self.ora_sid = ora_sid
        self.job_id = job_id
        self.shell_name = shell_name
        self.shell_type = shell_type
        self.db_name = db_name
        self.sysdate_str = sysdate_str
        self.tg_job_dtl_id = tg_job_dtl_id
        self.o_common = common.Common()
        self.socket_server = self.get_server()
        self.job_st= 'Running'
        self.log_date = datetime.datetime.strptime(self.sysdate_str.strip(), '%Y-%m-%d %H:%M:%S').strftime('%Y_%m%d_%H')
        self.log_date_min = datetime.datetime.strptime(self.sysdate_str.strip(), '%Y-%m-%d %H:%M:%S').strftime('%Y_%m%d_%H%M')
        self.owner = self.get_owner()

    def get_owner(self):
        cmd = "ps -ef |grep ora_pmon_%s |grep -v grep | awk '{print $1}'"%self.ora_sid
        ret=os.popen(cmd).read()
        return ret.strip()

    def log_resize(self,log_lines):
        if len(log_lines) > 1100:
            log_content = '\n'.join(log_lines[:100])
            log_content = log_content + '~' * 80 + '\n'
            log_content = log_content + '~' * 80 + '\n'
            log_content = log_content + 'LOG FILE SIZE TOO BIG' + '\n'
            log_content = log_content + '~' * 80 + '\n'
            log_content = log_content + '~' * 80 + '\n'
            log_content = log_content + '\n'.join(log_lines[-1000:])
            return log_content
        else:
            return '\n'.join(log_lines)

    def set_job_monitor(self):
        a_job_monitor_data = {
                         'ora_sid' : self.ora_sid
                        ,'pid' : self.pid
                        ,'session_key' : self.session_key
                        ,'job_id' : self.job_id
                        ,'shell_name' :self.shell_name
                        ,'shell_type' : self.shell_type
                        ,'db_name' : self.db_name
                        ,'sysdate' : self.sysdate_str
                        ,'tg_job_dtl_id' : self.tg_job_dtl_id
        }
        self.socket_server.job_set_monitor(a_job_monitor_data)

    def get_log(self):
        log_date = datetime.datetime.strptime(self.sysdate_str.strip(), '%Y-%m-%d %H:%M:%S')
        yyyy = log_date.strftime('%Y')
        mmdd = log_date.strftime('%m%d')
        log_path = self.cfg.get('common', 'log_path')

        s_file_header_prefix = self.db_name

        a_log_path_tmp  = re.findall('^/[^{]*/', log_path)

        if len(a_log_path_tmp) > 0:
            s_log_path_tmp_odg = os.path.join(a_log_path_tmp[0], s_file_header_prefix + 'DG')
            if os.path.isdir(s_log_path_tmp_odg):
                s_file_header_prefix = s_file_header_prefix + 'DG'

        log_path = log_path.replace('{DB_NAME}', s_file_header_prefix)
        log_path = log_path.replace('{YYYY}', yyyy)
        log_path = log_path.replace('{MMDD}', mmdd)

        if self.shell_type in ('INCR_L1', 'INCR_MRG'):

            log_file = '{DB_NAME}_RMAN_Level1_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date)
            log_file_min = '{DB_NAME}_RMAN_Level1_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date_min)
        elif self.shell_type == 'INCR_L0':

            log_file = '{DB_NAME}_RMAN_Level0_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date)
            log_file_min = '{DB_NAME}_RMAN_Level0_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date_min)
        elif self.shell_type == 'FULL_L0':

            log_file = '{DB_NAME}_RMAN_Full_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date)
            log_file_min = '{DB_NAME}_RMAN_Full_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date_min)
        elif self.shell_type == 'ARCH':

            log_file = '{DB_NAME}_Archive_log_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date)
            log_file_min = '{DB_NAME}_Archive_log_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date_min)

        elif self.shell_type == 'MRG':

            log_file = '{DB_NAME}_Merge_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date)
            log_file_min = '{DB_NAME}_Merge_{DATE_STRING}'.format(DB_NAME=s_file_header_prefix, DATE_STRING=self.log_date_min)

        else:
            log_file = ''

        target_log = os.path.join(log_path, '{LOG_FILE}.log'.format(LOG_FILE=log_file))
        target_log_min = os.path.join(log_path, '{LOG_FILE}.log'.format(LOG_FILE=log_file_min))

        #log type : ORACLE19_Archive_log_2023_0330_00.log
        if os.path.isfile(target_log):
            log_name = os.path.basename(target_log)
            with open(target_log) as f:
                log_data = f.read()
            log_data_line = log_data.splitlines()
        #log type : ORACLE19_Archive_log_2023_0330_0010.log
        elif os.path.isfile(target_log_min):
            log_name = os.path.basename(target_log_min)
            with open(target_log_min) as f:
                log_data = f.read()
            log_data_line = log_data.splitlines()
        else:
            log_name ='RMAN'
            log_data = self.get_rman_log(self.session_key)
            log_data_line = log_data.splitlines()

        try:
            self.log_content = self.log_resize(log_data_line)
        except:
            print('log content except')
            pass
        new_log_name = '{}_{}'.format(self.job_id, log_name)
        new_log_file = os.path.join('data', new_log_name)
        with open(new_log_file, 'w') as fw:
            fw.write(self.log_content)

        socketClient.SocketSender(FILENAME=new_log_file, DIR='ibrm_backup_log', ENDCHECK='NO').main()

        log_return_data = {}

        if os.path.isfile(target_log_min):
            log_return_data['log_file'] = target_log_min
        else:
            log_return_data['log_file'] = target_log

        log_return_data['log_contents'] = new_log_file
        log_return_data['pid'] = self.pid
        log_return_data['job_id'] = self.job_id
        log_return_data['tg_job_dtl_id'] = self.tg_job_dtl_id
        os.remove(new_log_file)
        return log_return_data

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config', 'config.cfg')
        cfg.read(cfg_file)
        return cfg

    def server_info(self):
        HOST = self.cfg.get('ibrm_server', 'ip')
        PORT = self.cfg.get('ibrm_server', 'socket_port')
        return HOST, PORT

    def get_server(self):
        HOST, PORT = self.server_info()
        socket_server = ibrm_server_daemon_send.SocketSender(HOST, int(PORT))
        return socket_server

    def rman_stats(self, session_id):
        sql = """SET PAGES 0
                SET LINESIZE 2048
                SET PAGESIZE 2000
                SET TIMING OFF
                SELECT
                SESSION_RECID AS SESSION_KEY
                ,TO_CHAR(START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME
                ,TO_CHAR(END_TIME,'yyyy-mm-dd hh24:mi:ss') END_TIME
                ,OBJECT_TYPE
                ,ROW_TYPE
                ,OPERATION
                ,STATUS
                ,INPUT_BYTES 
                ,OUTPUT_BYTES
                ,TO_CHAR(SYSTIMESTAMP,'yyyy-mm-dd hh24:mi:ss.FF3') AS QUERY_TIME
            FROM V$RMAN_STATUS V$RMAN_STATUS WHERE  session_recid='{SESSION_ID}' 
            ORDER BY start_time asc;
            col OUTPUT_BYTES_PER_SEC_DISPLAY for a20
            col START_TIME for a21
            col END_TIME for a21
            col INPUT_BYTES for a20
            col INPUT_BYTES_PER_SEC for a20
            col OUTPUT_BYTES_PER_SEC for a20
            col OUTPUT_BYTES for a20
            SET PAGES 0
            SET LINESIZE 2048
            SET PAGESIZE 2000
            SET TIMING OFF
            SET COLSEP ,
            SELECT
                ro.OUTPUT_BYTES_PER_SEC_DISPLAY,
                TO_CHAR(ro.START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,
                TO_CHAR(ro.END_TIME,'yyyy-mm-dd hh24:mi:ss') END_TIME,
                NVL(ro.ELAPSED_SECONDS, 0) AS ELAPSED_SECONDS,
                ro.STATUS,
                ro.INPUT_TYPE,
                ro.SESSION_RECID,
                ro.SESSION_STAMP,
                TO_CHAR(ro.INPUT_BYTES) AS INPUT_BYTES,
                TO_CHAR(TRUNC(ro.INPUT_BYTES_PER_SEC)) AS INPUT_BYTES_PER_SEC,
                TO_CHAR(TRUNC(ro.OUTPUT_BYTES_PER_SEC)) AS OUTPUT_BYTES_PER_SEC,
                TO_CHAR(ro.OUTPUT_BYTES) AS OUTPUT_BYTES,
                TO_CHAR(SYSTIMESTAMP,'yyyy-mm-dd hh24:mi:ss.FF3') AS QUERY_TIME
            FROM V$RMAN_BACKUP_JOB_DETAILS ro , v$rman_status rs
            where ro.SESSION_STAMP = rs.stamp
            AND ro.SESSION_KEY='{SESSION_ID}' ;
            """.format(SESSION_ID=session_id)
            
        sql_file_rman_stats = '{}_{}_{}_{}_rman_stats.sql'.format(self.tg_job_dtl_id,self.db_name, self.job_id, self.shell_type)
        sql_file = os.path.join(self.o_common.s_sql_path, sql_file_rman_stats)
        with open(sql_file, 'w') as f:
            f.write(sql)
        os.chmod(sql_file, 0644)

        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORA_SID};sqlplus \$rman_user/\$rman_passwd@\$rman_SID as sysdba < {SQL_FILE}" """.format(OWNER=self.owner, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        elif self.cfg.get('common', 'ora_home_bit') == 'True':
            oracle_home = self.o_common.get_ora_home(self.ora_sid)
            cmd = """/bin/su - {OWNER} -c "ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORACLE_HOME=oracle_home, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid, SQL_FILE=sql_file)

        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print ("job_monitor() Exception : %s" %(str(e)))
        finally:
            log.logdata('AGENT', 'DEBUG', '60008', "RMAN STATUS(V$RMAN_STATUS) QUERY : %s" %(str(sql)))
            os.remove(sql_file)
        return ret

    def job_monitor(self, **kwargs):

        session_id = kwargs['session_id']
        sql = """col OUTPUT_BYTES_PER_SEC_DISPLAY for a20
col START_TIME for a21
col END_TIME for a21
col INPUT_BYTES for a20
col INPUT_BYTES_PER_SEC for a20
col OUTPUT_BYTES_PER_SEC for a20
col OUTPUT_BYTES for a20
SET PAGES 0
SET LINESIZE 2048
SET PAGESIZE 2000
SET TIMING OFF
SET COLSEP ,
SELECT
    ro.OUTPUT_BYTES_PER_SEC_DISPLAY,
    TO_CHAR(ro.START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,
    TO_CHAR(ro.END_TIME,'yyyy-mm-dd hh24:mi:ss') END_TIME,
    NVL(ro.ELAPSED_SECONDS, 0) AS ELAPSED_SECONDS,
    ro.STATUS,
    ro.INPUT_TYPE,
    ro.SESSION_RECID,
    ro.SESSION_STAMP,
    TO_CHAR(ro.INPUT_BYTES) AS INPUT_BYTES,
    TO_CHAR(TRUNC(ro.INPUT_BYTES_PER_SEC)) AS INPUT_BYTES_PER_SEC,
    TO_CHAR(TRUNC(ro.OUTPUT_BYTES_PER_SEC)) AS OUTPUT_BYTES_PER_SEC,
    TO_CHAR(ro.OUTPUT_BYTES) AS OUTPUT_BYTES,
    TO_CHAR(SYSTIMESTAMP,'yyyy-mm-dd hh24:mi:ss.FF3') AS QUERY_TIME
FROM V$RMAN_BACKUP_JOB_DETAILS ro , v$rman_status rs
where ro.SESSION_STAMP = rs.stamp
AND ro.SESSION_KEY='{}';""".format(session_id)

        sql_file = '{}_{}_{}_{}_session_monitor.sql'.format(self.tg_job_dtl_id,self.db_name, self.job_id, self.shell_type)
        sql_file = os.path.join(self.o_common.s_sql_path, sql_file)
        with open(sql_file, 'w') as f:
            f.write(sql)
        os.chmod(sql_file, 0644)
        
        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORA_SID};sqlplus \$rman_user/\$rman_passwd@\$rman_SID as sysdba < {SQL_FILE}" """.format(OWNER=self.owner, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        elif self.cfg.get('common', 'ora_home_bit') == 'True':
            oracle_home = self.o_common.get_ora_home(self.ora_sid)
            cmd = """/bin/su - {OWNER} -c "ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORACLE_HOME=oracle_home, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid, SQL_FILE=sql_file)

        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print ("job_monitor() Exception : %s" %(str(e)))
        finally:
            os.remove(sql_file)

        data = {}

        ret = ret.strip()
        
        line_split = ret.split('\n')
        a_line = []
        for line in line_split:
            if re.match('^[A-Za-z\-\-\-]', line.strip()):
                pass
            else:
                a_line.append(line)
        if len(a_line) > 0:
            line_set = ''.join(a_line)
        else:
            line_set = ''
        if ',' in line_set:
            line_set = line_set.split(',')
            data['session_id'] = session_id
            data['write_bps'] = line_set[0].strip()
            try:
                data['query_time'] = line_set[-1].strip()
            except:
                data['query_time'] = ''
            try:
                data['start_time'] = line_set[1].strip()
                data['end_time'] = line_set[2].strip()
            except Exception as e:
                data['start_time'] = ''
                data['end_time'] = ''

            data['elapsed_seconds'] = line_set[3].strip()
            data['status'] = line_set[4].strip()
            data['input_type'] = line_set[5].strip()
            data['session_recid'] = line_set[6].strip()
            data['session_stamp'] = line_set[7].strip()
            data['input_bytes'] = line_set[8].split()[0].strip()

            try:
                data['input_bytes_per_sec'] = int(float(line_set[9].strip()))
                data['output_bytes_per_sec'] = int(float(line_set[10].strip()))
                data['output_bytes'] = line_set[11].strip()
            except Exception as e:
                # version 하위 호환성
                data['input_bytes_per_sec'] = -1
                data['output_bytes_per_sec'] = -1
                data['output_bytes'] = -1

            data['pid'] = self.pid
            data['job_id'] = self.job_id
            data['ora_sid'] = self.ora_sid
            data['job_st'] = self.job_st
            data['tg_job_dtl_id'] = self.tg_job_dtl_id
        else:
            data['pid'] = self.pid
            data['job_id'] = self.job_id
            data['ora_sid'] = self.ora_sid
            data['job_st'] = self.job_st
            data['tg_job_dtl_id'] = self.tg_job_dtl_id
            data['session_id'] = session_id
            data['write_bps'] = -1
            data['start_time'] = -1
            data['end_time'] = -1
            data['elapsed_seconds'] = -1
            data['status'] = -1
            data['input_type'] = -1
            data['session_recid'] = -1
            data['session_stamp'] = -1
            data['input_bytes'] = -1
            data['input_bytes_per_sec'] = -1
            data['output_bytes_per_sec'] = -1
            data['output_bytes'] = -1
        log.logdata('AGENT', 'INFO', '60008', '{} - [{}] JOB Monitor RMAN STATUS'.format(str(data['query_time']), data['status']))
        if data['status'] not in('COMPLETED', 'RUNNING'):
            s_msg = """DB NAME : {}
ORACLE SID : {}
JOB ID : {}
PID : {}
SESSION KEY : {}
SHELL FILE : {}
tg_job_dtl_id : {}""".format(self.db_name
                             ,self.ora_sid
                             ,self.job_id
                             ,str(self.pid)
                             ,str(self.session_key)
                             ,str(self.shell_name)
                             ,str(self.tg_job_dtl_id)
                    )
            log.logdata('AGENT',  'INFO', '60004', 'RMAN LOG FAIL.. Please Check DEBUG LOG FILE')
            log.logdata('AGENT', 'DEBUG', '60008', '-------------------------------------------------[%s] FAIL LOG DEBUG START-------------------------------------------------' %str(data['query_time']))
            log.logdata('AGENT', 'DEBUG', '60008', 'JOB INFO  : %s' %(s_msg))
            log.logdata('AGENT', 'DEBUG', '60008', 'PROCESS_CNT : %s' %str(self.ps_cnt()))
            log.logdata('AGENT', 'DEBUG', '60008', 'V_$RMAN_BACKUP_JOB_DETAILS STATUS CHECK :  %s' %str(ret))
            s_get_rman_error_output = self.get_rman_error_output(self.session_key)
            log.logdata('AGENT', 'DEBUG', '60008', 'RMAN OUTPUT QUERY RMAN Error :  %s' %str(s_get_rman_error_output))
            s_rman_stats = self.rman_stats(self.session_key)
            log.logdata('AGENT', 'DEBUG', '60008', 'RMAN STATUS QUERY DATA (V$RMAN_STATUS, V_$RMAN_BACKUP_JOB_DETAILS) : %s' %str(s_rman_stats))
            s_rman_full_log = self.get_rman_log(session_id, monitor=True)
            log.logdata('AGENT', 'DEBUG', '60008', 'RMAN LOG DATA : %s' %str(s_rman_full_log))
            log.logdata('AGENT', 'DEBUG', '60008', '--------------------------------------------------[%s] FAIL LOG DEBUG END--------------------------------------------------' %str(data['query_time']))
        return data

    def ps_cnt(self):
        time.sleep(3)
        if platform.system() == 'Linux':
            cmd = 'ps -ef | grep {PID} | grep {SHELL_NAME} | grep "{ORA_SID};" | grep -v grep | grep -v job_monitor | wc -l'.format(PID=self.pid, SHELL_NAME=self.shell_name,ORA_SID=self.ora_sid)
        else:
            cmd = 'ps -ef | grep {PID} | grep {SHELL_NAME} | grep "{ORA_SID}" | grep -v grep | grep -v job_monitor | wc -l'.format(PID=self.pid, SHELL_NAME=self.shell_name,ORA_SID=self.ora_sid)
        try :
            ret = os.popen(cmd).read().strip()
            if re.match('^[0-9]+$', ret) is not None:
                cnt = int(ret)
            else:
                cnt = -1
        except:
            cnt = -1
          
        return cnt

    def job_status(self, return_date):
        self.socket_server.job_status(return_date)

    def log_update(self, log_data):
        self.socket_server.log_update(log_data)

    def log_monitor(self, return_data):
        log_content = ''
        log_data = {}
        try:
            log_data = self.get_log()
            log_data['job_st'] = self.job_st
            log_data['status'] = return_data['status']
            log_data['elapsed_seconds'] = return_data['elapsed_seconds']

            self.socket_server.log_update(log_data)
        except Exception as e:
            print ("job_monitor() Exception : %s" %(str(e)))
        return log_data

    def get_ora_info(self):
        ora_info = {}
        oracle_home = ''
        ora_bit = False
        try:
            if self.cfg.get('common', 'ora_home_bit') == 'True':
                ora_bit = True
                oracle_home = self.o_common.get_ora_home(self.ora_sid)
        except:
            pass

        ora_info['ora_home_bit'] = ora_bit
        ora_info['ora_sid'] = self.ora_sid
        ora_info['ora_home'] = oracle_home
        ora_info['owner'] = self.owner
        self.ora_info = ora_info
        return ora_info
    '''
    #2023.11.06 @jhbae
    def get_backup_files(self, ora_info):
        ora_home_bit = ora_info['ora_home_bit']
        ora_sid = ora_info['ora_sid']
        ora_home = ora_info['ora_home']
        owner = ora_info['owner']

        query = """
        select 'FILE_NO:' as FILENO,NAME FROM V$DATAFILE;
        """
        s_sql_file = os.path.join(self.o_common.s_sql_path, 'backup_count.sql')
        with open(s_sql_file, 'w') as f:
            f.write(query)
        os.chmod(s_sql_file, 0644)

        if ora_home_bit:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, SQL_FILE=s_sql_file)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < {SQL_FILE}"
                        """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, SQL_FILE=s_sql_file)

        ret = os.popen(cmd).read()

        backup_file_list = []
        for line in ret.splitlines():
            if 'FILE_NO:' in line:
                backup_file_list.append(line.split(':')[-1].strip())
        try:
            os.remove(s_sql_file)
        except:
            pass
        return backup_file_list
    '''
    def get_rman_tag(self):
        ret = self.socket_server.get_rman_tag(self.job_id)
        return ret
    
    def get_rman_error_output(self, session_id):
        ora_home_bit = self.ora_info['ora_home_bit']
        owner = self.ora_info['owner']
        ora_sid = self.ora_info['ora_sid']
        sql = """
        SET PAGES 0
        SET LINESIZE 2048
        SET PAGESIZE 2000
        SET TIMING OFF
        SELECT to_char(rs.START_TIME,'yyyy-mm-dd hh24:mi:ss') start_time, ro.output 
        FROM V$RMAN_OUTPUT ro, v$rman_status rs 
        WHERE ro.SESSION_KEY='{}' 
        AND (ro.OUTPUT LIKE '%RMAN-%' OR ro.OUTPUT LIKE '%ORA-%')  
        AND ro.stamp = rs.stamp 
        ORDER BY ro.RECID;
        """.format(session_id)
        query_file = '{}_{}_{}_{}_rman_error_output.sql'.format(self.tg_job_dtl_id, self.db_name,self.job_id, self.shell_type)
        query_file = os.path.join(self.o_common.s_sql_path, query_file)
        with open(query_file, 'w') as fw:
            fw.write(sql)
        os.chmod(query_file, 0644)
        
        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORACLE_SID};sqlplus \$rman_user/\$rman_passwd@\$rman_SID as sysdba < {SQL_FILE}" """.format(OWNER=self.owner, ORACLE_SID=ora_sid, SQL_FILE=query_file)
        elif ora_home_bit:
            ora_home = self.ora_info['ora_home']
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};sqlplus -s '/as sysdba' < {QEURY_FILE}"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=query_file)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};sqlplus -s '/as sysdba' < {QEURY_FILE}"
                        """.format(OWNER=owner, ORACLE_SID=ora_sid, QEURY_FILE=query_file)
        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print('e')
        finally:
            os.remove(query_file)
        return ret
    
    def get_rman_log(self, session_id, monitor = False):
        ora_home_bit = self.ora_info['ora_home_bit']
        owner = self.ora_info['owner']
        ora_sid = self.ora_info['ora_sid']
        if monitor is False:
            sql = """
            SET heading off
            SET PAGES 0
            SET LINESIZE 2048
            SET PAGESIZE 2000
            SELECT OUTPUT FROM V$RMAN_OUTPUT WHERE SESSION_KEY='{}' AND OUTPUT != ' '  ORDER BY RECID;
            """.format(session_id)
        else:
            sql = """
            SET PAGES 0
            SET LINESIZE 2048
            SET PAGESIZE 2000
            SELECT to_char(rs.START_TIME,'yyyy-mm-dd hh24:mi:ss') start_time, ro.output
            FROM v$rman_output ro, v$rman_status rs
            WHERE ro.SESSION_KEY='{}' AND ro.stamp = rs.stamp AND ro.OUTPUT != ' '
            ORDER BY ro.recid;
            """.format(session_id)
            
        query_file = '{}_{}_{}_{}_rman_output.sql'.format(self.tg_job_dtl_id, self.db_name,self.job_id, self.shell_type)
        query_file = os.path.join(self.o_common.s_sql_path, query_file)
        with open(query_file, 'w') as fw:
            fw.write(sql)
        os.chmod(query_file, 0644)
        
        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORACLE_SID};sqlplus \$rman_user/\$rman_passwd@\$rman_SID as sysdba < {SQL_FILE}" """.format(OWNER=self.owner, ORACLE_SID=ora_sid, SQL_FILE=query_file)
        elif ora_home_bit:
            ora_home = self.ora_info['ora_home']
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};sqlplus -s '/as sysdba' < {QEURY_FILE}"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=query_file)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};sqlplus -s '/as sysdba' < {QEURY_FILE}"
                        """.format(OWNER=owner, ORACLE_SID=ora_sid, QEURY_FILE=query_file)
        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print('e')
        finally:
            os.remove(query_file)
        return ret

    def get_arch_prgress_count(self, rman_tag, sysdate_str, ora_info):
        """
        :param rman_tag:
        :param sysdate_str:
        :return:
        sysdate_str = "2021-04-01 10:15:59"
        """
        self.ora_home_bit = ora_info['ora_home_bit']
        ora_sid = ora_info['ora_sid']
        ora_home = ora_info['ora_home']
        owner = ora_info['owner']
        stime_date = datetime.datetime.strptime(sysdate_str, '%Y-%m-%d %H:%M:%S')
        stime = stime_date.strftime('%Y/%m/%d_%H:%M:%S')

        cnt_cmd = """list copy of archivelog from time="to_date('{}','yyyy-mm-dd hh24:mi:ss')";
        """.format(sysdate_str)
        s_rman_cnt_file = "{}_{}_{}_{}_rman_cnt.sql".format(self.tg_job_dtl_id, self.db_name, self.job_id, self.shell_type)
        s_rman_cnt_file = os.path.join(self.o_common.s_sql_path, s_rman_cnt_file)
        with open(s_rman_cnt_file, 'w') as f:
            f.write(cnt_cmd)
        os.chmod(s_rman_cnt_file, 0644)

        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORACLE_SID};rman target \$rman_user/\$rman_passwd@\$rman_SID < {QEURY_FILE}" """.format(ORACLE_SID=ora_sid, OWNER=owner, QEURY_FILE=s_rman_cnt_file)            
        elif self.ora_home_bit:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};rman target / nocatalog < {QEURY_FILE}"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=s_rman_cnt_file)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};rman target / nocatalog < {QEURY_FILE}"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=s_rman_cnt_file)
        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print(e)
        finally:
            os.remove(s_rman_cnt_file)


        backup_file_list = []
        for line in ret.splitlines():
            if 'Name:' in line:
                backup_file = line.split(':')[-1].strip()
                backup_file_list.append(backup_file)
        return backup_file_list

    def get_tot_count(self, ora_home_bit, ora_home, ora_sid, owner):
        query =  """set head off
        select 'TOT_CNT,',count(name) from v$datafile where name not like '%/pdb%';"""

        tot_cnt_file = "{}_{}_{}_{}_tot_cnt.sql".format(self.tg_job_dtl_id, self.db_name, self.job_id, self.shell_type)
        tot_cnt_file = os.path.join(self.o_common.s_sql_path, tot_cnt_file)
        with open(tot_cnt_file, 'w') as fw:
            fw.write(query)
        os.chmod(tot_cnt_file, 0644)

        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORACLE_SID};sqlplus \$rman_user/\$rman_passwd@\$rman_SID as sysdba < {SQL_FILE}" """.format(OWNER=self.owner, ORACLE_SID=ora_sid, SQL_FILE=tot_cnt_file)
        elif ora_home_bit:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < {QEURY_FILE}"
                """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=tot_cnt_file)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < {QEURY_FILE}"
                            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=tot_cnt_file)

        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print('e')
        finally:
            os.remove(tot_cnt_file)

        tot_cnt = 0

        for line in ret.splitlines():
            if 'TOT_CNT,' in line:
                tot_cnt = line.split(',')[-1].strip()
        return int(tot_cnt)

    def get_prgress_count(self, rman_tag, sysdate_str, ora_info):

        ora_home_bit = ora_info['ora_home_bit']
        ora_sid = ora_info['ora_sid']
        ora_home = ora_info['ora_home']
        owner = ora_info['owner']
        stime_date = datetime.datetime.strptime(sysdate_str, '%Y-%m-%d %H:%M:%S')
        stime = stime_date.strftime('%Y/%m/%d_%H:%M:%S')
        cnt_cmd = """LIST COPY TAG {} completed after "to_date('{}','yyyy-mm-dd hh24:mi:ss')";
        """.format(rman_tag, sysdate_str)
        """
        "ARCH", "INCR_L0", "INCR_L1", "FULL_L0", "MRG", "INCR_MRG"
        """
        if self.shell_type in ("INCR_L0", "INCR_L1", "FULL_L0", "MRG", "INCR_MRG"):
            try:
                tot_cnt = self.get_tot_count(ora_home_bit, ora_home, ora_sid, owner)
            except Exception as e:
                print ("get_prgress_count() Exception : %s" %(str(e)))
                tot_cnt = 0
        else:
            tot_cnt = 0
        if self.shell_type in ("INCR_L0", "FULL_L0", "MRG"):
            cnt_cmd = """LIST COPY TAG {} completed after "to_date('{}','yyyy-mm-dd hh24:mi:ss')";
        """.format(rman_tag, sysdate_str)
        elif self.shell_type in ("INCR_L1", "INCR_MRG"):
            cnt_cmd = """LIST BACKUP TAG {} completed after "to_date('{}','yyyy-mm-dd hh24:mi:ss')";
        """.format(rman_tag, sysdate_str)
        s_rman_cnt_file = "{}_{}_{}_{}_rman_cnt.sql".format(self.tg_job_dtl_id, self.db_name, self.job_id, self.shell_type)
        s_rman_cnt_file = os.path.join(self.o_common.s_sql_path, s_rman_cnt_file)
        with open(s_rman_cnt_file, 'w') as f:
            f.write(cnt_cmd)
        os.chmod(s_rman_cnt_file, 0644)

        try :
            b_auth = self.o_common.get_cfg().get('common', 'ora_auth')
        except:
            b_auth = 'F'
        if b_auth == 'T':
            cmd = """/bin/su - {OWNER} -c ". .ibrm_profile;export ORACLE_SID={ORACLE_SID};rman target \$rman_user/\$rman_passwd@\$rman_SID < {QEURY_FILE}" """.format(OWNER=owner, ORACLE_SID=ora_sid, QEURY_FILE=s_rman_cnt_file)
        elif ora_home_bit:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};rman target / nocatalog < {QEURY_FILE}"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=s_rman_cnt_file)
        else:
            cmd = """/bin/su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};rman target / nocatalog < {QEURY_FILE}"
                        """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=s_rman_cnt_file)

        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print('e')
        finally:
            os.remove(s_rman_cnt_file)

        backup_file_list = []
        prg_cnt = 0
        backup_files = []
        for line in ret.splitlines():

            if 'Name:' in line:
                if self.shell_type in ("INCR_L0", "FULL_L0", "MRG"):
                    prg_cnt = prg_cnt + 1
                elif self.shell_type in ("INCR_L1", "INCR_MRG"):
                    b_file = line.split(':')[-1].strip()
                    b_file = os.path.basename(b_file)
                    if not b_file in backup_files:
                        prg_cnt = prg_cnt + 1
                        backup_files.append(b_file)
                if prg_cnt > tot_cnt:
                    prg_cnt = tot_cnt
        try:
            progress = (float(prg_cnt) / float(tot_cnt)) * 100
            progress = round(progress, 2)
            if progress > 100:
                progress = 100
        except:
            progress = 0
        return prg_cnt, tot_cnt, progress

    def main(self):
        self.set_job_monitor()

        log = log_control.LogControl()
        cnt = 1
        #20220915 sdhyun
        #pedwdb71 rman_tag 이 비정상적인 케이스 발견 및 exception
        try:
            rman_tag = self.get_rman_tag()
        except Exception as e:
            print ("job_monitor main() Exception1 : %s" %(str(e)))
            rman_tag = ''

        ora_info = self.get_ora_info()
        log_data = ''
        return_data = {}
        retry_cnt = 0
        while True:
            return_data = self.job_monitor(session_id=self.session_key)
            self.log_monitor(return_data)

            prg_cnt, tot_cnt, progress = '0', '0', '0'
            if self.shell_type.strip() in ('INCR_L0', 'INCR_L1', 'FULL_L0', 'INCR_MRG', 'MRG'):
                """
                "ARCH" "Archive" 1 "아카이브백업"
                "INCR_L0" "INCR_LEV0" 2 "Incremental Level 0 백업"
                "INCR_L1" "INCR_LEV1" 3 "Incremental Level 1 백업"
                "FULL_L0" "FULL_LEV0" 4 "Full Level 0 백업"
                """
                try:
                    prg_cnt, tot_cnt, progress = self.get_prgress_count(rman_tag, self.sysdate_str, ora_info)
                except Exception as e:
                    print ("job_monitor main() Exception2 : %s" %(str(e)))
                    prg_cnt, tot_cnt, progress = '0', '0', '0'

            elif self.shell_type.strip() == 'ARCH':
                #20220914 sdhyun
                # pedwdb71 서버에서 rman 사용시 oracle audit 로그 발생
                # 로그파일이 커서 rman -> log count 방식으로 전환.
                ret = re.findall(r"validation succeeded for archived log", self.log_content)
                prg_cnt = len(ret)
                tot_cnt = '-1'
                progress = prg_cnt

            try:
                return_data['prg_cnt'] = str(prg_cnt)
                return_data['tot_cnt'] = str(tot_cnt)
                return_data['progress'] = str(progress)
            except Exception as e:
                print ("job_monitor main() Exception3 : %s" %(str(e)))
                return_data['prg_cnt'] = "0"
                return_data['tot_cnt'] = "0"
                return_data['progress'] = "0"


            if return_data['status'] == 'RUNNING':
                self.job_st = 'Running'
                return_data['job_st'] = self.job_st
                log.logdata('AGENT', 'INFO', '80000', 'JOB STATUS : {}'.format(str(return_data)))
                self.socket_server.job_status(return_data)
            else:
                ps_cnt = self.ps_cnt()
                if int(ps_cnt) == 0:
                    if return_data['status'] == 'COMPLETED':
                        self.job_st = 'End-OK'
                        return_data['job_st'] = self.job_st
                        log.logdata('AGENT', 'INFO', '60003', str(return_data))
                        self.socket_server.job_status(return_data)
                        break
                    else:
                        retry_cnt = retry_cnt  + 1
                        if retry_cnt > 5:
                            self.job_st = 'Fail'
                            return_data['job_st'] = self.job_st
                            log.logdata('AGENT', 'INFO', '60004', str(return_data))
                            self.socket_server.job_status(return_data)
                            break
                        else:
                            log.logdata('AGENT', 'INFO', '60009', 'JOB STATUS : %s Retry Check count %s'  %(return_data['job_st'],str(retry_cnt)))

            if self.shell_type in ('INCR_L0', 'INCR_L1', 'FULL_L0'):
                time.sleep(60)
            else:
                time.sleep(10)

if __name__ == '__main__':
    arg = sys.argv

    ora_sid = arg[1]
    pid = arg[2]
    session_id = arg[3]
    job_id = arg[4]
    shell_name = arg[5]
    shell_type = arg[6]
    db_name = arg[7]
    sysdate = arg[8]
    tg_job_dtl_id = arg[9]
    log.logdata('AGENT', 'INFO', '60001', str(tg_job_dtl_id))
    job_mon(ora_sid, pid, session_id, job_id, shell_name, shell_type, db_name, sysdate, tg_job_dtl_id).main()