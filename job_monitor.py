# -*- encoding:utf-8*-
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
import os
import ConfigParser
import common
import sys
import time
import datetime
import ibrm_server_daemon_send
import socketClient
import re


class job_mon():
    def __init__(self,ora_sid,pid,session_key,job_id,shell_name,shell_type,db_name,sysdate_str,tg_job_dtl_id):
        self.cfg = self.get_cfg()
        self.session_key = session_key.strip()
        self.pid = pid.strip()
        self.ora_sid = ora_sid.strip()
        self.job_id = job_id
        self.shell_name = shell_name.strip()
        self.shell_type = shell_type.strip()
        self.db_name = db_name.strip()
        self.sysdate_str = sysdate_str
        self.tg_job_dtl_id = tg_job_dtl_id
        self.com = common.Common()
        self.socket_server = self.get_server()
        self.job_st= 'Running'
        self.log_date = datetime.datetime.strptime(self.sysdate_str.strip(), '%Y-%m-%d %H:%M:%S').strftime('%Y_%m%d_%H')
        self.owner = self.get_owner()
        print '-' * 50
        print 'PID         :', self.pid
        print 'session_key :', self.session_key
        print 'ora_sid     :', self.ora_sid
        print 'owner       :', self.owner
        print '-' * 50
        print 'MONITOR STARTING ....'

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

    def get_log(self):

        self.shell_name
        self.shell_type
        self.db_name
        # now = datetime.datetime.now()
        # yyyy = now.strftime('%Y')
        # mmdd = now.strftime('%m%d')
        log_date = datetime.datetime.strptime(self.sysdate_str.strip(), '%Y-%m-%d %H:%M:%S')
        yyyy = log_date.strftime('%Y')
        mmdd = log_date.strftime('%m%d')
        log_path = self.cfg.get('common', 'log_path')
        log_path = log_path.replace('{DB_NAME}', self.db_name)
        log_path = log_path.replace('{YYYY}', yyyy)
        log_path = log_path.replace('{MMDD}', mmdd)
        date_string = log_date.strftime('%Y_%m%d_%H')
        """
        UPGR_RMAN_Level0_20201015_17.log
        IBRM_RMAN_Level0_2020_1025_10.log
        UPGR_RMAN_Level0_20201026_13.log
        export RMAN_L0_LOG=${BACKUPDB_LOG}/${DB_DIR}_RMAN_Level0_${YMDH}.log
        export RMAN_L1_LOG=${BACKUPDB_LOG}/${DB_DIR}_RMAN_Level1_${YMDH}.log
        export Arch_LOG=${BACKUPDB_LOG}/${DB_DIR}_Archive_log_${YMDH}.log
        2020_1028_10
        IBRM_Archive_log_2020_1028_16.log
        """

        log_base = os.path.splitext(self.shell_name)[0]
        """
        
        "ARCH" "Archive" 1 "아카이브백업"
        "INCR_L0" "INCR_LEV0" 2 "Incremental Level 0 백업"
        "INCR_L1" "INCR_LEV1" 3 "Incremental Level 1 백업"
        "FULL_L0" "FULL_LEV0" 4 "Full Level 0 백업"
        "MRG" "MERGE" 5 "MERGE"
        "INCR_MRG" "INCR+MERGE" 6 "Incremental + Merge"
        "DSC" "DATA SNAP Create" 7 "Data Snapshot Create"
        "DSD" "DATA SNAP DEL" 8 "Data Snapshot Delete"
        "ASC" "ARCH SNAP CREATE" 9 "Archieve Snapshot Create"
        "ASD" "ARCH SNAP DEL" 10 "Archieve Snapshot Delete"

        """

        # if 'level1' in log_base.lower():
        if self.shell_type in ('INCR_L1', 'INCR_MRG'):
            mid = 'Level1'
            log_file = '{DB_NAME}_RMAN_Level1_{DATE_STRING}'.format(DB_NAME=self.db_name, DATE_STRING=self.log_date)
        # elif 'level0' in log_base.lower():
        elif self.shell_type == 'INCR_L0':
            mid = 'Level0'
            log_file = '{DB_NAME}_RMAN_Level0_{DATE_STRING}'.format(DB_NAME=self.db_name, DATE_STRING=self.log_date)
        # elif 'archive' in log_base.lower():
        elif self.shell_type == 'FULL_L0':
            mid = 'Full'
            log_file = '{DB_NAME}_RMAN_Full_{DATE_STRING}'.format(DB_NAME=self.db_name, DATE_STRING=self.log_date)
        elif self.shell_type == 'ARCH':
            mid = 'Archive'
            log_file = '{DB_NAME}_Archive_log_{DATE_STRING}'.format(DB_NAME=self.db_name, DATE_STRING=self.log_date)
        elif self.shell_type == 'MRG':
            mid = 'MERGE'
            log_file = '{DB_NAME}_RMAN_Merge_{DATE_STRING}'.format(DB_NAME=self.db_name, DATE_STRING=self.log_date)
        else:
            log_file = ''
            # log_file = '{DB_NAME}_Archive_log_{DATE_STRING}'.format(DB_NAME=self.db_name, DATE_STRING=log_date)


        #/ZFS/LOGS/ORCL/ORCL_2021_1020/ORCL_RMAN_Merge_2021_1020_13.log
        #/ZFS/LOGS/ORCL/ORCL_2021_1020/ORCL_Merge_2021_1020_13.log
        # log_date = datetime.strptime(self.sysdate_str.strip(), '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d_%H')
        # log_name = '{LOGBASE}_{LOG_DATE}.log'.format(LOGBASE=log_base, LOG_DATE=log_date)
        # log_name = log_name.replace('Incr', 'RMAN')
        # log_file = os.path.join(log_path, log_name)
        # self.log.info(self.shell_type)

        target_log = os.path.join(log_path, '{LOG_FILE}.log'.format(LOG_FILE=log_file))
        print 'target_log : ', target_log, os.path.isfile(target_log)

        # cmd='ls -alt %s'%target_log
        # print os.popen(cmd).read()
        # if not os.path.isfile(target_log):
        #     tlog  = os.path.join(log_path,'{DB_NAME}_*'.format(DB_NAME=self.db_name))
        #     cmd=  'ls -t {} '.format(tlog)
        #     print cmd
        #     ret = os.popen(cmd).read().split()[0]
        #     log_file = ret.split()[-1]
        # else:
        #     log_file = target_log
        # print 'log_file :',log_file
        # print 'log exist:',os.path.isfile(log_file)
        # if not os.path.isfile(log_file):
        #     cmd = 'ls -t {}/*{}*.log'.format(log_path,mid)
        #     print 'cmd : ',cmd
        #     log_file=os.popen(cmd).read().split()[0]
        log_name = os.path.basename(target_log)
        if os.path.isfile(target_log):
            with open(target_log) as f:
                log_data = f.read()
            log_data_line = log_data.splitlines()
        else:
            print 'LOG FILE NOT FOUND'
            log_data = self.get_rman_log(self.session_key)
            log_data_line = log_data.splitlines()

        try:
            self.log_content = self.log_resize(log_data_line)
        except Exception as e:
            print str(e)

        new_log_name = '{}_{}'.format(self.job_id, log_name)
        new_log_file = os.path.join('data', new_log_name)
        with open(new_log_file, 'w') as fw:
            fw.write(self.log_content)

        print socketClient.SocketSender(FILENAME=new_log_file, DIR='ibrm_backup_log', ENDCHECK='NO').main()
        log_return_data = {}
        log_return_data['log_file'] = target_log
        log_return_data['log_contents'] = new_log_file
        log_return_data['pid'] = self.pid
        log_return_data['job_id'] = self.job_id
        log_return_data['tg_job_dtl_id'] = self.tg_job_dtl_id
        # os.remove(new_log_file)
        return log_return_data
        # else:
        #     log_data = self.get_rman_log(self.session_key)
        #     log_data_lines=log_data.splitlines()
        #
        #     log.logdata('AGENT', 'ERROR', '60100', str(log_file))
        #     # return {}
        #     log_return_data = {}
        #     log_return_data['log_file'] = log_file
        #     log_return_data['log_contents'] = 'LOG FILE NOT FOUND ({})'.format(log_file)
        #     log_return_data['pid'] = self.pid
        #     log_return_data['job_id'] = self.job_id
        #     log_return_data['tg_job_dtl_id'] = self.tg_job_dtl_id
        #     return log_return_data

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

    def job_monitor(self, **kwargs):
        # sysdate = kwargs['sysdate']
        session_id = kwargs['session_id']
        # self.make_sql(sysdate=sysdate,session_id=session_id)
        sql = """col OUTPUT_BYTES_PER_SEC_DISPLAY for a20
col START_TIME for a21
col END_TIME for a21
col INPUT_BYTES for a20
col INPUT_BYTES_PER_SEC for a20
col OUTPUT_BYTES_PER_SEC for a20
col OUTPUT_BYTES for a20
SET PAGES 0 
SET HEADING OFF
SET LINESIZE 2048
SET PAGESIZE 2000
SET TIMING OFF
SET COLSEP ,
SELECT 
    'JOB_MONITOR',
    OUTPUT_BYTES_PER_SEC_DISPLAY,
    TO_CHAR(START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,
    TO_CHAR(END_TIME,'yyyy-mm-dd hh24:mi:ss') END_TIME,
    NVL(ELAPSED_SECONDS, 0) AS ELAPSED_SECONDS,
    STATUS,
    INPUT_TYPE,
    SESSION_RECID,
    SESSION_STAMP,
    TO_CHAR(INPUT_BYTES) AS INPUT_BYTES,
    TO_CHAR(INPUT_BYTES_PER_SEC) AS INPUT_BYTES_PER_SEC,
    TO_CHAR(OUTPUT_BYTES_PER_SEC) AS OUTPUT_BYTES_PER_SEC,
    TO_CHAR(OUTPUT_BYTES) AS OUTPUT_BYTES
FROM V_$RMAN_BACKUP_JOB_DETAILS WHERE SESSION_KEY='{}'; """.format(session_id)

        sql_file = '/tmp/{}_session_monitor.sql'.format(self.ora_sid)
        with open(sql_file, 'w') as f:
            f.write(sql)
        os.popen('chmod 777 {}'.format(sql_file))
        # with open(sql_file) as f:
        #     print f.read()
        cmd = """su - oracle -c "export ORACLE_SID=catdb;sqlplus -s 'rmancat/welcome1@catdb '< %s" 
        """ % sql_file
        if self.cfg.get('common', 'ora_home_bit') == 'True':
            oracle_home = self.com.get_ora_home(self.ora_sid)
            cmd = """su - {OWNER} -c "ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORACLE_HOME=oracle_home, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORA_SID};sqlplus -s '/as sysdba' < {SQL_FILE}"
            """.format(OWNER=self.owner, ORA_SID=self.ora_sid, SQL_FILE=sql_file)
        print cmd
        try:
            ret = os.popen(cmd).read()
        except Exception as e:
            print str(e)


        """
        ret :
           41.14M 2020-11-09 18:14:21 2020-11-09 18:52:52 2311 COMPLETED DB INCR 2247 1056046460 99691200512
        """
        data = {}

        # print ret.split(',')
        for line in ret.splitlines():
            if 'JOB_MONITOR' in ret:
                line_set = ret.split(',')

                data['session_id'] = session_id
                data['write_bps'] = line_set[1].strip()
                try:
                    data['start_time'] = line_set[2].strip()
                    data['end_time'] = line_set[3].strip()
                except:
                    data['start_time'] = ''
                    data['end_time'] = ''
                data['elapsed_seconds'] = line_set[4].strip()
                data['status'] = line_set[5].strip()
                data['input_type'] = line_set[6].strip()
                data['session_recid'] = line_set[7].strip()
                data['session_stamp'] = line_set[8].strip()
                data['input_bytes'] = line_set[9].split()[0].strip()
                # print line_set[9]
                # print line_set[9].strip()
                # print line_set[9].split()[0].strip()

                try:
                    data['input_bytes_per_sec'] = int(float(line_set[10].strip()))
                    data['output_bytes_per_sec'] = int(float(line_set[11].strip()))
                    data['output_bytes'] = line_set[12].strip()
                except:
                    # version 하위 호환성
                    data['input_bytes_per_sec'] = -1
                    data['output_bytes_per_sec'] = -1
                    data['output_bytes'] = -1

                data['pid'] = self.pid
                data['job_id'] = self.job_id
                data['ora_sid'] = self.ora_sid
                data['job_st'] = self.job_st
                data['tg_job_dtl_id'] = self.tg_job_dtl_id
                return data
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
            return data

    def ps_mon(self):
        cmd = 'ps -ef | grep {PID} | grep -v grep '.format(PID=self.pid)
        ret = os.popen(cmd).read()
        return ret

    def ps_cnt(self):
        cmd = 'ps -ef | grep {PID} | grep -v grep | grep -v job_monitor'.format(PID=self.pid)
        # print cmd
        ret = os.popen(cmd).read()
        cnt = 0
        for line in ret.splitlines():
            pid = line.split()[1]
            gid = line.split()[2]
            if self.pid == pid or self.pid == gid:
                cnt = cnt + 1
        return int(cnt)

    def job_status(self, return_date):
        self.socket_server.job_status(return_date)

    def log_update(self, log_data):
        print 'log_update :', log_data
        self.socket_server.log_update(log_data)

    def job_update(self, return_data):
        self.socket_server.job_update(return_data)

    def log_monitor(self, return_data):
        log_content = ''
        log_data = {}
        try:
            log_data = self.get_log()
            log_data['job_st'] = self.job_st
            log_data['status'] = return_data['status']
            log_data['elapsed_seconds'] = return_data['elapsed_seconds']
            # log.info(log_data)
            # log.logdata('AGENT', 'INFO', '60100', str(log_file))
            # print 'log_data'
            self.socket_server.log_update(log_data)
        except Exception as e:
            print str(e)
        return log_data

    def get_ora_info(self):
        ora_info = {}
        try:
            if self.cfg.get('common', 'ora_home_bit') == 'True':
                ora_bit = True
            else:
                ora_bit = False
        except:
            ora_bit = False
        try:
            oracle_home = self.com.get_ora_home(self.ora_sid)
        except:
            oracle_home = ''
        ora_info['ora_home_bit'] = ora_bit
        ora_info['ora_sid'] = self.ora_sid
        ora_info['ora_home'] = oracle_home
        ora_info['owner'] = self.owner
        self.ora_info = ora_info
        return ora_info

    def get_backup_files(self, ora_info):
        ora_home_bit = ora_info['ora_home_bit']
        ora_sid = ora_info['ora_sid']
        ora_home = ora_info['ora_home']
        owner = ora_info['owner']
        """
        /u01/app/oracle/product/11.2.0.4
        :return:
        """
        query = """
        select 'FILE_NO:' as FILENO,NAME FROM V$DATAFILE;
        """
        with open('/tmp/backup_count.sql', 'w') as f:
            f.write(query)
        if ora_home_bit:
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < /tmp/backup_count.sql"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < /tmp/backup_count.sql"
                        """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid)
        ret = os.popen(cmd).read()
        backup_file_list = []
        for line in ret.splitlines():
            if 'FILE_NO:' in line:
                backup_file_list.append(line.split(':')[-1].strip())
        try:
            os.remove('/tmp/backup_count.sql')
        except:
            pass
        return backup_file_list

    def get_rman_tag(self):
        ret = self.socket_server.get_rman_tag(self.job_id)
        return ret

    def get_rman_log(self, session_id):
        ora_home_bit = self.ora_info['ora_home_bit']
        owner = self.ora_info['owner']
        ora_sid = self.ora_info['ora_sid']
        sql = """
        set heading off;
        SELECT OUTPUT FROM V$RMAN_OUTPUT WHERE SESSION_KEY='{}' ORDER BY RECID;
        """.format(session_id)
        query_file = '/tmp/rman_output.sql'
        with open(query_file, 'w') as fw:
            fw.write(sql)
        os.chmod(query_file, 0644)
        if ora_home_bit:
            ora_home = self.ora_info['ora_home']
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < {QEURY_FILE}"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=query_file)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < {QEURY_FILE}"
                        """.format(OWNER=owner, ORACLE_SID=ora_sid, QEURY_FILE=query_file)
        print '-' * 40
        print cmd
        print '-' * 40
        ret = os.popen(cmd).read()
        return ret

    def get_arch_prgress_count(self, sysdate_str, ora_info):
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
        # cnt_cmd = """LIST COPY TAG {} completed after "to_date('{}','yyyy-mm-dd hh24:mi:ss')";
        # """.format(rman_tag, sysdate_str)
        cnt_cmd = """list copy of archivelog from time="to_date('{}','yyyy-mm-dd hh24:mi:ss')";
        """.format(sysdate_str)

        with open('/tmp/rman_cnt.sql', 'w') as f:
            f.write(cnt_cmd)
        with open('/tmp/rman_cnt.sql') as f:
            print f.read()
        if self.ora_home_bit:
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};rman target / nocatalog < /tmp/rman_cnt.sql"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};rman target / nocatalog < /tmp/rman_cnt.sql"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid)
        print cmd
        ret = os.popen(cmd).read()
        print ret
        backup_file_list = []
        for line in ret.splitlines():
            if 'Name:' in line:
                backup_file = line.split(':')[-1].strip()
                backup_file_list.append(backup_file)
        return backup_file_list

    def get_tot_count(self, ora_home_bit, ora_home, ora_sid, owner):

        query = """
    set head off
    select 'TOT_CNT,',count(name) from v$datafile;
            """
        tot_cnt_file = '/tmp/tot_cnt.sql'
        with open(tot_cnt_file, 'w') as fw:
            fw.write(query)
        os.popen('chmod 777 %s' % tot_cnt_file).read()
        if ora_home_bit:
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < {QEURY_FILE}"
                """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=tot_cnt_file)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};sqlplus '/as sysdba' < {QEURY_FILE}"
                            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid, QEURY_FILE=tot_cnt_file)
        print '-' * 40
        print cmd
        print '-' * 40
        ret = os.popen(cmd).read()
        tot_cnt = 0
        # print '-' * 40
        # print ret
        # print '-' * 40

        print '-' * 40
        for line in ret.splitlines():
            if 'TOT_CNT,' in line:
                tot_cnt = line.split(',')[-1].strip()
        print 'tot_cnt :', tot_cnt
        return int(tot_cnt)

    def get_prgress_count(self, rman_tag, sysdate_str, ora_info):
        """
        :param rman_tag:
        :param sysdate_str:
        :return:
        sysdate_str = "2021-04-01 10:15:59"
        """
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
                print str(e)
                tot_cnt = 0
        else:
            tot_cnt = 0
        if self.shell_type in ("INCR_L0", "FULL_L0", "MRG"):
            cnt_cmd = """LIST COPY TAG {} completed after "to_date('{}','yyyy-mm-dd hh24:mi:ss')";
        """.format(rman_tag, sysdate_str)
        elif self.shell_type in ("INCR_L1", "INCR_MRG"):
            cnt_cmd = """LIST BACKUP TAG {} completed after "to_date('{}','yyyy-mm-dd hh24:mi:ss')";
        """.format(rman_tag, sysdate_str)

        with open('/tmp/rman_cnt.sql', 'w') as f:
            f.write(cnt_cmd)
        # with open('/tmp/rman_cnt.sql') as f:
        #     print f.read()
        if ora_home_bit:
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={ORACLE_SID};rman target / nocatalog < /tmp/rman_cnt.sql"
            """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={ORACLE_SID};rman target / nocatalog < /tmp/rman_cnt.sql"
                        """.format(OWNER=owner, ORACLE_HOME=ora_home, ORACLE_SID=ora_sid)
        print cmd
        ret = os.popen(cmd).read()
        # print ret
        backup_file_list = []
        prg_cnt = 0
        backup_files = []
        for line in ret.splitlines():

            if 'Name:' in line:
                print line
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
        # print backup_files
        print len(backup_files)

        try:
            progress = (float(prg_cnt) / float(tot_cnt)) * 100
            progress = round(progress, 2)
            if progress > 100:
                progress = 100
        except:
            progress = 0
        print "return prg_cnt,tot_cnt,progress :", prg_cnt, tot_cnt, progress
        return prg_cnt, tot_cnt, progress
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
            cmd = 'ps -ef | grep {} | grep -v grep | grep -v job_monitor'.format(self.pid)
            print cmd
            print os.popen(cmd).read()
            print '-'*50

        except Exception as e:
            print str(e)
    def main(self):

        cnt = 1
        try:
            rman_tag = self.get_rman_tag()
        except Exception as e:
            print str(e)
            rman_tag = ''
        print 'rman_tag :', rman_tag
        ora_info = self.get_ora_info()


        while True:
            self.process_monitor()
            return_data = self.job_monitor(session_id=self.session_key)
            self.log_monitor(return_data)
            prg_cnt, tot_cnt, progress = '0', '0', '0'
            if self.shell_type in ('INCR_L0', 'INCR_L1', 'FULL_L0', 'INCR_MRG', 'MGR'):
                """
                "ARCH" "Archive" 1 "아카이브백업"
                "INCR_L0" "INCR_LEV0" 2 "Incremental Level 0 백업"
                "INCR_L1" "INCR_LEV1" 3 "Incremental Level 1 백업"
                "FULL_L0" "FULL_LEV0" 4 "Full Level 0 백업"
                """
                try:
                    prg_cnt, tot_cnt, progress = self.get_prgress_count(rman_tag, self.sysdate_str, ora_info)
                except Exception as e:
                    print str(e)
                    prg_cnt, tot_cnt, progress = '0', '0', '0'

            elif self.shell_type == 'ARCH':
                backup_files = self.get_arch_prgress_count(self.sysdate_str,ora_info)
                #ret = re.findall(r"validation succeeded for archived log", self.log_content)
                prg_cnt = len(backup_files)
                tot_cnt = '-1'
                progress = prg_cnt
            else:
                prg_cnt = '0'
                tot_cnt = '0'
                progress = '0'
            print '-' * 50
            print 'prg_cnt, tot_cnt, progress', prg_cnt, tot_cnt, progress, type(prg_cnt)
            print '-' * 50

            # print return_data, type(return_data)
            try:
                return_data['prg_cnt'] = str(prg_cnt)
                return_data['tot_cnt'] = str(tot_cnt)
                return_data['progress'] = str(progress)
            except:
                return_data['prg_cnt'] = "0"
                return_data['tot_cnt'] = "0"
                return_data['progress'] = "0"
            print '-' * 50

            # self.socket_server.job_status(return_data)
            print 'return data :', return_data
            ps_cnt = self.ps_cnt()
            ps_ret = self.ps_mon()

            # print 'PROCESS COUNT :', ps_cnt
            # print 'PROCESS : ', ps_ret
            # print 'PS_CNT :', ps_cnt, return_data['status']
            if ps_cnt == 0:
                if return_data['status'] == 'COMPLETED':
                    self.job_st = 'End-OK'
                    return_data = self.job_monitor(session_id=self.session_key)
                    return_data['job_st'] = self.job_st
                    return_data['prg_cnt'] = prg_cnt
                    return_data['tot_cnt'] = tot_cnt
                    return_data['progress'] = progress
                    #log.logdata('AGENT', 'INFO', '60003', str(return_data))
                    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    print return_data
                    self.socket_server.job_status(return_data)
                else:
                    self.job_st = 'Fail'
                    return_data = self.job_monitor(session_id=self.session_key)
                    return_data['job_st'] = self.job_st
                    return_data['prg_cnt'] = prg_cnt
                    return_data['tot_cnt'] = tot_cnt
                    return_data['progress'] = progress
                    #log.logdata('AGENT', 'INFO', '60004', str(return_data))
                    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    print return_data
                    self.socket_server.job_status(return_data)
                break
            else:
                self.job_st = 'Running'
                return_data['job_st'] = self.job_st
                self.socket_server.job_status(return_data)

            print 'monitor count :', cnt
            cnt = cnt + 1
            if '[sh] <defunct>' in ps_ret:
                print ps_ret
                #log.logdata('AGENT', 'INFO', '60101', str(ps_ret))
                # log.info(ps_ret)
                # os.popen("ps -ef | grep defunct | awk '{print $3}' | xargs kill -9").read()
                # sys.exit()

            if self.shell_type in ('INCR_L0', 'INCR_L1', 'FULL_L0'):
                time.sleep(60)
            else:
                time.sleep(10)
        print '#'*50
        print '# JOB COMPLETE'
        print '# DB NAME     : {}'.format(self.db_name)
        print '# ORA SID     : {}'.format(self.ora_sid)
        print '# JOB ID      : {}'.format(self.job_id)
        print '# JOB DTL ID  : {}'.format(self.tg_job_dtl_id)
        print '# SHELL NAME  : {}'.format(self.shell_name)
        print '# BACKUP TYPE : {}'.format(self.shell_type)
        print '# SESSION KEY : {}'.format(self.session_key)
        print '# PID         : {}'.format(self.pid)
        print '# JOB STAT    : {}'.format(self.job_st)
        print '# DATE TIME   : {}'.format(datetime.datetime.now().strftime('%H-%m-%d %H:%M:%S'))
        print '#' * 50
        self.process_monitor()
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
    #log.logdata('AGENT', 'INFO', '60001', str(tg_job_dtl_id))
    job_mon(ora_sid, pid, session_id, job_id, shell_name, shell_type, db_name, sysdate, tg_job_dtl_id).main()