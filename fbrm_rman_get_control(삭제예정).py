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
import datetime
import glob
import os
import sys

import common
import fbrm_rman_progress
import socketClient
import get_database


class fbrm_ora():

    def __init__(self):
        self.cfg = self.get_cfg()
        self.hostname = os.popen('hostname').read().strip()
        self.check_config()
        self.com = common.Common()
        self.now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.file_name = self.get_filename()

    def get_filename(self):

        fname = 'rman_%s_%s.tmp' % (self.hostname, self.now)
        return os.path.join('data', fname)

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config', 'config.cfg')
        cfg.read(cfg_file)
        return cfg

    def check_config(self):
        uid = os.getuid()
        print uid, uid == 0
        if not uid == 0:
            print 'root only'
            print 'program exit'
            sys.exit()
        fbrm_dir = '/tmp/fbrm'
        if not os.path.isdir(fbrm_dir):
            os.mkdir(fbrm_dir)
        data_path = os.path.join('/tmp', 'fbrm', 'data')
        if not os.path.isdir(data_path):
            os.mkdir(data_path)
        sql_path = os.path.join('/tmp', 'fbrm', 'sql')
        print sql_path, os.path.isdir(sql_path)
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
        rman_data = '/tmp/fbrm/rman_data'
        if not os.path.isdir(rman_data):
            os.makedirs(rman_data)

    def rman(self,db_info):
        """
        [rmancat]
        user = rmancat
        passwd = welcome1
        dbname = catdb
        """
        target_dir = os.path.join('/tmp', 'fbrm', 'rman_sql')

        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)

        src_file = os.path.join('./','sql', 'rman', 'fbrm_rman_control.sql')
        tar_file = os.path.join(target_dir, 'fbrm_rman_control.sql')

        # shutil.copy(src_file,tar_file)
        with open(src_file) as f:
            lineset = f.read()
        print lineset
        print '{HOSTNAME}' in lineset
        n_lineset = lineset.replace('{HOSTNAME}', self.hostname)
        if '{DB_NAME}' in n_lineset:
            n_lineset = n_lineset.replace('{DB_NAME}',db_info['db_name'])
        print n_lineset
        with open(tar_file, 'w') as fw:
            fw.write(n_lineset)

        print os.popen('chmod -R 777 /tmp/fbrm').read()
        print os.popen('chmod -R 777 /tmp/fbrm/rman_sql').read()
        print os.popen('chmod -R 777 {SQL_FILE}'.format(SQL_FILE=tar_file)).read()




        if db_info['ora_home_bit'] == 'True':
            cmd = """su - {OWNER} -c "export ORACLE_HOME={ORACLE_HOME};export ORACLE_SID={SID};sqlplus -s '/as sysdba'< {TARGET_FILE}" """.format(ORACLE_HOME=db_info['oracle_home'],SID=db_info['sid'],OWNER=db_info['owner'],TARGET_FILE=tar_file)
        else:
            cmd = """su - {OWNER} -c "export ORACLE_SID={SID};sqlplus -s '/as sysdba'< {TARGET_FILE}" """.format(SID=db_info['sid'], OWNER=db_info['owner'], TARGET_FILE=tar_file)
        print cmd

        print os.popen(cmd).read()

    def set_sql_file(self, sid):
        src_files = os.path.join('sql', 'rman', 'fbrm_rman.sql')
        print src_files
        tar_dir = '/tmp/fbrm/rman_sql/fbrm_rman.sql'
        print src_files

        with open(src_file) as f:
            lineset = f.read()
        lineset = lineset.replace('{HOSTNAME}', self.hostname)
        lineset = lineset.replace('{DBNAME}', sid)
        # print lineset
        print 'tar_file :', tar_file, os.path.isfile(tar_file)
        print os.popen('chmod -R 777 /tmp/fbrm').read()
        with open(tar_file, 'w') as fw:
            fw.write(lineset)
        os.popen('chmod 666 %s' % (tar_file))

        return tar_file

    def get_data_list(self):
        data_list = glob.glob(os.path.join('/tmp', 'fbrm', 'data', '*.tmp'))
        return data_list

    def get_ret(self):
        data_file = '/tmp/fbrm/rman_data/%s_catdb_list_backup_detail.tmp' % self.hostname
        print data_file
        print os.path.isfile(data_file)
        with open(data_file) as f:
            lineset = f.read()
        lineset = lineset.replace('{HOSTNAME}', self.hostname)
        oracle_home = os.popen('env | grep ORACLE_HOME').read().strip()

        with open(self.file_name, 'w') as fw:
            msg = self.com.getHeadMsg('RMANCATDB_%s' % self.hostname)
            fw.write(msg + '\n')
            fw.write('***###datetime###***\n')
            fw.write(self.now + '\n')
            fw.write('***###hostname###***\n')
            hostname = os.popen('hostname').read().strip()
            fw.write(hostname + '\n')
            fw.write('***###agent_ip###***\n')
            fw.write(self.cfg.get('ibrm_agent', 'ip') + '\n')
            fw.write('***###ORACLE_HOME###***\n')
            fw.write(oracle_home + '\n')
            fw.write('***###rman_catalog###***\n')
            fw.write(lineset)
            end_msg = self.com.getEndMsg()
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

    def main(self):
        db_list= get_database.db_info().get_dbinfo_list()
        os.popen('rm -rf /tmp/fbrm/data/*.tmp').read()
        os.popen('rm -rf /tmp/fbrm/sql/*.sql').read()

        for db_info in db_list:


            self.rman(db_info)
            data_file = self.get_ret()
            # (FILENAME=fileName,DIR='ZFS_MON',ENDCHECK='NO').main()
            socketClient.SocketSender(FILENAME=self.file_name, DIR='FBRM_RMAN', ENDCHECK='YES').main()

            fbrm_rman_progress.fbrm_ora().get_progres(self.file_name)
            # try:
            #    os.remove(self.file_name)
            # except:
            #    pass




if __name__ == '__main__':
    fbrm_ora().main()
#    while True:
#        fbrm_ora().main()
#        time.sleep(60)
