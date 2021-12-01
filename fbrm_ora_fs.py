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
import stat
import socketClient
import time
import datetime

class fbrm_ora():

    def __init__(self):
        self.cfg=self.get_cfg()
        self.hostname = os.popen('hostname').read().strip()
        self.check_config()

    
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
        os.popen('chmod -R 666 /tmp/fbrm').read()
        
    



    def get_ora_env(self,SID):
        cfg=ConfigParser.RawConfigParser()
        cfg_file=  os.path.join('config','list.cfg')
        cfg.read(cfg_file)
        env_info={}
        if SID in cfg.sections():
            for opt in cfg.options(SID):
                os.environ[opt] = cfg.get(SID,opt)
                env_info[opt]= cfg.get(SID,opt)
        return env_info



    def set_sql_file(self,sid):
        src_files=glob.glob(os.path.join('sql','*.sql'))
        print src_files
        tar_dir='/tmp/fbrm/sql/'
        print src_files
        for src_file in src_files:
            fn=os.path.basename(src_file)
            tar_file = os.path.join(tar_dir,fn)
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
        print 'target_dir',os.path.isdir(tar_dir)
        return glob.glob(os.path.join(tar_dir,'*'))







    
    def ora(self,owner,sid,sql):
        print sql
        with open('/tmp/fbrm/sql/fbrm_ora.sql') as f:
            print f.read()
        cmd="""su - %s -c "export ORACLE_SID=%s;sqlplus  \'/as sysdba\' < %s" """%(owner,sid,sql)
        print cmd
        print os.popen(cmd).read()
        
    
    
    def get_ora(self):
        cmd='ps -ef | grep ora_pmon | grep -v grep'
        ret=os.popen(cmd).read()
        ora_list=[]
        for line in ret.splitlines():
            print line
            ora_ps=line.split()
            owner = ora_ps [0]
            sid = ora_ps[-1].split('_')[-1]
            print owner,sid
            if not sid == 'catdb':
                if [owner,sid] not in ora_list:
                    ora_list.append([owner,sid])

        return ora_list
    


    def get_data_list(self):
        data_list = glob.glob(os.path.join('/tmp', 'fbrm', 'data', '*.tmp'))
        return data_list

    def get_ret(self):
        data_list= self.get_data_list()
        for fn in data_list:
            print fn
            with open(fn) as f:
                print f.read()
        print data_list

    def get_zfs(self,sid):
        cfg=ConfigParser.RawConfigParser()
        cfgFile = os.path.join('config','zfs_config.cfg')
        cfg.read(cfgFile)
        zfs_ip,zfs_name = None,None
        for sec in cfg.sections():
            if sec== sid :
                zfs_ip = cfg.get(sec,'ZFS_IP')
                zfs_name = cfg.get(sec,'ZFS_NAME')
        return zfs_ip,zfs_name

    def get_ora_fs(self,sid,owner,env_info):

        spool_file='/tmp/{HOSTNAME}_{DBNAME}_hsrm_ora_fs.tmp'
        spool_file=spool_file.format(HOSTNAME=self.hostname,DBNAME=sid)

        sql_file=os.path.join('sql','hsrm_ora.sql')
        with open(sql_file) as f:
            lineset=f.read()
        print lineset

        lineset = lineset.replace('{HOSTNAME}',self.hostname)
        lineset = lineset.replace('{DBNAME}',sid)
        print lineset
        tsql_file= os.path.join('/tmp','hsrm_ora_fs.sql')
        print tsql_file
        os.chmod('/tmp/fbrm', stat.S_IROTH)
        print os.popen('ls -al /tmp/*.sql').read()
        with open(tsql_file,'w') as f:
            f.write(lineset)
        print os.popen('ls -al /tmp/*.sql').read()
        cmd="chmod 777 {sql_file}"
        cmd=cmd.format(sql_file=tsql_file)
        print os.popen('ls -al /tmp/fbrm/sql/*').read()
        print os.popen('chmod 777 /tmp/*.sql').read()


        env_cnt = len(env_info.keys())
        env_str='export '
        if env_cnt > 0:
            for i in range(len(env_info.keys())):
                key = env_info.keys()[i]
                val = env_info.values()[i]
                env_str=env_str + key.upper() + '=' + val +';'
            print key.upper(), val
            cmd = """su - {ora_user} -c "{env_str}export ORACLE_SID={sid};sqlplus  \'/as sysdba\' < {sql_file}" """
            cmd = cmd.format(env_str=env_str,sid=sid, sql_file=tsql_file, ora_user=owner)
        else:
            cmd = """su - {ora_user} -c "export ORACLE_SID={sid};sqlplus  \'/as sysdba\' < {sql_file}" """
            cmd=cmd.format(sid=sid,sql_file=tsql_file,ora_user=owner)

        print cmd
        spools=  os.popen(cmd).read()
        print spool_file,os.path.isfile(spool_file)

        if os.path.isfile(spool_file):
            with open(spool_file) as f:
                spool = f.read()
        else:
            spool = spools
            #print spool
        ttime=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        fname = '{HOSTNAME}_{DBNAME}_hsrm_ora_fs.tmp'
        fname = fname.format(HOSTNAME=self.hostname,DBNAME=sid,TIME=ttime)

        ora_tmp_file= os.path.join('data',fname)
        agent_ip = self.cfg.get('ibrm_agent','ip')

        with open(ora_tmp_file,'w') as fw:
            head_msg=self.getHeadMsg('{HOSTNAME}_{DBNAME}_ORA.tmp')
            head_msg=head_msg.format(HOSTNAME=self.hostname,DBNAME=sid)
            fw.write(head_msg)
            fw.write('###***HOSTNAME***###\n')
            fw.write(self.hostname+'\n')
            fw.write('###***AGENT_IP***###\n')
            fw.write(agent_ip+ '\n')
            fw.write('###***date_time***###\n')

            fw.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\n')
            fw.write('###***ORACLE_SID***###\n')
            fw.write(sid + '\n')
            fw.write('###***ORACLE_HOME***###\n')
            cmd = 'su - {ora_user} -c "env"'.format(ora_user=owner)
            fw.write(os.popen(cmd).read() + '\n')
            fw.write('###***ORACLE_SPOOL***###\n')
            fw.write(spool)
            fw.write('\n')
            fw.write(self.getEndMsg())

        socketClient.SocketSender(FILENAME=ora_tmp_file, DIR='FBRM_ORA_HSRM', ENDCHECK='YES').main()





    def getHeadMsg(self, title='FLETA BATCH LAOD'):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = '\n'
        msg += '#' * 79 + '\n'
        msg += '#### ' + ' ' * 71 + '###\n'
        msg += '#### ' + ('TITLE     : %s' % title).ljust(71) + '###\n'
        msg += '#### ' + ('DATA TIME : %s' % now).ljust(71) + '###\n'
        msg += '#### ' + ' ' * 71 + '###\n'
        msg += '#' * 79 + '\n'
        return msg

    def getEndMsg(self):
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = '\n'
        msg += '#' * 79 + '\n'
        msg += '####  ' + ('END  -  DATA TIME : %s' % now).ljust(71) + '###\n'
        msg += '#' * 79 + '\n'
        return msg

    def main(self):
        print  self.get_data_list()
        os.popen('rm -rf /tmp/fbrm/data/*.tmp').read()
        os.popen('rm -rf /tmp/fbrm/ora/*.sql').read()
        print self.get_data_list()

        ora_list=self.get_ora()
        print ora_list
        print '-' *50
        print '-' * 50
        print '-' * 50
        for owner,sid in ora_list:
            env_info=self.get_ora_env(sid)
            print env_info,env_info.keys(),env_info.values()
            self.get_ora_fs(sid,owner,env_info)



if __name__=='__main__':
   fbrm_ora().main()

#    while True:
#        fbrm_ora().main()
#        time.sleep(60*5)
