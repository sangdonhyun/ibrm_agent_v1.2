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
import cx_Oracle
import json
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
        self.ibrm_path = os.path.dirname(os.path.abspath(__file__))
        print self.ibrm_path
        self.python_path = os.path.join(self.ibrm_path,'python27','bin','python')


    def get_owner(self):
        cmd = 'ps -ef | grep ora_pmon_{ORA_SID} | grep -v grep '.format(ORA_SID=self.ora_sid)
        print cmd
        try:
            owner=os.popen(cmd).read().split()[0]
        except:
            owner = 'oracle'
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


        sql="""SELECT STATUS,to_char(START_TIME,'yyyy-mm-dd hh24:mi:ss') START_TIME,OUTPUT_BYTES_PER_SEC_DISPLAY
 FROM V_$RMAN_BACKUP_JOB_DETAILS WHERE SESSION_KEY={SESSION_ID}
""".format(SESSION_ID=session_id)

        print sql
        sfile = '/tmp/{SID}_rman.sql'.format(self.ora_sid)
        with open(sfile,'w') as f:
            f.write(sql)
        os.popen('chmod 777 {}'.format(sfile))

    def get_input_type(self):
        input_type = ''
        if self.shell_type == 'level0':
            input_type = 'DB INCR'
        elif self.shell_type == 'INCR':
            input_type = 'DB INCR'
        elif self.shell_type == 'ARCH':
            input_type = 'ARCHIVELOG'
        elif self.shell_type == 'FULL_L0':
            input_type = 'DB FULL'
        else:
            input_type = 'DB INCR'
        return input_type

    def get_session_id(self, old_session_id):

        arg = 'get_session_id {} "{}"'.format(old_session_id,self.shell_type)
        data = self.cx_query(arg)

    def ora_datetime(self):

        arg = 'get_sysdate'
        sysdate= self.cx_query(arg)
        return sysdate

    def cx_root_query(self,arg):
        print arg
        """
        export ORACLE_HOME=/usr/lib/oracle/12.1/client64
        export LD_LIBRARY_PATH=$ORACLE_HOME/lib
        export PATH=$ORACLE_HOME/bin:$PATH
        """
        oracle_home ="/u01/app/oracle/product/18c"
        ld_library_path = "{}/lib"
        os.environ['ORACLE_HOME'] = oracle_home
        os.environ['LD_LIBRARY_PATH'] = ld_library_path
        path = os.environ['PATH']
        os.environ['PATH'] = "{}/bin:{}}".format(oracle_home,path)



    def cx_query(self,arg):
        py_src = os.path.join(self.ibrm_path, 'cx_py', 'cx_oracle.py')
        cmd = 'su - {} -c "{} {} {}"'.format(self.owner, self.python_path, py_src,arg)
        print cmd
        return os.popen(cmd).read()


    def get_last_session_id(self):
        arg = 'get_last_session_key'
        last_session_id = self.cx_query(arg)
        return last_session_id

    def get_session_id(self, last_session_key):

        arg = 'get_session_id {} {}'.format(last_session_key,self.shell_type)
        data = self.cx_query(arg)
        print data


    def job_monitor(self,**kwargs):

        session_id= kwargs['session_id']
        arg = 'job_monitor {}'.format(session_id)

        print arg
        data  = self.cx_query(arg)
        print json.dumps(data)
        return json.dumps(data)




if __name__=='__main__':
    shell_type='ARCH'
    db_name ='ORCL'
    ora_sid = 'ORCL'
    rman = rman_control(shell_type=shell_type,db_name =db_name,ora_sid=ora_sid)


    sysdate=rman.ora_datetime()
    print '-'*30

    print 'sysdate :',sysdate
    print '#'*50

    last_session_key = rman.get_last_session_id().strip()
    print 'last session key :',last_session_key
    # print 'last session key :',last_session_key
    #
    # #session_id=  rman.get_session_id(sysdate,last_session_key)
    # #print 'new session_id', session_id
    # last_session_key= '77839'
    monitor_data=rman.job_monitor(session_id=last_session_key)

    print 'monitor_data :',monitor_data

    new_session_id = rman.get_session_id(last_session_key)
    # print 'ORACLE SID',ora_sid,
    # print 'ORACLE SID',ora_sid,
#
#    while True:
#        data=rman.job_monitor(sysdate=rtime,session_id=session_id)
#        if not 'RUNNING' in data['status']:
#            break
#        print data
#        time.sleep(5)

