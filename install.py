import os
import sys
import ConfigParser
import fbrm_fs
import fbrm_ora_fs
import fbrm_ora_get
import fbrm_nfs
import re


inst_path= os.path.dirname(os.path.abspath(__file__))

if not os.path.isdir('data'):
    os.mkdir('data')
if not os.path.isdir('log'):
    os.mkdir('log')
if not os.path.isdir('logs'):
    os.mkdir('logs')
def set_config(ibrm_path,ibrm_cfg):
    print inst_path
    cfg_file=os.path.join(ibrm_path,'ibrm_agent_v1','config','config.cfg')
    print 'cfg_file :',cfg_file


    cfg_msg="""

[common]
ibrm_path = {IBRM_PATH}/ibrm_agent_v1
shell_path= {SHELL_PATH}
log_path = {BACKUPLOG_PATH}
ora_home_bit = True
owner = db_owner

[ibrm_agent]
ip = {AGENT_IP}
socket_port = {AGENT_PORT}
restapi_port = {SERVER_RESTAPI_PORT}

[ibrm_server]
ip = {SERVER_IP}
socket_port = {SERVER_PORT}

[file_server]
ip = {SERVER_IP}
port = {SERVER_FILE_RECV_PORT}

[socket]
HOST = {AGENT_IP}
PORT = 53001

[log]
log_remove = 30
logPath={IBRM_PATH}/ibrm_agent_v1/logs    
    """.format(IBRM_PATH=ibrm_cfg['ibrm_path'].replace("//",'/'),
               SHELL_PATH =ibrm_cfg['shell_path'].replace("//",'/'),
               BACKUPLOG_PATH=ibrm_cfg['backuplog_path'].replace("//",'/'),
               AGENT_IP=ibrm_cfg['agent_ip'],
               AGENT_PORT=ibrm_cfg['agent_port'],
               SERVER_RESTAPI_PORT=ibrm_cfg['server_restapi_port'],
               SERVER_IP=ibrm_cfg['server_ip'],
               SERVER_PORT=ibrm_cfg['server_port'],
               SERVER_RECV_IP=ibrm_cfg['server_recv_ip'],
               SERVER_RECV_PORT=ibrm_cfg['server_recv_port'],
               SERVER_FILE_RECV_PORT=ibrm_cfg['server_file_recv_port'],
               )
    print cfg_msg
    with open(cfg_file,'w') as fw:
        fw.write(cfg_msg)


def get_config(ins_path):
    ins_cfg_file = os.path.join(inst_path,'ibrm.ini')
    ins_cfg = ConfigParser.RawConfigParser()
    ins_cfg.read(ins_cfg_file)
    return ins_cfg


def get_ip():
    cmd="""ifconfig | grep "inet addr" | grep -v "127.0.0.1" | awk '{print $2}'"""
    ret=os.popen(cmd).read()
    ip_line_set=ret.splitlines()
    cnt=1
    print 'set num agnet main ip'
    print '-' * 50
    for i in range(len(ip_line_set)):
        ip_line= ip_line_set[i]
        num = i+1
        print 'number :',num,':',ip_line

    print '-' * 50
    num_ret=raw_input('SELECT ONE NUMBER>>')
    if int(num_ret) -1 in range(len(ip_line_set)):
        ip_line = ip_line_set[int(num_ret)-1]
        print ip_line
    else:
        print 'ip is no match this system'
    ip = None
    if ':'in ip_line:
        ip=ip_line.split(':')[-1].strip()
    else:
        ip=ip_line
    return ip

def set_ora_cfg():
    """
    config = configparser.RawConfigParser()

config.add_section('oracle_sid')
config.set('oracle_sid', 'ORACLE_HOME', '/u01/app/oracle/product/11.2.0.4')
config.set('oracle_sid', 'BACKUP_LOG', '/ZFS/LOGS/IBRM/IBRM_{YYYY}_{MMDD}')

# Writing our configuration file to 'example.cfg'
with open('config\\list.cfg', 'w') as configfile:
    config.write('config\\list.cfg')
    :return:
    """
    ora_cfg_file = os.path.join('config','list.cfg')
    cmd="ps -ef | grep ora_pmon | grep -v grep | grep -v rman | grep -v _CLONE"
    ret=os.popen(cmd).read()
    cfg = ConfigParser.RawConfigParser()
    cfg_file = os.path.join('config','list.cfg')
    for line in ret.splitlines():
        lineset = line.split()
        ora_home = ''
        backup_log = ''
        if len(lineset) >6:
            owner = lineset[0]
            sid = lineset[-1].split('_')[-1]

            env_cmd = 'su - {} -c "env"'.format(owner)
            env = os.popen(env_cmd).read()
            for line in env.splitlines():
                if 'ORACLE_HOME=' in line:
                    ora_home = line.split('=')[-1]
            print "#"*50
            print 'ORACLE_SID :',sid
            print 'ORACLE_OWNER :',owner
            print ''
            print 'set  {} DATABASE ORACLE_HOME PATH'.format(sid)
            print '-' * 50
            raw_ora_home=ora_home
            print 'set  {} DATABASE BACKUP LOG  PATH'.format(sid)
            print '-' * 50
            print 'ORACLE_HOME :',ora_home
            backup_log='/ZFS/LOGS/%s/%s_{YYYY}_{MMDD}'%(sid,sid)
            print 'BACKUP LOG PATH :', backup_log
            print '-' * 50
            raw_input("enter")
        cfg.add_section(sid)
        cfg.set(sid,'ORACLE_HOME',ora_home)
        cfg.set(sid, 'BACKUP_LOG', backup_log)

    with open(cfg_file, 'w') as configfile:
        cfg.write(configfile)


if __name__=='__main__':
    arg = sys.argv
    inst_path = arg[1]
    ibrm_path = arg[2]

    print 'INST PATH :',inst_path
    print 'IBRM PATH :',ibrm_path
    agent_ip=get_ip()
    print 'agent_ip :',agent_ip
    default_cfg=get_config(inst_path)
    ibrm_cfg={}
    ibrm_cfg['agent_ip'] = agent_ip
    for opt in default_cfg.options('SETUP'):
        ibrm_cfg[opt] = default_cfg.get('SETUP',opt)

    set_config(ibrm_path,ibrm_cfg)
    start_sh="""#!/bin/sh
echo `netstat -an | grep 53001`
export PATH={IBRM_PATH}/ibrm_agent_v1/python27/bin:$PATH
if [[ $(id -u) -ne 0 ]] ; then echo "Please run as root" ; exit 1 ; fi
python ./ibrm_menu.py
""".format(IBRM_PATH=ibrm_path)
    print start_sh
    start_sh_file = os.path.join(ibrm_path,'ibrm_agent_v1','ibrm_start.sh')
    with open(start_sh_file,'w') as fw:
        fw.write(start_sh)


    set_ora_cfg()
    fbrm_fs.fs().main()
    fbrm_ora_fs.fbrm_ora().main()
    fbrm_ora_get.fbrm_ora().main()
    fbrm_nfs.fs().main()
