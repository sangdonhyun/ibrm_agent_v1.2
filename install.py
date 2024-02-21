import os
import sys
import ConfigParser
import re
import platform


ibrm_path= os.path.dirname(os.path.abspath(__file__))

if not os.path.isdir(os.path.join(ibrm_path,'data')):
    os.mkdir(os.path.join(ibrm_path,'data'))
if not os.path.isdir(os.path.join(ibrm_path,'log')):
    os.mkdir(os.path.join(ibrm_path,'log'))
if not os.path.isdir(os.path.join(ibrm_path,'logs')):
    os.mkdir(os.path.join(ibrm_path,'logs'))

def set_config(ibrm_path,ibrm_cfg):
    cfg_file=os.path.join(ibrm_path,'config','config.cfg')
    cfg_msg="""[common]
ibrm_path = {IBRM_PATH}
shell_path= {SHELL_PATH}
log_path = {BACKUPLOG_PATH}
ora_home_bit = False
owner = db_owner

[ibrm_agent]
ip = {AGENT_IP}
socket_port = {AGENT_PORT}
restapi_port = {SERVER_RESTAPI_PORT}

[ibrm_server]
ip = {SERVER_IP}
socket_port = {SERVER_PORT}

[file_server]
ip = {SERVER_RECV_IP}
port = {SERVER_RECV_PORT}

[socket]
HOST = {AGENT_IP}
PORT = {AGENT_PORT}

[log]
log_remove = 30
logPath={IBRM_PATH}/logs
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

               )
    with open(cfg_file,'w') as fw:
        fw.write(cfg_msg)

    print("#"*100)
    print("VIEW [%s] Information" %(cfg_file))
    print("#"*100)
    print(cfg_msg)

def get_config(ins_path):
    ins_cfg_file = os.path.join(ins_path,'config','ibrm.ini')
    ins_cfg = ConfigParser.RawConfigParser()
    ins_cfg.read(ins_cfg_file)
    return ins_cfg


def get_ip():
    cmd="""ifconfig -a | grep "inet " | grep -v "127.0.0.1" | awk '{print $2}'"""
    ret=os.popen(cmd).read()
    ip_line_set=ret.splitlines()
    cnt=1

    print 'set num agent main ip'
    print '-' * 50
    for i in range(len(ip_line_set)):
        ip_line= ip_line_set[i]
        num = i+1
        print
        print 'number :',num,':',ip_line

    print '-' * 50
    num_ret=raw_input('SELECT ONE NUMBER>>')

    if num_ret.strip() == ''  or int(num_ret) < 1:
        get_ip()
    try:
        if int(num_ret) -1 in range(len(ip_line_set)):
            ip_line = ip_line_set[int(num_ret)-1]
            print ip_line
        else:
            print 'ip is no match this system'
    except:
        pass
    ip = None

    if ':'in ip_line:
        ip=ip_line.split(':')[-1].strip()
    else:
        ip=ip_line
    return ip

def set_ora_cfg(ins_path):
    ora_cfg_file = os.path.join(ins_path, 'config','list.cfg')
    cmd="ps -ef | grep ora_pmon | grep -v grep | grep -v rman | grep -v _CLONE"
    ret=os.popen(cmd).read()
    cfg = ConfigParser.RawConfigParser()
    cfg_file = os.path.join(ins_path, 'config', 'list.cfg')
    for line in ret.splitlines():
        lineset = line.split()
        ora_home = ''
        backup_log = ''
        if len(lineset) >6:
            owner = lineset[0]
            sid = lineset[-1].split('_')[-1]

            env_cmd = '/bin/su - {} -c "env"'.format(owner)

            env = os.popen(env_cmd).read()
            for line in env.splitlines():
                if 'ORACLE_HOME=' in line:
                    ora_home = line.split('=')[-1]
        cfg.add_section(sid)
        cfg.set(sid,'ORACLE_HOME',ora_home)
        cfg.set(sid, 'BACKUP_LOG', backup_log)

    print("#"*100)
    print("VIEW [%s] Information" %(cfg_file))
    print("#"*100)
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
    with open(cfg_file, 'w') as configfile:
        cfg.write(configfile)

if __name__=='__main__':
    agent_ip=get_ip()
    default_cfg=get_config(ibrm_path)
    ibrm_cfg={}
    ibrm_cfg['agent_ip'] = agent_ip
    for opt in default_cfg.options('SETUP'):
        ibrm_cfg[opt] = default_cfg.get('SETUP',opt)

    set_config(ibrm_path,ibrm_cfg)
    #set_ora_cfg(ibrm_path)