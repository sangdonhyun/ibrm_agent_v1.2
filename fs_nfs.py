import os
import common
import datetime
import socketClient
import ConfigParser
import time
import json
"""

"""
class fs():
    def __init__(self):
        self.com=common.Common
        self.hostname = os.popen('hostname').read().strip()
        self.cfg = self.get_cfg()
        self.agent_ip = self.cfg.get('ibrm_agent', 'ip')
        self.check_date =datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config', 'config.cfg')
        cfg.read(cfg_file)
        return cfg



    def nzf_is_mount(self):
        nfs_list=self.filesystem()
        nfs_mount_list = []
        for nfs in nfs_list:
            if ':' in  nfs[0]:
                fs= nfs[0]
                mounted = nfs[5]
                tfile= '{mounted_on}/nfs_mount.tmp'.format(mounted_on=mounted)
                cmd = 'touch %s'%tfile

                try:
                    ret=os.popen(cmd).read()
                    mounted_bit = os.path.isfile(tfile)
                except:
                    mounted_bit = False

                if ':' in fs:
                    zfs_hostname=fs.split(':')[0]
                nfs_mount_list.append([self.check_date, self.hostname, self.agent_ip, nfs[0], mounted, zfs_hostname, str(mounted_bit)])
                try:
                    os.remove(tfile)
                except Exception as e:
                    print str(e)
                    pass
        return nfs_mount_list




    def get_nfs_list(self):
        data = os.popen('df -k').read()
        lineset = data.splitlines()
        arg_list = []

        title = ['Filesystem', '1K-blocks', 'Used', 'Available', 'Used_rate', 'Mounted_on']
        for i in range(len(lineset)):
            line = lineset[i]
            arg_set = line.split()
            if len(arg_set) == 1:
                arg_list.append(arg_set + lineset[i + 1].split())
            if len(arg_set) >= 6:
                arg_list.append(arg_set)



        data=os.popen('df -k').read()
        nfs_list =[]
        for arg_set in arg_list:

            Filesystem = arg_set[0]
            Mounted_on = arg_set[-1]
            if '/ZFS/' in Mounted_on:
                nfs_info = dict()
                nfs_info['Filesystem'] = Filesystem
                nfs_info['Mounted_on'] = Mounted_on
                nfs=os.statvfs(Mounted_on)
                nfs_info['free'] = nfs.f_bsize * nfs.f_bavail
                nfs_info['total'] = nfs.f_bsize * nfs.f_blocks
                nfs_info['used'] = nfs.f_bsize * (nfs.f_blocks - nfs.f_bavail)
                mounted_bit = self.get_mounted_bit(Mounted_on)
                nfs_info['mounted_bit'] = mounted_bit
                nfs_list.append(nfs_info)
        return nfs_list

    def get_mounted_bit(self,fs):
        mounted_bit = False

        mount_file = '{}/mounted_bit.tmp'.format(fs)
        ret=os.popen('touch {}'.format(mount_file)).read()
        if os.path.isfile(mount_file):
            mounted_bit = True
            os.remove(mount_file)

        return mounted_bit


    def get_hosts(self):
        cmd = 'cat /etc/hosts'
        ret = os.popen(cmd).read()

        ip_list=[]
        for line in ret.splitlines():

            if '#'in line:
                line = line[0:line.index('#')]
            ip_set = line.split()
            if len(ip_set) == 2:
                ip_list.append({'ip':ip_set[0],'hostname':ip_set[1]})
            elif len(ip_set) > 2:
                ip_list.append({'ip':ip_set[0],'hostname':ip_set[1],'alias':ip_set[2]})

        return ip_list


    def return_data(self,send_title,send_data):
        return_info = dict()
        return_info['hostname'] = self.hostname
        return_info['ip'] = self.agent_ip
        return_info['datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return_info[send_title] = send_data
        json_data = json.dumps(return_info)
        return json_data

    def main(self):
        nfs_info = dict()
        json_data = ""
        nfs_list=self.get_nfs_list()

        return_data = self.return_data('nfs_list',nfs_list)
        print return_data
        # print 'host list :',self.get_hosts()
        host_list = self.get_hosts()
        return_data = self.return_data('host_list', host_list)
        print return_data
if __name__ =='__main__':
    fs().main()

