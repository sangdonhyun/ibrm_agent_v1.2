import os
import common
import datetime
import socketClient
import ConfigParser
import time
"""

"""
class fs():
    def __init__(self):
        self.com=common.Common
        self.hostname = os.popen('hostname').read().strip()
        self.fname = self.get_file_name()
        self.cfg = self.get_cfg()
        self.agent_ip = self.cfg.get('ibrm_agent', 'ip')
        self.check_date =datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config', 'config.cfg')
        cfg.read(cfg_file)
        return cfg

    def get_file_name(self):
        fname = 'posix_{HOSTNAME}_filesystem.tmp'.format(HOSTNAME=self.hostname)
        fname = os.path.join('data',fname)
        return fname

    def get_fs(self):
        cmd = 'df -k'
        ret=os.popen(cmd).read().strip()
        fs_list=[]

        line_list=ret.splitlines()
        head=line_list[0].split()
        new_lineset = []
        for i in range(len(line_list[:-1])):
            line = line_list[i]
            lineset=  line.split()
            print len(lineset)

            print line
            if len(lineset) == 6 :
                fs_list.append(lineset)
            else:
                print i,len(line_list)
                if not line[0] ==' ' and i < len(line_list):
                    lineset = line.split() + line_list[i+1].split()
                    if len(lineset) == 6:
                        fs_list.append(lineset)

        print head
        for fs in fs_list:
            print fs

    def fwrite(self,msg,wbit='a'):
        with open(self.fname,wbit) as f:
            f.write(msg+'\n')

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


    def nzf_is_mount(self):
        nfs_list=self.filesystem()
        nfs_mount_list = []
        for nfs in nfs_list:
            if ':' in  nfs[0]:
                fs= nfs[0]
                mounted = nfs[5]
                tfile= '{mounted_on}/nfs_mount.tmp'.format(mounted_on=mounted)
                cmd = 'touch %s'%tfile
                #print cmd
                ret=os.popen(cmd).read()
                #print ret
                mounted_bit = os.path.isfile(tfile)

                if ':' in fs:
                    zfs_hostname=fs.split(':')[0]
                nfs_mount_list.append([self.check_date,self.hostname, self.agent_ip,nfs[0],mounted,zfs_hostname,str(mounted_bit)])

                os.remove(tfile)
        print nfs_mount_list
        return nfs_mount_list




    def filesystem(self):
        data=os.popen('df -k').read()
        lineset=data.splitlines()
        arg_list=[]
        print data
        title = ['Filesystem','1K-blocks','Used','Available','Used_rate',' Mounted_on']
        for i in range(len(lineset)):
            line = lineset[i]
            arg_set = line.split()
            if len(arg_set) == 1:
                arg_list.append(arg_set + lineset[i+1].split())
            if len(arg_set) == 6:
                arg_list.append(arg_set)

        return arg_list


    def main(self):
        title = 'posix {HOSTNAME} filesystem'.format(HOSTNAME=self.hostname)
        print title
        head_msg=self.getHeadMsg(title)

        self.fwrite(head_msg,'w')

        self.fwrite('###***hostname***###')
        self.fwrite(self.hostname)
        self.fwrite('###***agent_ip***###')
        self.fwrite(self.agent_ip)
        self.fwrite('###***date_time***###')
        self.fwrite(self.check_date)
        self.fwrite('###***uname***###')
        self.fwrite(os.popen('uname -a').read())
        self.fwrite('###***release***###')
        self.fwrite(os.popen('cat /etc/*release*').read())
        self.fwrite('###***df -k***###')
        self.fwrite(os.popen('df -k').read())
        self.fwrite('###***virt-what***###')
        self.fwrite(os.popen('virt-what').read())
        self.fwrite('###***lscpu***###')
        self.fwrite(os.popen('lscpu').read())
        self.fwrite('###***memery***###')
        self.fwrite(os.popen('free').read())
        self.fwrite('###***is_dbms***###')
        self.fwrite(os.popen('ps -ef |grep ora_pmon | grep -v grep').read())
        self.fwrite('###***fdisk -l***###')
        self.fwrite(os.popen('fdisk -l').read())
        self.fwrite('###***lsblk***###')
        self.fwrite(os.popen('lsblk').read())
        self.fwrite('###***cat /etc/hosts***###')
        self.fwrite(os.popen('cat /etc/hosts').read())
        self.fwrite('###***nsf_mounted_on***###')
        nsf_mount_list=self.nzf_is_mount()
        for nfs in nsf_mount_list:
            print ','.join(nfs)
            self.fwrite(','.join(nfs))
        self.fwrite(self.getEndMsg())

        socketClient.SocketSender(FILENAME=self.fname, DIR='FBRM_FS', ENDCHECK='YES').main()





if __name__ =='__main__':
    fs().main()
    #while True:
    #    fs().main()
    #    time.sleep(60*60)
