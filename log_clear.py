import os
import glob
import datetime
import time
import ConfigParser

class clear():
    def __init__(self):
        self.cfg=self.get_cfg()

    def get_cfg(self):
        cfg = ConfigParser.RawConfigParser()
        cfg_file = os.path.join('config','config.cfg')
        cfg.read(cfg_file)
        return cfg


    def list_up(self,dir):
        file_list=glob.glob(os.path.join(dir,'*'))
        try:
            log_remove_day= int(self.cfg.get('log','log_remove'))
        except:
            log_remove_day = 30
        cnt=0
        for file in file_list:


            a_time = time.ctime(os.path.getatime(file))
            c_time = time.ctime(os.path.getctime(file))
            m_time = time.ctime(os.path.getmtime(file))
            atime_obj = datetime.datetime.strptime(a_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            ctime_obj = datetime.datetime.strptime(c_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            mtime_obj = datetime.datetime.strptime(m_time, '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S')
            remove_atime_obj = datetime.datetime.strptime(a_time, '%a %b %d %H:%M:%S %Y').strftime('%Y%m%d')
            # print '-'*40
            # print file
            # print atime_obj,ctime_obj,mtime_obj

            remove_bit= remove_atime_obj < (datetime.datetime.now() - datetime.timedelta(log_remove_day)).strftime('%Y%m%d')
            if remove_bit:
                try:
                    os.remove(file)
                    cnt=cnt+1
                except:
                    pass
        return cnt

    def main(self):
        remove_cnt=0
        dir_list=['logs','data']
        for dir in dir_list:
            remove_cnt=remove_cnt+self.list_up(dir)
        print 'remove log :',remove_cnt
        return remove_cnt

if __name__=='__main__':
    clear().main()