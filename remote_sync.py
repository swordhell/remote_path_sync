#!/usr/bin/python
# encoding: utf-8
# 
# pip install fabric==2.5.0
# pip install pypiwin32
#
import logging
import os
from fabric import Connection
import hashlib
import json
import sys
import glob

logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')

class r_sync(object):
    def __init__(self,_cfg_file):
        super().__init__()
        self.cfg_file_ = _cfg_file
        self.load_cfg()

    def work(self):
        self.open_conn()
        self.check_sync()
        self.close_conn()
        pass
    
    def load_cfg(self):
        self.j_obj_ = None
        with open(self.cfg_file_, 'r') as jsonFile:
            self.j_obj_ = json.load(jsonFile)
        self.r_ip_ = self.j_obj_["r_ip"]
        self.r_usr_ = self.j_obj_["r_usr"]
        self.r_path_ = self.j_obj_["r_path"]
        self.keyfile_ = self.j_obj_["keyfile"]
        self.l_path_ = self.j_obj_["l_path"]
    
    def open_conn(self):
        # 链接远程服务器的配置
        self.conn_ = Connection(host=self.r_ip_,
            user=self.r_usr_,
            connect_kwargs={"key_filename": self.keyfile_,})
        self.conn_.open()
        logging.info(self.conn_.is_connected)
    def check_sync(self):
        old_cwd = os.getcwd()
        os.chdir(self.l_path_)
        self.recursion_path("./")
        os.chdir(old_cwd)
    def recursion_path(self,_path):
        logging.info("dir: {0}".format(_path))
        for root,dirs,files in os.walk(_path):
            for dn in dirs:
                sub_d = self.win2linux(str(_path) +"/"+ str(dn))
                if os.path.exists(sub_d) and os.listdir(sub_d):
                    self.recursion_path(sub_d)
            for fn in files:
                fn = self.win2linux(str(_path) +"/"+ str(fn))
                if os.path.exists(fn):
                    self.check_single_file(fn)
    def check_single_file(self,_filename):
        lmd5 = self.get_l_md5(_filename)
        rmd5 = self.get_r_md5(_filename)
        logging.info("local file: {0}, \nlmd5: {1}".format(_filename,lmd5))
        logging.info("rmd5: {0}".format(rmd5))
        if lmd5 != rmd5:
            self.put_file(_filename)
    def close_conn(self):
        self.conn_.close()
    def win2linux(self, _path):
        _path = _path.replace("\\","/")
        _path = _path.replace("/./","/")
        _path = _path.replace("//","/")
        return _path
    def get_r_md5(self,_name):
        r_p = self.win2linux("{0}/{1}".format(self.r_path_,_name))
        cmd="mkdir -p {0}".format(os.path.dirname(r_p))
        self.conn_.run(cmd)
        cmd = "md5sum {0}".format(r_p) + "| awk '{print $1}'"
        retstr = self.conn_.run(cmd)
        return retstr.stdout[:-1]
    def get_l_md5(self, _name):
        if not os.path.isfile(_name):
            return None
        myhash = hashlib.md5()
        f = open(_name,'rb')
        while True:
            b = f.read(8096)
            if not b :
                break
            myhash.update(b)
        f.close()
        return myhash.hexdigest()
    def put_file(self, _name):
        self.conn_.put(_name,self.win2linux("{0}/{1}".format(self.r_path_,_name)))

def main():
    if len(sys.argv) == 1:
        r = r_sync("./config.json")
        r.work()
        pass
    else:
        for cfg_name in sys.argv[1:]:
            r = r_sync(cfg_name)
            r.work()

if __name__ == '__main__':
    main()