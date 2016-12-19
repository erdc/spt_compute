# -*- coding: utf-8 -*-
##
##  ftp_ecmwf_download.py
##  spt_ecmwf_autorapid_process
##
##  Created by Alan D. Snow.
##  Copyright © 2015-2016 Alan D Snow. All rights reserved.
##  License: BSD-3 Clause

import datetime
from glob import glob
import os
from shutil import rmtree

#local imports
from .extractnested import ExtractNested, FileExtension

"""
This section adapted from https://github.com/keepitsimple/pyFTPclient
"""
import threading
import ftplib
import socket
import time


def setInterval(interval, times = -1):
    # This will be the actual decorator,
    # with fixed interval and times parameter
    def outer_wrap(function):
        # This will be the function to be
        # called
        def wrap(*args, **kwargs):
            stop = threading.Event()

            # This is another function to be executed
            # in a different thread to simulate setInterval
            def inner_wrap():
                i = 0
                while i != times and not stop.isSet():
                    stop.wait(interval)
                    function(*args, **kwargs)
                    i += 1

            t = threading.Timer(0, inner_wrap)
            t.daemon = True
            t.start()
            return stop
        return wrap
    return outer_wrap


class PyFTPclient:
    def __init__(self, host, login, passwd, directory="", monitor_interval = 30):
        self.host = host
        self.login = login
        self.passwd = passwd
        self.directory = directory
        self.monitor_interval = monitor_interval
        self.ptr = None
        self.max_attempts = 15
        self.waiting = True
        self.ftp = ftplib.FTP(self.host)

    def connect(self):
        """
        Connect to ftp site
        """
        self.ftp = ftplib.FTP(self.host)
        self.ftp.set_debuglevel(1)
        self.ftp.set_pasv(True)
        self.ftp.login(self.login, self.passwd)
        if self.directory:
            self.ftp.cwd(self.directory)
        # optimize socket params for download task
        self.ftp.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 75)
        self.ftp.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)

    def download_file(self, dst_filename, local_filename = None):
        res = ''
        if local_filename is None:
            local_filename = dst_filename

        with open(local_filename, 'w+b') as f:
            self.ptr = f.tell()

            @setInterval(self.monitor_interval)
            def monitor():
                if not self.waiting:
                    i = f.tell()
                    if self.ptr < i:
                        print("DEBUG: %d  -  %0.1f Kb/s" % (i, (i-self.ptr)/(1024*self.monitor_interval)))
                        self.ptr = i
                    else:
                        self.ftp.close()

            self.connect()
            self.ftp.voidcmd('TYPE I')
            dst_filesize = self.ftp.size(dst_filename)

            mon = monitor()
            while dst_filesize > f.tell():
                try:
                    self.connect()
                    self.waiting = False
                    # retrieve file from position where we were disconnected
                    res = self.ftp.retrbinary('RETR %s' % dst_filename, f.write) if f.tell() == 0 else \
                              self.ftp.retrbinary('RETR %s' % dst_filename, f.write, rest=f.tell())

                except:
                    self.max_attempts -= 1
                    if self.max_attempts == 0:
                        mon.set()
                        raise
                    self.waiting = True
                    print('INFO: waiting 30 sec...')
                    time.sleep(30)
                    print('INFO: reconnect')


            mon.set() #stop monitor
            self.ftp.close()

            if not res.startswith('226'): #file successfully transferred
                print('ERROR: Downloaded file {0} is not full.'.format(dst_filename))
                print(res)
                return False
            return True
"""
end pyFTPclient adapation section
"""
def get_ftp_forecast_list(file_match, ftp_host, ftp_login, 
                          ftp_passwd, ftp_directory):
    """
    Retrieves list of forecast on ftp server
    """
    ftp_client = PyFTPclient(host=ftp_host,
                             login=ftp_login,
                             passwd=ftp_passwd,
                             directory=ftp_directory)
    ftp_client.connect()
    file_list = ftp_client.ftp.nlst(file_match)
    ftp_client.ftp.quit()
    return file_list


def remove_old_ftp_downloads(folder):
    """
    Remove all previous ECMWF downloads
    """
    all_paths = glob(os.path.join(folder,'Runoff*netcdf*'))
    for path in all_paths:
        if os.path.isdir(path):
            rmtree(path)
        else:
            os.remove(path)
            
def download_and_extract_ftp(download_dir, file_to_download, 
                             ftp_host, ftp_login, 
                             ftp_passwd, ftp_directory,
                             remove_past_downloads=True):
                                 
    """
    Downloads and extracts file from FTP server
    remove old downloads to preserve space
    """
    if remove_past_downloads
        remove_old_ftp_downloads(download_dir)
    
    ftp_client = PyFTPclient(host=ftp_host,
                             login=ftp_login,
                             passwd=ftp_passwd,
                             directory=ftp_directory)
    ftp_client.connect()
    file_list = ftp_client.ftp.nlst(file_to_download)
    ftp_client.ftp.quit()
    #if there is a file list and the request completed, it is a success
    if file_list:
        local_path = os.path.join(download_dir, file_to_download)
        local_dir = local_path[:-1*len(FileExtension(local_path))-1]
        #download and unzip file
        try:
            #download from ftp site
            unzip_file = False
            if not os.path.exists(local_path) and not os.path.exists(local_dir):
                print("Downloading from ftp site: {0}".format(file_to_download))
                unzip_file = ftp_client.download_file(file_to_download, local_path)
            else:
                print('{0} already exists. Skipping download ...'.format(file_to_download))
            #extract from tar.gz
            if unzip_file:
                print("Extracting: {0}".format(file_to_download))
                ExtractNested(local_path, True)
            else:
                print('{0} already extracted. Skipping extraction ...'.format(file_to_download))
        except Exception:
            if os.path.exists(local_path):
                os.remove(local_path)
            raise
        return local_dir