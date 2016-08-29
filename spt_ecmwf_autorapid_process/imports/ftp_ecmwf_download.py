# -*- coding: utf-8 -*-
##
##  ftp_ecmwf_download.py
##  spt_ecmwf_autorapid_process
##
##  Created by Alan D. Snow.
##  Copyright © 2015-2016 Alan D Snow. All rights reserved.
##  License: BSD-3 Clause

import datetime
from extractnested import ExtractNested, FileExtension
from glob import glob
import os
from shutil import rmtree

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
def ftp_connect(ftp_host, ftp_login, 
                ftp_passwd, ftp_directory):
    """
    Connect to ftp site
    """
    ftp = ftplib.FTP(ftp_host)
    ftp.login(ftp_login,ftp_passwd)
    ftp.cwd(ftp_directory)
    ftp.set_debuglevel(1)
    return ftp

                
def download_ftp(dst_filename, local_path, ftp_host, ftp_login, 
                ftp_passwd, ftp_directory):
    """
    Download single file from the ftp site
    """
    file = open(local_path, 'wb')
    print('Reconnecting ...')
    handle = ftp_connect(ftp_host, ftp_login, 
                         ftp_passwd, ftp_directory)
    handle.voidcmd('TYPE I')
    dst_filesize = handle.size(dst_filename)
    attempts_left = 15
    while dst_filesize > file.tell():
        try:
            if file.tell() == 0:
                res = handle.retrbinary('RETR %s' % dst_filename, file.write)
            else:
                # retrieve file from position where we were disconnected
                handle.retrbinary('RETR %s' % dst_filename, file.write, rest=file.tell())
        except Exception as ex:
            print(ex)
            if attempts_left == 0:
                print("Max number of attempts reached. Download stopped.")
                handle.quit()
                file.close()
                os.remove(local_path)
                return False
            print('Waiting 30 sec...')
            time.sleep(30)
            print('Reconnecting ...')
            handle.quit()
            handle = ftp_connect(ftp_host, ftp_login, 
                                 ftp_passwd, ftp_directory)
            print('Connected. {0} attempt(s) left.'.format(attempts_left))
        attempts_left -= 1
    handle.quit()
    file.close()
    return True

def remove_old_ftp_downloads(folder):
    """
    remove files/folders older than 1 days old
    """
    date_now = datetime.datetime.utcnow()
    all_paths = glob(os.path.join(folder,'Runoff*netcdf*'))
    for path in all_paths:
	date_file = datetime.datetime.strptime(os.path.basename(path).split('.')[1],'%Y%m%d')
        if os.path.isdir(path):
            rmtree(path)
        else:
            os.remove(path)
	if date_now - date_file < datetime.timedelta(1):
	    os.mkdir(path)
                
def download_all_ftp(download_dir, file_match, ftp_host, ftp_login, 
                     ftp_passwd, ftp_directory, max_wait=45):
    """
    Remove downloads from before 1 day ago
    Download all files from the ftp site matching date
    Extract downloaded files
    """
    if max_wait < 0:
        max_wait = 0
        
    remove_old_ftp_downloads(download_dir)
    #open the file for writing in binary mode
    all_files_downloaded = []
    print('Opening local file')
    time_start_connect_attempt = datetime.datetime.utcnow()
    request_incomplete = True
    ftp_exception = "FTP Request Incomplete"
    attempt_count = 1
    while ((datetime.datetime.utcnow()-time_start_connect_attempt)<datetime.timedelta(minutes=max_wait) \
          or attempt_count == 1) and request_incomplete:
        try:
            #init FTPClient (moved here because of traffic issues)
            ftp_client = PyFTPclient(host=ftp_host,
                                     login=ftp_login,
                                     passwd=ftp_passwd,
                                     directory=ftp_directory)
            ftp_client.connect()
            file_list = ftp_client.ftp.nlst(file_match)
            ftp_client.ftp.quit()
            #if there is a file list and the request completed, it is a success
            if file_list:
                for dst_filename in file_list:
                    local_path = os.path.join(download_dir, dst_filename)
                    local_dir = local_path[:-1*len(FileExtension(local_path))-1]
                    #download and unzip file
                    try:
                        #download from ftp site
                        unzip_file = False
                        if not os.path.exists(local_path) and not os.path.exists(local_dir):
                            print("Downloading from ftp site: {0}".format(dst_filename))
                            unzip_file = ftp_client.download_file(dst_filename, local_path)
                        else:
                            print('{0} already exists. Skipping download ...'.format(dst_filename))
                        #extract from tar.gz
                        if unzip_file:
                            print("Extracting: {0}".format(dst_filename))
                            ExtractNested(local_path, True)
                            #add successfully downloaded file to list
                            all_files_downloaded.append(local_dir)
                            #request successful when one file downloaded and extracted                            
                            request_incomplete = False
                        else:
                            print('{0} already extracted. Skipping extraction ...'.format(dst_filename))
                    except Exception as ex:
                        print(ex)
                        if os.path.exists(local_path):
                            os.remove(local_path)
                        continue
                    
        except Exception as ex:
            ftp_exception = ex
            pass
        
        if request_incomplete:
            print("Attempt {0} failed ...".format(attempt_count))
            attempt_count += 1
            if max_wait > 0:
                sleep_time = 5.1
                if max_wait < 5.1:
                    sleep_time = max(max_wait, 0.1)
                print("Sleeping for {0} minutes and trying again ...".format(sleep_time-0.1))
                time.sleep((sleep_time-0.1)*60)
            
        
        
    if request_incomplete:
        print("Maximum wait time of {0} minutes exeeded"
              " and request still failed. Quitting ...".format(max_wait))
        raise Exception(ftp_exception)
        
    print("All downloads completed!")
    return all_files_downloaded
