import datetime
from glob import glob
import os
from shutil import rmtree
import tarfile

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
                        print "DEBUG: %d  -  %0.1f Kb/s" % (i, (i-self.ptr)/(1024*self.monitor_interval))
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
                    print 'INFO: waiting 30 sec...'
                    time.sleep(30)
                    print 'INFO: reconnect'


            mon.set() #stop monitor
            self.ftp.close()

            if not res.startswith('226'): #file successfully transferred
                print 'ERROR: Downloaded file {0} is not full.'.format(dst_filename)
                print res
                return False
            return True
"""
end pyFTPclient adapation section
"""

def remove_old_ftp_downloads(folder):
    """
    remove files/folders older than 1 days old
    """
    date_now = datetime.datetime.utcnow()
    all_paths = glob(os.path.join(folder,'Runoff*netcdf*'))
    for path in all_paths:
        date_file = datetime.datetime.strptime(os.path.basename(path).split('.')[1],'%Y%m%d')
        if date_now - date_file > datetime.timedelta(1):
            if os.path.isdir(path):
                rmtree(path)
            else:
                os.remove(path)
                
def download_all_ftp(download_dir, file_match):
    """
    Remove downloads from before 2 days ago
    Download all files from the ftp site matching date
    Extract downloaded files
    """
    remove_old_ftp_downloads(download_dir)
    #init FTPClient
    ftp_client = PyFTPclient(host='ftp.ecmwf.int',
                             login='safer',
                             passwd='neo2008',
                             directory='tcyc')
    ftp_client.connect()
    #open the file for writing in binary mode
    print 'Opening local file'
    file_list = ftp_client.ftp.nlst(file_match)
    ftp_client.ftp.quit()
    all_files_downloaded = []
    for dst_filename in file_list:
        local_path = os.path.join(download_dir,dst_filename)
        #get correct local_dir
        if local_path.endswith('.tar.gz'):
            local_dir = local_path[:-7]
        else:
            local_dir = download_dir
        #download and unzip file
        try:
            #download from ftp site
            unzip_file = False
            if not os.path.exists(local_path) and not os.path.exists(local_dir):
                print "Downloading from ftp site: " + dst_filename
                unzip_file = ftp_client.download_file(dst_filename, local_path)
            else:
                print dst_filename + ' already exists. Skipping download.'
            #extract from tar.gz
            if unzip_file:
                os.mkdir(local_dir)
                print "Extracting: " + dst_filename
                tar = tarfile.open(local_path)
                tar.extractall(local_dir)
                tar.close()
                #add successfully downloaded file to list
                all_files_downloaded.append(local_dir)
            else:
                print dst_filename + ' already extracted. Skipping extraction.'
            #remove the tarfile
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception as ex:
            print ex
            continue
        
    print "All downloads completed!"
    return all_files_downloaded

if __name__ == "__main__":
    ecmwf_forecast_location = "C:/Users/byu_rapid/Documents/RAPID/ECMWF"
    time_string = datetime.datetime.utcnow().strftime('%Y%m%d')
    #time_string = datetime.datetime(2014,11,2).strftime('%Y%m%d')
    all_ecmwf_files = download_all_ftp(ecmwf_forecast_location,'Runoff.'+time_string+'*.netcdf.tar.gz')
