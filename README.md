# spt_ecmwf_autorapid_process
Code to use to prepare input data for RAPID from ECMWF forecast using HTCondor

[![License (3-Clause BSD)](https://img.shields.io/badge/license-BSD%203--Clause-yellow.svg)](https://github.com/erdc-cm/spt_ecmwf_autorapid_process/blob/master/LICENSE)

[![DOI](https://zenodo.org/badge/19918/erdc-cm/spt_ecmwf_autorapid_process.svg)](https://zenodo.org/badge/latestdoi/19918/erdc-cm/spt_ecmwf_autorapid_process)

Note: For steps 1-2, use the *install_rapid_htcondor.sh* at your own risk.

##Step 1: Install RAPID and RAPIDpy
See: https://github.com/erdc-cm/RAPIDpy

##Step 2: Install HTCondor (if not using Amazon Web Services and StarCluster or not using Multiprocessing mode)
###On Ubuntu
```
apt-get install -y libvirt0 libdate-manip-perl vim
wget http://ciwckan.chpc.utah.edu/dataset/be272798-f2a7-4b27-9dc8-4a131f0bb3f0/resource/86aa16c9-0575-44f7-a143-a050cd72f4c8/download/condor8.2.8312769ubuntu14.04amd64.deb
dpkg -i condor8.2.8312769ubuntu14.04amd64.deb
```
###On RedHat/CentOS 7
See: https://research.cs.wisc.edu/htcondor/yum/
###After Installation:
```
#if master node uncomment CONDOR_HOST and comment out CONDOR_HOST and DAEMON_LIST lines
#echo CONDOR_HOST = \$\(IP_ADDRESS\) >> /etc/condor/condor_config.local
echo CONDOR_HOST = 10.8.123.71 >> /etc/condor/condor_config.local
echo DAEMON_LIST = MASTER, SCHEDD, STARTD >> /etc/condor/condor_config.local
echo ALLOW_ADMINISTRATOR = \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_OWNER = \$\(FULL_HOSTNAME\), \$\(ALLOW_ADMINISTRATOR\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_READ = \$\(FULL_HOSTNAME\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_WRITE = \$\(FULL_HOSTNAME\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo START = True >> /etc/condor/condor_config.local
echo SUSPEND = False >> /etc/condor/condor_config.local
echo CONTINUE = True >> /etc/condor/condor_config.local
echo PREEMPT = False >> /etc/condor/condor_config.local
echo KILL = False >> /etc/condor/condor_config.local
echo WANT_SUSPEND = False >> /etc/condor/condor_config.local
echo WANT_VACATE = False >> /etc/condor/condor_config.local
```
NOTE: if you forgot to change lines for master node, change CONDOR_HOST = $(IP_ADDRESS)
and restart condor as ROOT

If Ubuntu:
```
# . /etc/init.d/condor start
```
If RedHat:
```
# systemctl start condor
```
##Step 3: Install Prerequisite Packages
###On Ubuntu:
```
$ apt-get install libssl-dev libffi-dev
$ sudo su
$ pip install requests_toolbelt tethys_dataset_services condorpy
$ exit
```
###On RedHat/CentOS 7:
```
$ yum install libffi-devel openssl-devel
$ sudo su
$ pip install requests_toolbelt tethys_dataset_services condorpy
$ exit
```
If you are on RHEL 7 and having troubles, add the epel repo:
```
$ wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
$ sudo rpm -Uvh epel-release-7*.rpm
```
If you are on CentOS 7 and having troubles, add the epel repo:
```
$ sudo yum install epel-release
```
Then install packages listed above.

##Step 4: Install AutoRoute and AutoRoutePy
Follow the instructions here: https://github.com/erdc-cm/AutoRoutePy

##Step 5: Download the source code
```
$ cd /path/to/your/scripts/
$ git clone https://github.com/erdc-cm/spt_ecmwf_autorapid_process.git
$ cd spt_ecmwf_autorapid_process
$ git submodule init
$ git submodule update
```
##Step 6: Install Submodule Dependencies
See: https://github.com/erdc-cm/spt_dataset_manager

##Step 7: Create folders for RAPID input and for downloading ECMWF
In this instance:
```
$ cd /your/working/directory
$ mkdir -p rapid-io/input rapid-io/output ecmwf logs condor_logs
```
##Step 8: Change the locations in the files
Create a file *run.py* and change these variables for your instance:
```python
# -*- coding: utf-8 -*-
from rapid_process import run_ecmwf_rapid_process
#------------------------------------------------------------------------------
#main process
#------------------------------------------------------------------------------
if __name__ == "__main__":
    run_ecmwf_rapid_process(
        rapid_executable_location='/home/alan/work/rapid/src/rapid',
        rapid_io_files_location='/home/alan/work/rapid-io',
        ecmwf_forecast_location ="/home/alan/work/ecmwf",
        era_interim_data_location="/home/alan/work/era_interim_watershed",
        subprocess_log_directory='/home/alan/work/condor_logs/', #path to store HTCondor/multiprocess logs
        main_log_directory='/home/alan/work/logs/',
        data_store_url='http://your-ckan/api/3/action',
        data_store_api_key='your-ckan-api-key',
        data_store_owner_org="your-organizatopn",
        app_instance_id='your-streamflow_prediction_tool-app-id',
        sync_rapid_input_with_ckan=False, #make rapid input sync with your app
        download_ecmwf=True,
        ftp_host="ftp.ecmwf",
        ftp_login="",
        ftp_passwd="",
        ftp_directory="",
        upload_output_to_ckan=True,
        initialize_flows=True,
        create_warning_points=True,
        delete_output_when_done=True,
        autoroute_executable_location='/home/alan/work/scripts/AutoRouteGDAL/source_code/autoroute',
        autoroute_io_files_location='/home/alan/work/autoroute-io',
        geoserver_url='http://localhost:8181/geoserver/rest',
        geoserver_username='admin',
        geoserver_password='password',
        mp_mode='htcondor', #valid options are htcondor and multiprocess,
        mp_execute_directory='',#required if using multiprocess mode
    )


```

##Step 9: Make sure permissions are correct for these files and any directories the script will use

Example:
```
$ chmod u+x run.py
$ chmod u+x rapid_process.sh
```
##Step 10: Add RAPID files to the work/rapid/input directory
Make sure the directory is in the format [watershed name]-[subbasin name]
with lowercase letters, numbers, and underscores only. No spaces!


Example:
```
$ ls /rapid/input
nfie_texas_gulf_region-huc_2_12
$ ls /rapid/input/nfie_texas_gulf_region-huc_2_12
comid_lat_lon_z.csv
k.csv
rapid_connect.csv
riv_bas_id.csv
weight_ecmwf_t1279.csv
weight_ecmwf_tco639.csv
x.csv
```
##Step 11: Create CRON job to run the scripts twice daily
See: http://askubuntu.com/questions/2368/how-do-i-set-up-a-cron-job

You only need to run rapid_process.sh
```
$ ./rapid_process.sh
```
###How to use *create_cron.py* to create the CRON jobs:

1) Install crontab Python package.
```
$ pip install python-crontab
```
2) Modify location of script in *create_cron.py*
```python
cron_command = '/home/cecsr/scripts/erfp_data_process_ubuntu_aws/rapid_process.sh'
```
3) Change execution times to suit your needs in *create_cron.py*
```python
cron_job_morning.minute.on(30)
cron_job_morning.hour.on(9)
...
cron_job_evening.minute.on(30)
cron_job_evening.hour.on(21)
```

#Troubleshooting
If you see this error:
ImportError: No module named packages.urllib3.poolmanager
```
$ pip install pip --upgrade
```
Restart your terminal
```
$ pip install requests --upgrade
```
