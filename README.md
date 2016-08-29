# spt_ecmwf_autorapid_process
Computational framework to ingest ECMWF ensemble runoff forcasts; generate input for and run the RAPID (rapid-hub.org) program using HTCondor or Python\'s Multiprocessing; and upload to CKAN in order to be used by the Streamflow Prediction Tool (SPT). There is also an experimental option to use the AutoRoute program for flood inundation mapping.

[![License (3-Clause BSD)](https://img.shields.io/badge/license-BSD%203--Clause-yellow.svg)](https://github.com/erdc-cm/spt_ecmwf_autorapid_process/blob/master/LICENSE)

[![DOI](https://zenodo.org/badge/19918/erdc-cm/spt_ecmwf_autorapid_process.svg)](https://zenodo.org/badge/latestdoi/19918/erdc-cm/spt_ecmwf_autorapid_process)

##How it works:

Snow, Alan D., Scott D. Christensen, Nathan R. Swain, E. James Nelson, Daniel P. Ames, Norman L. Jones,
Deng Ding, Nawajish S. Noman, Cedric H. David, Florian Pappenberger, and Ervin Zsoter, 2016. A High-Resolution
National-Scale Hydrologic Forecast System from a Global Ensemble Land Surface Model. *Journal of the
American Water Resources Association (JAWRA)* 1-15, DOI: 10.1111/1752-1688.12434

Snow, Alan Dee, "A New Global Forecasting Model to Produce High-Resolution Stream Forecasts" (2015). All Theses and Dissertations. Paper 5272. http://scholarsarchive.byu.edu/etd/5272

#Installation

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
# . /etc/init.d/condor stop
# . /etc/init.d/condor start
```
If RedHat:
```
# systemctl stop condor
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

##Step 5: Install Submodule Dependencies
See: https://github.com/erdc-cm/spt_dataset_manager

##Step 6: Download and install the source code
```
$ cd /path/to/your/scripts/
$ git clone https://github.com/erdc-cm/spt_ecmwf_autorapid_process.git
$ cd spt_ecmwf_autorapid_process
$ git submodule init
$ git submodule update
$ python setup.py install
```

##Step 7: Create folders for RAPID input and for downloading ECMWF
```
$ cd /your/working/directory
$ mkdir -p rapid-io/input rapid-io/output ecmwf logs subprocess_logs era_interim_watershed mp_execute 
```
##Step 8: Change the locations in the files
Create a file *run_ecmwf_rapid.py* and change these variables for your instance. See below for different configurations.

```python
# -*- coding: utf-8 -*-
from spt_ecmwf_autorapid_process import run_ecmwf_rapid_process
#------------------------------------------------------------------------------
#main process
#------------------------------------------------------------------------------
if __name__ == "__main__":
    run_ecmwf_rapid_process(
        rapid_executable_location='/home/alan/scripts/rapid/src/rapid',
        rapid_io_files_location='/home/alan/rapid-io',
        ecmwf_forecast_location ="/home/alan/ecmwf",
        era_interim_data_location="/home/alan/era_interim_watershed",
        subprocess_log_directory='/home/alan/subprocess_logs',
        main_log_directory='/home/alan/logs',
        data_store_url='http://your-ckan/api/3/action',
        data_store_api_key='your-ckan-api-key',
        data_store_owner_org="your-organization",
        app_instance_id='your-streamflow_prediction_tool-app-id',
        #sync_rapid_input_with_ckan=False, 
        download_ecmwf=True,
        ftp_host="ftp.ecmwf.int",
        ftp_login="",
        ftp_passwd="",
        ftp_directory="",
        upload_output_to_ckan=True,
        initialize_flows=True,
        create_warning_points=True,
        delete_output_when_done=True,
        mp_mode='htcondor',
        #mp_execute_directory='',
    )
```
###run_ecmwf_rapid_process Function Variables

|Variable|Data Type|Description|Default|
|---|:---:|---|:---:|
|*rapid_executable_location*|String|Path to RAPID executable.||
|*rapid_io_files_location*|String|Path to RAPID input/output directory.||
|*ecmwf_forecast_location*|String|Path to ECMWF forecasts.||
|*main_log_directory*|String|Path to store HTCondor/multiprocess logs.||
|*data_store_url*|String|(Optional) CKAN API url (e.g. http://your-ckan/api/3/action)|""|
|*data_store_api_key*|String|(Optional) CKAN API Key (e.g. abcd-1234-defr-3345)|""|
|*data_store_owner_org*|String|(Optional) CKAN owner organization (e.g. erdc).|""|
|*app_instance_id*|String|(Optional) Streamflow Prediction tool instance ID. |""|
|*sync_rapid_input_with_ckan*|Boolean|(Optional) If set to true, this will download ECMWF-RAPID input cooresponding to your instance of the Streamflow Prediction Tool. |False|
|*download_ecmwf*|Boolean|(Optional) If set to true, this will download the most recent ECMWF forecasts for today before runnning the process. |True|
|*date_string*|String|(Optional) This string will be used to modify the date of the forecasts downloaded and/or the forecasts ran. It is in the format yyyymmdd (e.g. 20160808). |None|
|*ftp_host*|String|(Optional) ECMWF ftp site path (e.g. ftp.ecmwf.int). |""|
|*ftp_login*|String|(Optional) ECMWF ftp login name. |""|
|*ftp_passwd*|String|(Optional) ECMWF ftp password. |""|
|*ftp_directory*|String|(Optional) ECMWF ftp directory. |""|
|*upload_output_to_ckan*|Boolean|(Optional) If true, this will upload the output to CKAN for the Streamflow Prediction Tool to download. |False|
|*delete_output_when_done*|String|(Optional) If true, all output will be deleted when the process completes. It is used when using operationally with *upload_output_to_ckan* set to true. |False|
|*initialize_flows*|String|(Optional) If true, this will initialize flows from all avaialble methods (e.g. Past forecasts, historical data, streamgage data). |False|
|*era_interim_data_location*|String|(Optional) Path to ERA Interim based historical streamflow, return period data, and seasonal average data. |""|
|*create_warning_points*|Boolean|(Optional) Generate waring points for Streamflow Prediction Tool. This requires return period data to be located in the *era_interim_data_location*. |False|
|*autoroute_executable_location*|String|(Optional|Beta) Path to AutoRoute executable. |""|
|*autoroute_io_files_location*|String|(Optional|Beta) Path to AutoRoute input/output directory. |""|
|*geoserver_url*|String|(Optional|Beta) Url to API endpoint ending in geoserver/rest. |""|
|*geoserver_username*|String|(Optional|Beta) Username for geoserver. |""|
|*geoserver_password*|String|(Optional|Beta) Password for geoserver. |""|
|*mp_mode*|String|(Optional) This defines how the process is run (HTCondor or Python's Multiprocessing). Valid options are htcondor and multiprocess. |htcondor|
|*mp_execute_directory*|String|(Optional|Required if using multiprocess mode) Directory used in multiprocessing mode to temporarily store files begin generated.  |""|

#### Possible run configurations
There are many different configurations. Here are some examples.

#####Mode 1: Run ECMWF-RAPID for Streamflow Prediction Tool using HTCondor to run and CKAN to upload
```python
run_ecmwf_rapid_process(
    rapid_executable_location='/home/alan/scripts/rapid/src/rapid',
    rapid_io_files_location='/home/alan/rapid-io',
    ecmwf_forecast_location ="/home/alan/ecmwf",
    era_interim_data_location="/home/alan/era_interim_watershed",
    subprocess_log_directory='/home/alan/subprocess_logs',
    main_log_directory='/home/alan/logs',
    data_store_url='http://your-ckan/api/3/action',
    data_store_api_key='your-ckan-api-key',
    data_store_owner_org="your-organization",
    app_instance_id='your-streamflow_prediction_tool-app-id',
    download_ecmwf=True,
    ftp_host="ftp.ecmwf",
    ftp_login="",
    ftp_passwd="",
    ftp_directory="",
    upload_output_to_ckan=True,
    initialize_flows=True,
    create_warning_points=True,
    delete_output_when_done=True,
)
```

#####Mode 2: Run ECMWF-RAPID for Streamflow Prediction Tool using HTCondor to run and CKAN to upload & to download model files 
```python
run_ecmwf_rapid_process(
    rapid_executable_location='/home/alan/scripts/rapid/src/rapid',
    rapid_io_files_location='/home/alan/rapid-io',
    ecmwf_forecast_location ="/home/alan/ecmwf",
    era_interim_data_location="/home/alan/era_interim_watershed",
    subprocess_log_directory='/home/alan/subprocess_logs',
    main_log_directory='/home/alan/logs',
    data_store_url='http://your-ckan/api/3/action',
    data_store_api_key='your-ckan-api-key',
    data_store_owner_org="your-organization",
    app_instance_id='your-streamflow_prediction_tool-app-id',
    sync_rapid_input_with_ckan=True,
    download_ecmwf=True,
    ftp_host="ftp.ecmwf",
    ftp_login="",
    ftp_passwd="",
    ftp_directory="",
    upload_output_to_ckan=True,
    initialize_flows=True,
    create_warning_points=True,
    delete_output_when_done=True,
)
```
#####Mode 3: Run ECMWF-RAPID for Streamflow Prediction Tool using Multiprocessing to run and CKAN to upload
```python
run_ecmwf_rapid_process(
    rapid_executable_location='/home/alan/scripts/rapid/src/rapid',
    rapid_io_files_location='/home/alan/rapid-io',
    ecmwf_forecast_location ="/home/alan/ecmwf",
    era_interim_data_location="/home/alan/era_interim_watershed",
    subprocess_log_directory='/home/alan/subprocess_logs', 
    main_log_directory='/home/alan/work/logs',
    data_store_url='http://your-ckan/api/3/action',
    data_store_api_key='your-ckan-api-key',
    data_store_owner_org="your-organization",
    app_instance_id='your-streamflow_prediction_tool-app-id',
    download_ecmwf=True,
    ftp_host="ftp.ecmwf",
    ftp_login="",
    ftp_passwd="",
    ftp_directory="",
    upload_output_to_ckan=True,
    initialize_flows=True,
    create_warning_points=True,
    delete_output_when_done=True,
    mp_mode='multiprocess',
    mp_execute_directory='/home/alan/mp_execute',
)
```
#####Mode 4: (BETA) Run ECMWF-RAPID for Streamflow Prediction Tool with AutoRoute using Multiprocessing to run
Note that in this example, CKAN was not used. However, you can still add CKAN back in to this example with the parameters shown in the previous examples.

```python
run_ecmwf_rapid_process(
    rapid_executable_location='/home/alan/rapid/src/rapid',
    rapid_io_files_location='/home/alan/rapid-io',
    ecmwf_forecast_location ="/home/alan/ecmwf",
    era_interim_data_location="/home/alan/era_interim_watershed",
    subprocess_log_directory='/home/alan/subprocess_logs/', #path to store HTCondor/multiprocess logs
    main_log_directory='/home/alan/logs/',
    download_ecmwf=True,
    ftp_host="ftp.ecmwf",
    ftp_login="",
    ftp_passwd="",
    ftp_directory="",
    upload_output_to_ckan=True,
    initialize_flows=True,
    create_warning_points=True,
    delete_output_when_done=False,
    autoroute_executable_location='/home/alan/scripts/AutoRoute/src/autoroute',
    autoroute_io_files_location='/home/alan/autoroute-io',
    geoserver_url='http://localhost:8181/geoserver/rest',
    geoserver_username='admin',
    geoserver_password='password',
    mp_mode='multiprocess',
    mp_execute_directory='/home/alan/mp_execute',
)
```

##Step 9: Make sure permissions are correct for these files and any directories the script will use

Example:
```
$ chmod u+x run_ecmwf_rapid.py
```

##Step 10: Add RAPID files to the work/rapid/input directory
To generate these files see: https://github.com/erdc-cm/RAPIDpy/wiki/GIS-Tools

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

##Step 11: Create CRON job to run the scripts hourly
To run this automatically, it is necessary to generate cron jobs to run the script. There are many ways to do this and two are presented here.

### Method 1: In terminal using crontab command
```
$ crontab -e
```
Then add:
```
45 5 * * * /usr/bin/python /path/to/run_ecmwf_rapid.py # ECMWF RAPID PROCESS
45 17 * * * /usr/bin/python /path/to/run_ecmwf_rapid.py # ECMWF RAPID PROCESS
``` 
Note: The time varies based on the time zone of your machine. The example here is for CT.

### Method 2: Use *create_cron.py* to create the CRON jobs:

1) Install crontab Python package.
```
$ pip install python-crontab
```
2) Create a script to initialize cron job *create_cron.py*. Change execution times based on your time zone (Note: It is CT in this example).

```python
        from spt_ecmwf_autorapid_process.setup import create_cron
        
        create_cron(execute_command='/usr/bin/python /path/to/run_ecmwf_rapid.py', 
                    job_1_start_hour=5,
                    job_1_start_minute=45,
                    job_2_start_hour=17,
                    job_2_start_minute=45)
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
