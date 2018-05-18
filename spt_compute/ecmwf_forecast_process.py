# -*- coding: utf-8 -*-
#
#  ecmwf_forecast_process.py
#  spt_compute
#
#  Created by Alan D. Snow.
#  Copyright © 2015-2016 Alan D Snow. All rights reserved.
#  License: BSD-3 Clause

import datetime
import time
from glob import glob
import json
from multiprocessing import Pool as mp_Pool
import os
from shutil import rmtree
import tarfile
from traceback import print_exc
import numpy as np
import pprint
from collections import OrderedDict
try:
    from condorpy import Job as CJob
    from condorpy import Templates as tmplt

    CONDOR_ENABLED = True
except ImportError:
    CONDOR_ENABLED = False
    pass
try:
    from spt_dataset_manager.dataset_manager import (ECMWFRAPIDDatasetManager,
                                                     RAPIDInputDatasetManager)

    SPT_DATASET_ENABLED = True
except ImportError:
    SPT_DATASET_ENABLED = False
    pass

# local imports
try:
    from .autorapid_process import run_autorapid_process

    AUTOROUTE_ENABLED = True
except ImportError:
    AUTOROUTE_ENABLED = False
    pass

from .process_lock import update_lock_info_file
from .imports.ftp_ecmwf_download import get_ftp_forecast_list, download_and_extract_ftp
from .imports.generate_warning_points import generate_ecmwf_warning_points
from .imports.helper_functions import (CaptureStdOutToLog,
                                       clean_logs,
                                       find_current_rapid_output,
                                       get_valid_watershed_list,
                                       get_datetime_from_date_timestep,
                                       get_datetime_from_forecast_folder,
                                       get_date_timestep_from_forecast_folder,
                                       get_ensemble_number_from_forecast,
                                       get_watershed_subbasin_from_folder, )
from .imports.ecmwf_rapid_multiprocess_worker import run_ecmwf_rapid_multiprocess_worker
from .imports.streamflow_assimilation import (compute_initial_rapid_flows,
                                              compute_seasonal_initial_rapid_flows_multicore_worker,
                                              update_inital_flows_usgs, )


# ----------------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------------------------
def upload_single_forecast(job_info, data_manager):
    """
    Uploads a single forecast file to CKAN
    """
    print("Uploading {0} {1} {2} {3}".format(job_info['watershed'],
                                             job_info['subbasin'],
                                             job_info['forecast_date_timestep'],
                                             job_info['ensemble_number']))

    # Upload to CKAN
    data_manager.initialize_run_ecmwf(job_info['watershed'], job_info['subbasin'], job_info['forecast_date_timestep'])
    data_manager.update_resource_ensemble_number(job_info['ensemble_number'])
    # upload file
    try:
        # tar.gz file
        output_tar_file = os.path.join(job_info['master_watershed_outflow_directory'],
                                       "%s.tar.gz" % data_manager.resource_name)
        if not os.path.exists(output_tar_file):
            with tarfile.open(output_tar_file, "w:gz") as tar:
                tar.add(job_info['outflow_file_name'], arcname=os.path.basename(job_info['outflow_file_name']))
        return_data = data_manager.upload_resource(output_tar_file)
        if not return_data['success']:
            print(return_data)
            print("Attempting to upload again")
            return_data = data_manager.upload_resource(output_tar_file)
            if not return_data['success']:
                print(return_data)
            else:
                print("Upload success")
        else:
            print("Upload success")
    except Exception as ex:
        print(ex)
        pass
    # remove tar.gz file
    os.remove(output_tar_file)


def analyize_subprocess_logs(subprocess_log_directory, forecast_date_timestep, watershed=None):
    watershed = watershed.replace('-', '_')
    subprocess_forecast_log_dir = os.path.join(subprocess_log_directory, forecast_date_timestep)
    log_files = glob(os.path.join(subprocess_forecast_log_dir,
                                  "job_{}_{}*.log".format(forecast_date_timestep, watershed)))

    times = list()
    for log_file in log_files:
        with open(log_file, 'r') as log:
            lines = log.readlines()
            time_components = lines[-1].split()[-1].split(':')
            time = float(time_components[0]) * 3600 + float(time_components[1]) * 60 + float(time_components[2])
            times.append(time)

    if times:

        return OrderedDict((
            ('forecast_date', forecast_date_timestep),
            ('max', str(max(times))),
            ('min', str(min(times))),
            ('mean', str(np.mean(times))),
            ('stdv', str(np.std(times))),
        ))


def timeit_with_log(func):
    def timed(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        delta_time = time.time() - start_time

        print('EXECUTION TIME: {} ran in {} seconds'.format(func.__name__, delta_time))

        return result
    return timed

# ----------------------------------------------------------------------------------------
# MAIN PROCESS
# ----------------------------------------------------------------------------------------


class ECMWFForecastProcessor(object):

    def __init__(
        self,
        rapid_executable_location,  # path to RAPID executable
        rapid_io_files_location,  # path ro RAPID input/output directory
        ecmwf_forecast_location,  # path to ECMWF forecasts
        subprocess_log_directory,  # path to store HTCondor/multiprocess logs
        main_log_directory,  # path to store main logs
        log_file_prepend="spt_compute_ecmwf_",
        region="",  # 1 of the 12 partitioned ECMWF files. Leave empty if using global,
        data_store_url="",  # CKAN API url
        data_store_api_key="",  # CKAN API Key,
        data_store_owner_org="",  # CKAN owner organization
        app_instance_id="",  # Streamflow Prediction tool instance ID
        sync_rapid_input_with_ckan=False,  # match Streamflow Prediciton tool RAPID input
        download_ecmwf=True,  # Download recent ECMWF forecast before running,
        date_string="",  # string of date of interest
        ftp_host="",  # ECMWF ftp site path
        ftp_login="",  # ECMWF ftp login name
        ftp_passwd="",  # ECMWF ftp password
        ftp_directory="",  # ECMWF ftp directory
        delete_past_ecmwf_forecasts=True,  # Deletes all past forecasts before next run
        upload_output_to_ckan=False,  # upload data to CKAN and remove local copy
        delete_output_when_done=False,  # delete all output data from this code
        initialize_flows=False,  # use forecast to initialize next run
        warning_flow_threshold=10,  # flows below this threshold will be ignored
        era_interim_data_location="",  # path to ERA Interim return period data
        create_warning_points=False,  # generate waring points for Streamflow Prediction Tool
        autoroute_executable_location="",  # location of AutoRoute executable
        autoroute_io_files_location="",  # path to AutoRoute input/outpuf directory
        geoserver_url="",  # url to API endpoint ending in geoserver/rest
        geoserver_username="",  # username for geoserver
        geoserver_password="",  # password for geoserver
        mp_mode='htcondor',  # valid options are htcondor and multiprocess,
        mp_execute_directory="",  # required if using multiprocess mode
    ):

        self.log_heading_manager = HeadingManager()

        # Step 1: Configure Logs
        self.time_begin_all = datetime.datetime.utcnow()
        self.log = None
        self.configure_logs(main_log_directory, log_file_prepend, subprocess_log_directory)

        # Step 2: Validate/Initialize
        self.validate_imports(mp_mode, mp_execute_directory, autoroute_executable_location, autoroute_io_files_location)

        self.rapid_io_files_location = rapid_io_files_location
        self.era_interim_data_location = era_interim_data_location
        self.region = region

        # Step 3: Read Lockfile
        self.LOCK_INFO_FILE = os.path.join(main_log_directory, "spt_compute_ecmwf_run_info_lock.txt")
        self.last_forecast_date = datetime.datetime.utcfromtimestamp(0)
        self.get_last_forecast_date_from_lockfile()

        # Step 4: Configure Data Manager
        self.data_manager = None
        if upload_output_to_ckan:
            self.configure_data_manager(
                upload_output_to_ckan,
                data_store_url,
                data_store_api_key,
                data_store_owner_org,
            )

        # Step 6: Download/Locate ECMWF Data
        self.ecmwf_folders = None
        self.get_ecmwf_folders(
            date_string,
            ecmwf_forecast_location,
            download_ecmwf,
            ftp_host, ftp_login,
            ftp_passwd,
            ftp_directory,
            delete_past_ecmwf_forecasts
        )

        # Step 7: Sync/Load Rapid Input Directories
        if sync_rapid_input_with_ckan:
            self.sync_rapid_input_with_ckan(
                app_instance_id,
                data_store_url,
                data_store_api_key,
            )

        self.rapid_input_directories = None
        self.get_rapid_input_directories()

        # Step 8: Configure Autoroute Manager
        self.autoroute_manager = None
        run_autoroute = self.configure_autoroute_manager(
            autoroute_executable_location,
            autoroute_io_files_location,
            rapid_io_files_location,
            geoserver_url,
            geoserver_username,
            geoserver_password,
            app_instance_id
        )

        # Step 8: Run Forecast Process
        self.run_ecmwf_forecast_process(
            rapid_executable_location,
            subprocess_log_directory,
            delete_output_when_done,
            initialize_flows,
            warning_flow_threshold,
            create_warning_points,
            run_autoroute,
            mp_mode,
            mp_execute_directory,
        )

        self.log_summary_stats()

        # Last Step: Close Log
        self.log.close()

    @timeit_with_log
    def run_ecmwf_forecast_process(self,
                                   rapid_executable_location,  # path to RAPID executable
                                   subprocess_log_directory,  # path to store HTCondor/multiprocess logs
                                   delete_output_when_done=False,  # delete all output data from this code
                                   initialize_flows=False,  # use forecast to initialize next run
                                   warning_flow_threshold=10,  # flows below this threshold will be ignored
                                   create_warning_points=False,  # generate waring points for Streamflow Prediction Tool
                                   run_autoroute=False,
                                   mp_mode='htcondor',  # valid options are htcondor and multiprocess,
                                   mp_execute_directory="",  # required if using multiprocess mode
                                   ):
        """
        This it the main ECMWF RAPID forecast process
        """

        self.log_heading('STARTING ECMWF RAPID FORCAST PROCESS')
        self.log_heading_manager.level += 1

        if not self.ecmwf_folders:
            self.log.INFO("No new forecasts found to run. Exiting ")
            return

        # GENERATE NEW LOCK INFO FILE
        self.update_lock_info_file()

        # Try/Except added for lock file
        try:
            # ADD SEASONAL INITIALIZATION WHERE APPLICABLE
            if initialize_flows:
                self.initialize_flows()
            # ----------------------------------------------------------------------
            # BEGIN ECMWF-RAPID FORECAST LOOP
            # ----------------------------------------------------------------------
            self.log_heading('STARTING ECMWF-RAPID FORECAST LOOP')
            self.log_heading_manager.level += 1
            master_job_info_list = []
            for ecmwf_folder in self.ecmwf_folders:

                ecmwf_forecasts = self.get_ecmwf_forecasts(ecmwf_folder)

                forecast_date_timestep = get_date_timestep_from_forecast_folder(ecmwf_folder)
                self.log_heading("Running ECMWF Forecast: {0}".format(forecast_date_timestep))
                self.log_heading_manager.level += 1

                # initialize HTCondor/multiprocess Logging Directory
                subprocess_forecast_log_dir = os.path.join(subprocess_log_directory, forecast_date_timestep)
                try:
                    os.makedirs(subprocess_forecast_log_dir)
                except OSError:
                    pass

                # submit jobs to downsize ecmwf files to watershed
                rapid_watershed_jobs = {}
                for rapid_input_directory in self.rapid_input_directories:
                    # keep list of jobs
                    rapid_watershed_jobs[rapid_input_directory] = {
                        'jobs': [],
                        'jobs_info': []
                    }
                    self.log.INFO("Preparing forecasts for: {0} {1}".format(rapid_input_directory,
                                                                  os.path.basename(ecmwf_folder)))

                    watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
                    master_watershed_input_directory = os.path.join(self.rapid_io_files_location, "input",
                                                                    rapid_input_directory)
                    master_watershed_outflow_directory = os.path.join(self.rapid_io_files_location, 'output',
                                                                      rapid_input_directory, forecast_date_timestep)
                    try:
                        os.makedirs(master_watershed_outflow_directory)
                    except OSError:
                        pass

                    # add USGS gage data to initialization file
                    if initialize_flows:
                        # update intial flows with usgs data
                        update_inital_flows_usgs(master_watershed_input_directory,
                                                 forecast_date_timestep)

                    # create jobs for HTCondor/multiprocess
                    for watershed_job_index, forecast in enumerate(ecmwf_forecasts):
                        ensemble_number = get_ensemble_number_from_forecast(forecast)

                        # get basin names
                        outflow_file_name = 'Qout_%s_%s_%s.nc' % (watershed.lower(), subbasin.lower(), ensemble_number)
                        node_rapid_outflow_file = outflow_file_name
                        master_rapid_outflow_file = os.path.join(master_watershed_outflow_directory, outflow_file_name)

                        job_name = 'job_%s_%s_%s_%s' % (forecast_date_timestep, watershed, subbasin, ensemble_number)

                        rapid_watershed_jobs[rapid_input_directory]['jobs_info'].append({'watershed': watershed,
                                                                                         'subbasin': subbasin,
                                                                                         'outflow_file_name': master_rapid_outflow_file,
                                                                                         'forecast_date_timestep': forecast_date_timestep,
                                                                                         'ensemble_number': ensemble_number,
                                                                                         'master_watershed_outflow_directory': master_watershed_outflow_directory,
                                                                                         })
                        if mp_mode == "htcondor":
                            # create job to downscale forecasts for watershed
                            LOCAL_SCRIPTS_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
                            job = CJob(job_name, tmplt.vanilla_transfer_files)
                            job.set('executable', os.path.join(LOCAL_SCRIPTS_DIRECTORY, 'htcondor_ecmwf_rapid.py'))
                            job.set('transfer_input_files', "%s, %s, %s" % (
                            forecast, master_watershed_input_directory, LOCAL_SCRIPTS_DIRECTORY))
                            job.set('initialdir', subprocess_forecast_log_dir)
                            job.set('arguments', '%s %s %s %s %s %s' % (
                            forecast, forecast_date_timestep, watershed.lower(), subbasin.lower(),
                            rapid_executable_location, initialize_flows))
                            job.set('transfer_output_remaps',
                                    "\"%s = %s\"" % (node_rapid_outflow_file, master_rapid_outflow_file))
                            job.submit()
                            rapid_watershed_jobs[rapid_input_directory]['jobs'].append(job)
                        elif mp_mode == "multiprocess":
                            rapid_watershed_jobs[rapid_input_directory]['jobs'].append((forecast,
                                                                                        forecast_date_timestep,
                                                                                        watershed.lower(),
                                                                                        subbasin.lower(),
                                                                                        rapid_executable_location,
                                                                                        initialize_flows,
                                                                                        job_name,
                                                                                        master_rapid_outflow_file,
                                                                                        master_watershed_input_directory,
                                                                                        mp_execute_directory,
                                                                                        subprocess_forecast_log_dir,
                                                                                        watershed_job_index))
                            # COMMENTED CODE FOR DEBUGGING SERIALLY
                            ##                    run_ecmwf_rapid_multiprocess_worker((forecast,
                            ##                                                         forecast_date_timestep,
                            ##                                                         watershed.lower(),
                            ##                                                         subbasin.lower(),
                            ##                                                         rapid_executable_location,
                            ##                                                         initialize_flows,
                            ##                                                         job_name,
                            ##                                                         master_rapid_outflow_file,
                            ##                                                         master_watershed_input_directory,
                            ##                                                         mp_execute_directory,
                            ##                                                         subprocess_forecast_log_dir,
                            ##                                                         watershed_job_index))
                        else:
                            raise Exception("ERROR: Invalid mp_mode. Valid types are htcondor and multiprocess ")

                for rapid_input_directory, watershed_job_info in rapid_watershed_jobs.items():
                    self.log_heading('SUBMITTING PROCESSING JOBS FOR {}'.format(rapid_input_directory))
                    self.log_heading_manager.level += 1
                    job_pool_start_time = datetime.datetime.utcnow()
                    # add sub job list to master job list
                    master_job_info_list = master_job_info_list + watershed_job_info['jobs_info']
                    if mp_mode == "htcondor":
                        # wait for jobs to finish then upload files
                        for job_index, job in enumerate(watershed_job_info['jobs']):
                            job.wait()
                            # upload file when done
                            if self.data_manager:
                                upload_single_forecast(watershed_job_info['jobs_info'][job_index], self.data_manager)

                    elif mp_mode == "multiprocess":
                        pool_main = mp_Pool()
                        multiprocess_worker_list = pool_main.imap_unordered(run_ecmwf_rapid_multiprocess_worker,
                                                                            watershed_job_info['jobs'],
                                                                            chunksize=1)
                        if self.data_manager:
                            for multi_job_index in multiprocess_worker_list:
                                # upload file when done
                                upload_single_forecast(watershed_job_info['jobs_info'][multi_job_index], self.data_manager)

                        # just in case
                        pool_main.close()
                        pool_main.join()

                    # process summary stats
                    stats = analyize_subprocess_logs(subprocess_log_directory, forecast_date_timestep,
                                                     rapid_input_directory)
                    stats['total'] = str(datetime.datetime.utcnow() - job_pool_start_time)

                    stats_report = '\n\t'.join(['{}: {}'.format(k, v) for k, v in stats.items()])
                    self.log.INFO('Process time stats for {}:\n\t{}'.format(rapid_input_directory, stats_report))
                    stats_file_path = os.path.join(self.rapid_io_files_location, "input",
                                                   rapid_input_directory, 'processing_time_summary_stats.csv')
                    with open(stats_file_path, 'a') as stats_file:
                        stats_file.write(','.join(stats.values()) + '\n')

                    # when all jobs in watershed are done, generate warning points
                    if create_warning_points:
                        self.calculate_warning_points(
                            rapid_input_directory,
                            forecast_date_timestep,
                            warning_flow_threshold
                        )

                    self.log_heading_manager.level -= 1

                # initialize flows for next run
                if initialize_flows:
                    self.generate_initial_flows(forecast_date_timestep)

                # run autoroute process if added
                if run_autoroute:
                    self.run_autoroute(forecast_date_timestep, subprocess_forecast_log_dir)

                self.last_forecast_date = get_datetime_from_date_timestep(forecast_date_timestep)

                # update lock info file with next forecast
                self.update_lock_info_file()

                self.log_heading_manager.level -= 1

                # ----------------------------------------------------------------------
                # END FORECAST LOOP
                # ----------------------------------------------------------------------
        except Exception as ex:
            print_exc()
            self.log.ERROR(ex)

        self.log_heading_manager.level -= 1
        # Release & update lock info file with all completed forecasts
        self.log_heading('UPDATING LOCKFILE')
        self.update_lock_info_file(is_running=False)
        self.log.INFO('  Last forecast time: {}'.format(self.last_forecast_date.strftime('%Y%m%d%H')))

        if delete_output_when_done:
            self.log_heading('DELETING OUTPUT')
            # delete local datasets
            for job_info in master_job_info_list:
                try:
                    rmtree(job_info['master_watershed_outflow_directory'])
                except OSError:
                    pass
            # delete watershed folder if empty
            for item in os.listdir(os.path.join(self.rapid_io_files_location, 'output')):
                try:
                    os.rmdir(os.path.join(self.rapid_io_files_location, 'output', item))
                except OSError:
                    pass

        self.log_heading_manager.level -= 1

    def validate_imports(self, mp_mode, mp_execute_directory,
                         autoroute_executable_location, autoroute_io_files_location):
        if mp_mode == "multiprocess":
            if not mp_execute_directory or not os.path.exists(mp_execute_directory):
                raise Exception("If mode is multiprocess, mp_execute_directory is required ")
            elif mp_mode == 'htcondor' and not CONDOR_ENABLED:
                raise ImportError("condorpy is not installed. Please install condorpy to use the 'htcondor' option.")

        if not AUTOROUTE_ENABLED and autoroute_executable_location and autoroute_io_files_location:
            raise ImportError("AutoRoute is not enabled. Please install tethys_dataset_services"
                              " and AutoRoutePy to use the AutoRoute option.")

    def get_last_forecast_date_from_lockfile(self):
        # LOAD LOCK INFO FILE
        self.log_heading('READING INFO FROM LOCKFILE')
        if os.path.exists(self.LOCK_INFO_FILE):
            with open(self.LOCK_INFO_FILE) as fp_lock_info:
                previous_lock_info = json.load(fp_lock_info)

            if previous_lock_info['running']:
                self.log.INFO("Another SPT ECMWF forecast process is running.\n"
                      "The lock file is located here: {0}\n"
                      "If this is an error, you have two options:\n"
                      "1) Delete the lock file.\n"
                      "2) Edit the lock file and set \"running\" to false. \n"
                      "Then, re-run this script. \n Exiting ".format(self.LOCK_INFO_FILE))
                return
            else:
                self.last_forecast_date = datetime.datetime.strptime(previous_lock_info['last_forecast_date'], '%Y%m%d%H')

    def configure_data_manager(self, data_store_url, data_store_api_key, data_store_owner_org):
        if data_store_url and data_store_api_key:
            self.log_heading('CONFIGURING CKAN DATASET MANAGER')
            if not SPT_DATASET_ENABLED:
                raise ImportError("spt_dataset_manager is not installed. "
                                  "Please install spt_dataset_manager to use the 'ckan' options.")

            # init data manager for CKAN
            self.data_manager = ECMWFRAPIDDatasetManager(
                data_store_url,
                data_store_api_key,
                data_store_owner_org
            )

    def configure_autoroute_manager(self,
                                    autoroute_executable_location,
                                    autoroute_io_files_location,
                                    rapid_io_files_location,
                                    geoserver_url,
                                    geoserver_username,
                                    geoserver_password,
                                    app_instance_id):

        if autoroute_executable_location and autoroute_executable_location:
            self.autoroute_manager = AutorouteManager(
                autoroute_executable_location,
                autoroute_io_files_location,
                rapid_io_files_location,
                geoserver_url,
                geoserver_username,
                geoserver_password,
                app_instance_id
            )
            return True
        return False

    @timeit_with_log
    def sync_rapid_input_with_ckan(self, app_instance_id, data_store_url, data_store_api_key):
        if app_instance_id and data_store_url and data_store_api_key:
            self.log_heading('Syncing RAPID input with CKAN')
            # sync with data store
            ri_manager = RAPIDInputDatasetManager(data_store_url,
                                                  data_store_api_key,
                                                  'ecmwf',
                                                  app_instance_id)
            ri_manager.sync_dataset(os.path.join(self.rapid_io_files_location, 'input'))

    @timeit_with_log
    def get_rapid_input_directories(self):
        # get list of correclty formatted rapid input directories in rapid directory
        self.log_heading('GETTING LIST OF VALID RAPID INPUT DIRECTORIES')
        self.rapid_input_directories = get_valid_watershed_list(os.path.join(self.rapid_io_files_location, "input"))
        self.log.DEBUG('  found the following valid input directories: \n{}'
                       .format(pprint.pformat(self.rapid_input_directories)))

    @timeit_with_log
    def get_ecmwf_folders(self, date_string, ecmwf_forecast_location, download_ecmwf,
                          ftp_host, ftp_login, ftp_passwd, ftp_directory, delete_past_ecmwf_forecasts):
        self.log_heading('LOADING ECMWF FORECASTS')
        if download_ecmwf and ftp_host:
            # get list of folders to download
            temp_ecmwf_folders = sorted(get_ftp_forecast_list('Runoff.{}*{}*.netcdf.tar*'.format(date_string, self.region),
                                                         ftp_host,
                                                         ftp_login,
                                                         ftp_passwd,
                                                         ftp_directory))
            downloaded_ecmwf_folders = []
            for ecmwf_folder in temp_ecmwf_folders:
                # get date
                forecast_date = get_datetime_from_forecast_folder(ecmwf_folder)
                # if more recent, add to list
                if forecast_date > self.last_forecast_date:
                    self.log.INFO('INFO: Downloading ecmwf data for {}'.format(ecmwf_folder))
                    # download forecast
                    ecmwf_folder = download_and_extract_ftp(ecmwf_forecast_location, ecmwf_folder,
                                                            ftp_host, ftp_login,
                                                            ftp_passwd, ftp_directory,
                                                            delete_past_ecmwf_forecasts)
                    downloaded_ecmwf_folders.append(ecmwf_folder)
            self.ecmwf_folders = downloaded_ecmwf_folders
        else:
            # get list of folders to run
            self.ecmwf_folders = sorted(glob(os.path.join(ecmwf_forecast_location,
                                                     'Runoff.' + date_string + '*.netcdf')))
            self.log.DEBUG('loaded the following previously downloaded forecasts:\n{}'
                           .format(pprint.pformat(self.ecmwf_folders)))

    @timeit_with_log
    def initialize_flows(self):
        self.log_heading('INITIALIZING FLOWS')
        initial_forecast_date_timestep = get_date_timestep_from_forecast_folder(self.ecmwf_folders[0])
        seasonal_init_job_list = []
        for rapid_input_directory in self.rapid_input_directories:
            seasonal_master_watershed_input_directory = os.path.join(self.rapid_io_files_location, "input",
                                                                     rapid_input_directory)
            # add seasonal initialization if no initialization file and historical Qout file exists
            if self.era_interim_data_location and os.path.exists(self.era_interim_data_location):
                era_interim_watershed_directory = os.path.join(self.era_interim_data_location, rapid_input_directory)
                if os.path.exists(era_interim_watershed_directory):
                    # INITIALIZE FROM SEASONAL AVERAGE FILE
                    seasonal_streamflow_file = glob(
                        os.path.join(era_interim_watershed_directory, "seasonal_average*.nc"))
                    if seasonal_streamflow_file:
                        seasonal_init_job_list.append((seasonal_streamflow_file[0],
                                                       seasonal_master_watershed_input_directory,
                                                       initial_forecast_date_timestep,
                                                       "seasonal_average_file"))
                    else:
                        # INITIALIZE FROM HISTORICAL STREAMFLOW FILE
                        historical_qout_file = glob(os.path.join(era_interim_watershed_directory, "Qout*.nc"))
                        if historical_qout_file:
                            seasonal_init_job_list.append((historical_qout_file[0],
                                                           seasonal_master_watershed_input_directory,
                                                           initial_forecast_date_timestep,
                                                           "historical_streamflow_file"))
        if seasonal_init_job_list:
            # use multiprocessing instead of htcondor due to potential for huge file sizes
            if len(seasonal_init_job_list) > 1:
                seasonal_pool = mp_Pool()
                seasonal_pool.imap(compute_seasonal_initial_rapid_flows_multicore_worker,
                                   seasonal_init_job_list,
                                   chunksize=1)
                seasonal_pool.close()
                seasonal_pool.join()
            else:
                compute_seasonal_initial_rapid_flows_multicore_worker(seasonal_init_job_list[0])

    @timeit_with_log
    def get_ecmwf_forecasts(self, ecmwf_folder):
        # get list of forecast files
        ecmwf_forecasts = glob(os.path.join(ecmwf_folder, '*.runoff.%s*nc' % self.region))

        # look for old version of forecasts
        if not ecmwf_forecasts:
            ecmwf_forecasts = glob(os.path.join(ecmwf_folder, 'full_*.runoff.netcdf')) + \
                              glob(os.path.join(ecmwf_folder, '*.52.205.*.runoff.netcdf'))

        if not ecmwf_forecasts:
            self.log.ERROR("ERROR: Forecasts not found in folder. Exiting ")
            self.exit(1)

        # make the largest files first
        ecmwf_forecasts.sort(key=os.path.getsize, reverse=True)

        return ecmwf_forecasts

    @timeit_with_log
    def generate_initial_flows(self, forecast_date_timestep):
        self.log_heading('INITIALIZING FLOWS FOR NEXT RUN')
        # create new init flow files/generate warning point files
        for rapid_input_directory in self.rapid_input_directories:
            input_directory = os.path.join(self.rapid_io_files_location,
                                           'input',
                                           rapid_input_directory)
            forecast_directory = os.path.join(self.rapid_io_files_location,
                                              'output',
                                              rapid_input_directory,
                                              forecast_date_timestep)
            if os.path.exists(forecast_directory):
                # loop through all the rapid_namelist files in directory
                watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
                self.log.INFO("Initializing flows for {0}-{1} from {2}".format(watershed, subbasin,
                                                                               forecast_date_timestep))
                basin_files = find_current_rapid_output(forecast_directory, watershed, subbasin)
                try:
                    compute_initial_rapid_flows(basin_files, input_directory, forecast_date_timestep)
                except Exception as ex:
                    self.log.ERROR(ex)
                    pass

    @timeit_with_log
    def calculate_warning_points(self, rapid_input_directory, forecast_date_timestep, warning_flow_threshold):
        self.log_heading('CREATING WARNING POINTS')
        watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
        forecast_directory = os.path.join(self.rapid_io_files_location,
                                          'output',
                                          rapid_input_directory,
                                          forecast_date_timestep)

        era_interim_watershed_directory = os.path.join(self.era_interim_data_location, rapid_input_directory)
        if os.path.exists(era_interim_watershed_directory):
            self.log.INFO("Generating warning points for {0}-{1} from {2}".format(watershed, subbasin,
                                                                          forecast_date_timestep))
            era_interim_files = glob(os.path.join(era_interim_watershed_directory, "return_period*.nc"))
            if era_interim_files:
                try:
                    generate_ecmwf_warning_points(forecast_directory, era_interim_files[0],
                                                  forecast_directory, threshold=warning_flow_threshold)
                    if self.data_manager is not None:
                        self.data_manager.initialize_run_ecmwf(watershed, subbasin, forecast_date_timestep)
                        self.data_manager.zip_upload_warning_points_in_directory(forecast_directory)
                except Exception as ex:
                    self.log.ERROR(ex)
                    pass
            else:
                self.log.INFO("No ERA Interim file found. Skipping ")
        else:
            self.log.INFO("No ERA Interim directory found for {0}. "
                          "Skipping warning point generation".format(rapid_input_directory))

    @timeit_with_log
    def run_autoroute(self, forecast_date_timestep, subprocess_forecast_log_dir):
        self.log_heading('RUNNING AUTOROUTE')
        if self.autoroute_manager:
            self.autoroute_manager.run_autoroute(forecast_date_timestep, subprocess_forecast_log_dir)

    def log_summary_stats(self):
        # print info to user
        self.log_heading('SUMMARY STATS')
        self.log.INFO('Number of forecasts processed: {}'.format(len(self.ecmwf_folders)))
        self.log.INFO('Number of watersheds processed: {}'.format(len(self.rapid_input_directories)))
        time_end = datetime.datetime.utcnow()
        self.log.INFO("Time Begin: {0}".format(self.time_begin_all))
        self.log.INFO("Time Finish: {0}".format(time_end))
        self.log.INFO("TOTAL TIME: {0}".format(time_end - self.time_begin_all))

    def log_heading(self, title):
        heading = self.log_heading_manager.next_heading(title)
        self.log.INFO(heading)

    def configure_logs(self, main_log_directory, log_file_prepend, subprocess_log_directory):
        log_file_path = os.path.join(
            main_log_directory,
            "{0}{1}.log".format(log_file_prepend, self.time_begin_all.strftime("%y%m%d%H%M%S")))
        self.log = STDOutLogger(log_file_path)

        # clean up old log files
        self.log_heading('CLEANING LOGS')
        clean_logs(subprocess_log_directory, main_log_directory, prepend=log_file_prepend, log_file_path=log_file_path)

    def update_lock_info_file(self, is_running=True):
        update_lock_info_file(self.LOCK_INFO_FILE, is_running, self.last_forecast_date.strftime('%Y%m%d%H'))

    def exit(self, code=0):
        self.update_lock_info_file(is_running=False)
        self.log.close()
        exit(code)


class AutorouteManager(object):
    def __init__(self,
                 autoroute_executable_location,
                 autoroute_io_files_location,
                 rapid_io_files_location,
                 geoserver_url,
                 geoserver_username,
                 geoserver_password,
                 app_instance_id):
        self.autoroute_executable_location = autoroute_executable_location
        self.autoroute_io_files_location = autoroute_io_files_location
        self.rapid_io_files_location = rapid_io_files_location
        self.geoserver_url = geoserver_url
        self.geoserver_username = geoserver_username
        self.geoserver_password = geoserver_password
        self.app_instance_id = app_instance_id

    def run_autoroute(self, forecast_date_timestep, subprocess_forecast_log_dir):
        # run autoroute on all of the watersheds
        run_autorapid_process(self.autoroute_executable_location,
                              self.autoroute_io_files_location,
                              self.rapid_io_files_location,
                              forecast_date_timestep,
                              subprocess_forecast_log_dir,
                              self.geoserver_url,
                              self.geoserver_username,
                              self.geoserver_password,
                              self.app_instance_id)


class STDOutLogger(object):
    def __init__(self, log_file_path):
        self.log = CaptureStdOutToLog(log_file_path)
        self.log.__enter__()

    def DEBUG(self, msg):
        print(msg)

    def INFO(self, msg):
        print(msg)

    def WARN(self, msg):
        print(msg)

    def ERROR(self, msg):
        print(msg)

    def close(self):
        self.log.__exit__()
        pass


class HeadingManager(object):

    def __init__(self):
        self.steps = [0]
        self.level = 1
        
    def set_level(self, level):
        self.level = level

    def heading_template(self, level=None):
        level = level or self.level
        if level == 1:
            return '\n\n***************  {step} - {title} ***************\n'
        return '\n{}*  {} - {}\n'.format('  ' * level, '{step}', '{title}')

    def get_step(self, level=None):
        level = level or self.level
        if len(self.steps) < level:
            self.steps.extend([0] * (level - len(self.steps)))

        self.steps = self.steps[:level]
        self.steps[level - 1] += 1

        return '.'.join(map(str, self.steps[:level]))

    def next_heading(self, title, level=None):
        level = level or self.level
        step = self.get_step(level)
        template = self.heading_template(level)
        return template.format(level=level, step=step, title=title)
