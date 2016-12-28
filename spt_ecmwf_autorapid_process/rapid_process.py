# -*- coding: utf-8 -*-
#
#  rapid_process.py
#  spt_ecmwf_autorapid_process
#
#  Created by Alan D. Snow.
#  License: BSD-3 Clause

import datetime
from glob import glob
import json
from multiprocessing import Pool as mp_Pool
import os
from shutil import rmtree
import tarfile
from traceback import print_exc

try:
    from condorpy import Job as CJob
    from condorpy import Templates as tmplt
    CONDOR_ENABLED = True
except ImportError:
    CONDOR_ENABLED = False    
    pass

# local imports
try:
    from .autorapid_process import run_autorapid_process
    AUTOROUTE_ENABLED = True
except ImportError:
    AUTOROUTE_ENABLED = False
    pass

from .imports.ftp_ecmwf_download import get_ftp_forecast_list, download_and_extract_ftp
from .imports.generate_warning_points import generate_warning_points
from .imports.helper_functions import (CaptureStdOutToLog,
                                       clean_logs,
                                       find_current_rapid_output,
                                       get_valid_watershed_list,
                                       get_date_timestep_from_forecast_folder,
                                       get_datetime_from_date_timestep,
                                       get_ensemble_number_from_forecast,
                                       get_watershed_subbasin_from_folder,)
from .imports.ecmwf_rapid_multiprocess_worker import run_ecmwf_rapid_multiprocess_worker                                      
from .imports.streamflow_assimilation import (compute_initial_rapid_flows,
                                              compute_seasonal_initial_rapid_flows_multicore_worker,
                                              update_inital_flows_usgs,)
# package imports
from spt_dataset_manager.dataset_manager import (ECMWFRAPIDDatasetManager,
                                                 RAPIDInputDatasetManager)


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
        output_tar_file =  os.path.join(job_info['master_watershed_outflow_directory'],
                                        "{0}.tar.gz".format(data_manager.resource_name))
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


def update_lock_info_file(lock_info_file_path, currently_running, last_forecast_date):
    """
    This function updates the lock info file
    """
    with open(lock_info_file_path, "w") as fp_lock_info:
        lock_info_data = { 
                            'running' : currently_running,
                            'last_forecast_date' : last_forecast_date,
                         }
        json.dump(lock_info_data, fp_lock_info)


def reset_lock_info_file(lock_info_file_path):
    """
    This function removes lock in file if the file exists.
    The purpose is for reboot of computer
    """                                  
    if os.path.exists(lock_info_file_path):
        
        # read in last forecast date
        with open(lock_info_file_path) as fp_lock_info:
            previous_lock_info = json.load(fp_lock_info)
            last_forecast_date_str = previous_lock_info['last_forecast_date']
            
        # update lock to false
        update_lock_info_file(lock_info_file_path, False, last_forecast_date_str)     


# ----------------------------------------------------------------------------------------
# MAIN PROCESS
# ----------------------------------------------------------------------------------------
def run_ecmwf_rapid_process(rapid_executable_location,  # path to RAPID executable
                            rapid_io_files_location,  # path ro RAPID input/output directory
                            ecmwf_forecast_location,  # path to ECMWF forecasts
                            subprocess_log_directory,  # path to store HTCondor/multiprocess logs
                            main_log_directory,  # path to store main logs
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
    """
    This it the main ECMWF RAPID process
    """
    time_begin_all = datetime.datetime.utcnow()

    LOCAL_SCRIPTS_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
    LOCK_INFO_FILE = os.path.join(main_log_directory, "ecmwf_rapid_run_info_lock.txt")
    
    log_file_path = os.path.join(main_log_directory, 
                                 "rapid_{0}.log".format(time_begin_all.strftime("%y%m%d%H%M%S")))

    with CaptureStdOutToLog(log_file_path):
        
        if not CONDOR_ENABLED and mp_mode == 'htcondor':
            raise ImportError("condorpy is not installed. Please install condorpy to use the \'htcondor\' option.")
            
        if not AUTOROUTE_ENABLED and autoroute_executable_location and autoroute_io_files_location:
            raise ImportError("AutoRoute is not enabled. Please install tethys_dataset_services"
                              " and AutoRoutePy to use the AutoRoute option.")
        
        if mp_mode == "multiprocess":
            if not mp_execute_directory or not os.path.exists(mp_execute_directory):
                raise Exception("If mode is multiprocess, mp_execute_directory is required ...")
                
        if sync_rapid_input_with_ckan and app_instance_id and data_store_url and data_store_api_key:
            # sync with data store
            ri_manager = RAPIDInputDatasetManager(data_store_url,
                                                  data_store_api_key,
                                                  'ecmwf',
                                                  app_instance_id)
            ri_manager.sync_dataset(os.path.join(rapid_io_files_location,'input'))

        # clean up old log files
        clean_logs(subprocess_log_directory, main_log_directory, log_file_path=log_file_path)

        data_manager = None
        if upload_output_to_ckan and data_store_url and data_store_api_key:
            # init data manager for CKAN
            data_manager = ECMWFRAPIDDatasetManager(data_store_url,
                                                    data_store_api_key,
                                                    data_store_owner_org)

        # get list of correclty formatted rapid input directories in rapid directory
        rapid_input_directories = get_valid_watershed_list(os.path.join(rapid_io_files_location, "input"))
        
        if download_ecmwf and ftp_host:
            # get list of folders to download
            ecmwf_folders = sorted(get_ftp_forecast_list('Runoff.%s*.netcdf.tar*' % date_string,
                                                         ftp_host,
                                                         ftp_login,
                                                         ftp_passwd,
                                                         ftp_directory))
        else:
            # get list of folders to run
            ecmwf_folders = sorted(glob(os.path.join(ecmwf_forecast_location,
                                                     'Runoff.'+date_string+'*.netcdf')))

        # LOAD LOCK INFO FILE
        last_forecast_date = datetime.datetime.utcfromtimestamp(0)
        if os.path.exists(LOCK_INFO_FILE):
            with open(LOCK_INFO_FILE) as fp_lock_info:
                previous_lock_info = json.load(fp_lock_info)
            
            if previous_lock_info['running']:
                print("Another ECMWF-RAPID process is running.\n"
                      "The lock file is located here: {0}\n"
                      "If this is an error, you have two options:\n"
                      "1) Delete the lock file.\n"
                      "2) Edit the lock file and set \"running\" to false. \n"
                      "Then, re-run this script. \n Exiting ...".format(LOCK_INFO_FILE))
                return
            else:
                last_forecast_date = datetime.datetime.strptime(previous_lock_info['last_forecast_date'], '%Y%m%d%H')
                run_ecmwf_folders = []
                for ecmwf_folder in ecmwf_folders:
                    # get date
                    forecast_date_timestep = get_date_timestep_from_forecast_folder(ecmwf_folder)
                    forecast_date = get_datetime_from_date_timestep(forecast_date_timestep)
                    # if more recent, add to list
                    if forecast_date > last_forecast_date:
                        run_ecmwf_folders.append(ecmwf_folder)
                        
                ecmwf_folders = run_ecmwf_folders
                
        if not ecmwf_folders:
            print("No new forecasts found to run. Exiting ...")
            return
                
        # GENERATE NEW LOCK INFO FILE
        update_lock_info_file(LOCK_INFO_FILE, True, last_forecast_date.strftime('%Y%m%d%H'))

        # Try/Except added for lock file
        try:
            # ADD SEASONAL INITIALIZATION WHERE APPLICABLE
            if initialize_flows:
                initial_forecast_date_timestep = get_date_timestep_from_forecast_folder(ecmwf_folders[0])
                seasonal_init_job_list = []
                for rapid_input_directory in rapid_input_directories:
                    seasonal_master_watershed_input_directory = os.path.join(rapid_io_files_location,
                                                                             "input", rapid_input_directory)
                    # add seasonal initialization if no initialization file and historical Qout file exists
                    if era_interim_data_location and os.path.exists(era_interim_data_location):
                        era_interim_watershed_directory = os.path.join(era_interim_data_location, rapid_input_directory)
                        if os.path.exists(era_interim_watershed_directory):
                            # INITIALIZE FROM SEASONAL AVERAGE FILE
                            seasonal_streamflow_file = glob(os.path.join(era_interim_watershed_directory,
                                                                         "seasonal_average*.nc"))
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
            # ----------------------------------------------------------------------
            # BEGIN ECMWF-RAPID FORECAST LOOP
            # ----------------------------------------------------------------------
            master_job_info_list = []
            for ecmwf_folder in ecmwf_folders:
                if download_ecmwf:
                    # download forecast
                    ecmwf_folder = download_and_extract_ftp(ecmwf_forecast_location, ecmwf_folder, 
                                                            ftp_host, ftp_login, 
                                                            ftp_passwd, ftp_directory,
                                                            delete_past_ecmwf_forecasts)

                # get list of forecast files
                ecmwf_forecasts = glob(os.path.join(ecmwf_folder, '*.runoff.nc'))
                                  
                # look for old version of forecasts
                if not ecmwf_forecasts:
                    ecmwf_forecasts = glob(os.path.join(ecmwf_folder, 'full_*.runoff.netcdf')) + \
                                      glob(os.path.join(ecmwf_folder, '*.52.205.*.runoff.netcdf'))
                
                if not ecmwf_forecasts:
                    print("ERROR: Forecasts not found in folder. Exiting ...")
                    update_lock_info_file(LOCK_INFO_FILE, False, last_forecast_date.strftime('%Y%m%d%H'))
                    return
                    
                # make the largest files first
                ecmwf_forecasts.sort(key=os.path.getsize, reverse=True)
    
                forecast_date_timestep = get_date_timestep_from_forecast_folder(ecmwf_folder)
                print("Running ECMWF Forecast: {0}".format(forecast_date_timestep))

                # submit jobs to downsize ecmwf files to watershed
                rapid_watershed_jobs = {}
                for rapid_input_directory in rapid_input_directories:
                    # keep list of jobs
                    rapid_watershed_jobs[rapid_input_directory] = {
                                                                    'jobs': [], 
                                                                    'jobs_info': []
                                                                   }
                    print("Running forecasts for: {0} {1}".format(rapid_input_directory, 
                                                                  os.path.basename(ecmwf_folder)))
                                                                  
                    watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
                    master_watershed_input_directory = os.path.join(rapid_io_files_location, "input",
                                                                    rapid_input_directory)
                    master_watershed_outflow_directory = os.path.join(rapid_io_files_location, 'output',
                                                                      rapid_input_directory, forecast_date_timestep)
                    try:
                        os.makedirs(master_watershed_outflow_directory)
                    except OSError:
                        pass

                    # initialize HTCondor/multiprocess Logging Directory
                    subprocess_forecast_log_dir = os.path.join(subprocess_log_directory, forecast_date_timestep)
                    try:
                        os.makedirs(subprocess_forecast_log_dir)
                    except OSError:
                        pass

                    # add USGS gage data to initialization file
                    if initialize_flows:
                        # update initial flows with usgs data
                        update_inital_flows_usgs(master_watershed_input_directory, 
                                                 forecast_date_timestep)
                    
                    # create jobs for HTCondor/multiprocess
                    for watershed_job_index, forecast in enumerate(ecmwf_forecasts):
                        ensemble_number = get_ensemble_number_from_forecast(forecast)
                        
                        # get basin names
                        outflow_file_name = 'Qout_{0}_{1}_{2}.nc'.format(watershed.lower(),
                                                                         subbasin.lower(),
                                                                         ensemble_number)
                        node_rapid_outflow_file = outflow_file_name
                        master_rapid_outflow_file = os.path.join(master_watershed_outflow_directory, outflow_file_name)
    
                        job_name = 'job_{0}_{1}_{2}_{3}'.format(forecast_date_timestep, watershed,
                                                                subbasin, ensemble_number)
    
                        rapid_watershed_jobs[rapid_input_directory]['jobs_info'] \
                            .append({'watershed': watershed,
                                     'subbasin': subbasin,
                                     'outflow_file_name': master_rapid_outflow_file,
                                     'forecast_date_timestep': forecast_date_timestep,
                                     'ensemble_number': ensemble_number,
                                     'master_watershed_outflow_directory': master_watershed_outflow_directory,
                                     })

                        if mp_mode == "htcondor":
                            # create job to downscale forecasts for watershed
                            job = CJob(job_name, tmplt.vanilla_transfer_files)
                            job.set('executable', os.path.join(LOCAL_SCRIPTS_DIRECTORY,'htcondor_ecmwf_rapid.py'))
                            job.set('transfer_input_files', "{0}, {1}, {2}".format(forecast,
                                                                                   master_watershed_input_directory,
                                                                                   LOCAL_SCRIPTS_DIRECTORY))
                            job.set('initialdir', subprocess_forecast_log_dir)
                            job.set('arguments', '{0} {1} {2} {3} {4} {5}'.format(forecast,
                                                                                  forecast_date_timestep,
                                                                                  watershed.lower(),
                                                                                  subbasin.lower(),
                                                                                  rapid_executable_location,
                                                                                  initialize_flows))
                            job.set('transfer_output_remaps', "\"{0} = {1}\"".format(node_rapid_outflow_file,
                                                                                     master_rapid_outflow_file))
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
        #                    run_ecmwf_rapid_multiprocess_worker((forecast,
        #                                                         forecast_date_timestep,
        #                                                         watershed.lower(),
        #                                                         subbasin.lower(),
        #                                                         rapid_executable_location,
        #                                                         initialize_flows,
        #                                                         job_name,
        #                                                         master_rapid_outflow_file,
        #                                                         master_watershed_input_directory,
        #                                                         mp_execute_directory,
        #                                                         subprocess_forecast_log_dir,
        #                                                         watershed_job_index))
                        else:
                            raise Exception("ERROR: Invalid mp_mode. Valid types are htcondor and multiprocess ...")
                
                for rapid_input_directory, watershed_job_info in rapid_watershed_jobs.iteritems():
                    # add sub job list to master job list
                    master_job_info_list = master_job_info_list + watershed_job_info['jobs_info']
                    if mp_mode == "htcondor":
                        # wait for jobs to finish then upload files
                        for job_index, job in enumerate(watershed_job_info['jobs']):
                            job.wait()
                            # upload file when done
                            if data_manager:
                                upload_single_forecast(watershed_job_info['jobs_info'][job_index], data_manager)
                                
                    elif mp_mode == "multiprocess":
                        pool_main = mp_Pool()
                        multiprocess_worker_list = pool_main.imap_unordered(run_ecmwf_rapid_multiprocess_worker, 
                                                                            watershed_job_info['jobs'], 
                                                                            chunksize=1)
                        if data_manager:
                            for multi_job_index in multiprocess_worker_list:
                                # upload file when done
                                upload_single_forecast(watershed_job_info['jobs_info'][multi_job_index], data_manager)
                                
                        # just in case ...
                        pool_main.close()
                        pool_main.join()
    
                    # when all jobs in watershed are done, generate warning points
                    if create_warning_points:
                        watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
                        forecast_directory = os.path.join(rapid_io_files_location, 
                                                          'output', 
                                                          rapid_input_directory, 
                                                          forecast_date_timestep)
    
                        era_interim_watershed_directory = os.path.join(era_interim_data_location, rapid_input_directory)
                        if os.path.exists(era_interim_watershed_directory):
                            print("Generating warning points for {0}-{1} from {2}".format(watershed, subbasin,
                                                                                          forecast_date_timestep))
                            era_interim_files = glob(os.path.join(era_interim_watershed_directory, "return_period*.nc"))
                            if era_interim_files:
                                try:
                                    generate_warning_points(forecast_directory, era_interim_files[0],
                                                            forecast_directory, threshold=10)
                                    if upload_output_to_ckan and data_store_url and data_store_api_key:
                                        data_manager.initialize_run_ecmwf(watershed, subbasin, forecast_date_timestep)
                                        data_manager.zip_upload_warning_points_in_directory(forecast_directory)
                                except Exception as ex:
                                    print(ex)
                                    pass
                            else:
                                print("No ERA Interim file found. Skipping ...")
                        else:
                            print("No ERA Interim directory found for {0}. "
                                  "Skipping warning point generation...".format(rapid_input_directory))
                    
                # initialize flows for next run
                if initialize_flows:
                    # create new init flow files/generate warning point files
                    for rapid_input_directory in rapid_input_directories:
                        input_directory = os.path.join(rapid_io_files_location, 
                                                       'input', 
                                                       rapid_input_directory)
                        forecast_directory = os.path.join(rapid_io_files_location, 
                                                          'output', 
                                                          rapid_input_directory, 
                                                          forecast_date_timestep)

                        if os.path.exists(forecast_directory):
                            # loop through all the rapid_namelist files in directory
                            watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
                            if initialize_flows:
                                print("Initializing flows for {0}-{1} from {2}".format(watershed, subbasin,
                                                                                       forecast_date_timestep))
                                basin_files = find_current_rapid_output(forecast_directory, watershed, subbasin)
                                try:
                                    compute_initial_rapid_flows(basin_files, input_directory, forecast_date_timestep)
                                except Exception as ex:
                                    print(ex)
                                    pass

                # run autoroute process if added
                if autoroute_executable_location and autoroute_io_files_location:
                    # run autoroute on all of the watersheds
                    run_autorapid_process(autoroute_executable_location,
                                          autoroute_io_files_location,
                                          rapid_io_files_location,
                                          forecast_date_timestep,
                                          subprocess_forecast_log_dir,
                                          geoserver_url,
                                          geoserver_username,
                                          geoserver_password,
                                          app_instance_id)
                
                last_forecast_date = get_datetime_from_date_timestep(forecast_date_timestep)

                # update lock info file with next forecast
                update_lock_info_file(LOCK_INFO_FILE, True, last_forecast_date.strftime('%Y%m%d%H'))
    
            # ----------------------------------------------------------------------
            # END FORECAST LOOP
            # ----------------------------------------------------------------------
        except Exception as ex:
            print_exc()
            print(ex)
            pass
            
        # Release & update lock info file with all completed forecasts
        update_lock_info_file(LOCK_INFO_FILE, False, last_forecast_date.strftime('%Y%m%d%H'))

        if delete_output_when_done:
            # delete local datasets
            for job_info in master_job_info_list:
                try:
                    rmtree(job_info['master_watershed_outflow_directory'])
                except OSError:
                    pass
            # delete watershed folder if empty
            for item in os.listdir(os.path.join(rapid_io_files_location, 'output')):
                try:
                    os.rmdir(os.path.join(rapid_io_files_location, 'output', item))
                except OSError:
                    pass

        # print info to user
        time_end = datetime.datetime.utcnow()
        print("Time Begin: {0}".format(time_begin_all))
        print("Time Finish: {0}".format(time_end))
        print("TOTAL TIME: {0}".format(time_end-time_begin_all))
