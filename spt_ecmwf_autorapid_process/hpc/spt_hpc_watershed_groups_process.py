import datetime
import json
import os
import subprocess

from ..imports.ftp_ecmwf_download import get_ftp_forecast_list,download_and_extract_ftp
from ..imports.helper_functions import CaptureStdOutToLog, get_date_timestep_from_forecast_folder
from ..rapid_process import update_lock_info_file


# TODO: Count how many forecasts to run beforehand for each region and multiply expected runtime by that number

def spt_hpc_watershed_groups_process(main_log_directory,
                                     ecmwf_forecast_location,
                                     region_qsub_path,
                                     region_reset_qsub_path,
                                     ftp_host,
                                     ftp_login,
                                     ftp_passwd,
                                     ftp_directory,
                                     region_data_list,
                                     hpc_project_number,
                                     ):
    '''
    Process to run SPT on HPC

    :param main_log_directory:
    :param ecmwf_forecast_location:
    :param region_qsub_path:
    :param region_reset_qsub_path:
    :param ftp_host:
    :param ftp_login:
    :param ftp_passwd:
    :param ftp_directory:
    :param region_data_list:
    :param hpc_project_number:
    :return:
    '''
    time_begin_all = datetime.datetime.utcnow()
    LOCK_INFO_FILE = os.path.join(main_log_directory, "ecmwf_rapid_run_info_lock.txt")
    log_file_path = os.path.join(main_log_directory,
                                 "rapid_{0}.log".format(time_begin_all.strftime("%y%m%d%H%M%S")))

    with CaptureStdOutToLog(log_file_path):
        ecmwf_folders = sorted(get_ftp_forecast_list('Runoff.*.netcdf.tar*',
                                                     ftp_host,
                                                     ftp_login,
                                                     ftp_passwd,
                                                     ftp_directory))

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
                    forecast_date = datetime.datetime.strptime(forecast_date_timestep[:11], '%Y%m%d.%H')
                    # if more recent, add to list
                    if forecast_date > last_forecast_date:
                        run_ecmwf_folders.append(ecmwf_folder)

                ecmwf_folders = run_ecmwf_folders

        # GENERATE NEW/UPDATE LOCK INFO FILE
        update_lock_info_file(LOCK_INFO_FILE, True, last_forecast_date.strftime('%Y%m%d%H'))

        if ecmwf_folders:
            region_job_id_info = {}
            for ecmwf_folder in ecmwf_folders:
                # tell program that you are running/update to last downloaded file
                update_lock_info_file(LOCK_INFO_FILE, True, last_forecast_date.strftime('%Y%m%d%H'))
                # download forecast
                try:
                    download_and_extract_ftp(ecmwf_forecast_location, ecmwf_folder,
                                             ftp_host, ftp_login,
                                             ftp_passwd, ftp_directory,
                                             False)
                    # SUBMIT JOBS IF DOWNLOAD
                    for region_data in region_data_list:
                        main_submit_command = ['qsub',
                                               '-v', 'region_name={0}'.format(region_data['name']),
                                               '-o', 'spt_main_region_log_{0}.out'.format(region_data['name']),
                                               '-l', 'walltime={0}'.format(region_data['walltime']),
                                               '-A', hpc_project_number,
                                               region_qsub_path]

                        # make job wait on previously submitted job if exists
                        previous_job_id = region_job_id_info.get(region_data['name'])
                        if previous_job_id is not None:
                            main_submit_command.insert(9, '-W')
                            main_submit_command.insert(10, 'depend=afterany:{0}'.format(previous_job_id))

                        job_info = subprocess.check_output(main_submit_command)
                        # submit job after finish to release lock file
                        job_id = job_info.split(".")[0]
                        job_reset_info = subprocess.check_output(['qsub',
                                                                  '-v', 'region_name={0}'.format(region_data['name']),
                                                                  '-o', 'spt_reset_region_log_{0}.out'.format(region_data['name']),
                                                                  '-A', hpc_project_number,
                                                                  '-W', 'depend=afterany:{0}'.format(job_id),
                                                                  region_reset_qsub_path])
                        # store for next iteration if needed
                        region_job_id_info[region_data['name']] = job_reset_info.split(".")[0]

                except Exception:
                    break
                    pass

                # get datetime from folder
                last_forecast_date_timestep = get_date_timestep_from_forecast_folder(ecmwf_folder)
                last_forecast_date = datetime.datetime.strptime(last_forecast_date_timestep[:11], '%Y%m%d.%H')

            # release lock file
            update_lock_info_file(LOCK_INFO_FILE, False, last_forecast_date.strftime('%Y%m%d%H'))

        else:
            print("No new forecasts found to run. Exiting ...")
