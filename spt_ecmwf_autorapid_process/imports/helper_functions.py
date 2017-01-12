# -*- coding: utf-8 -*-
#
#  helper_functions.py
#  spt_ecmwf_autorapid_process
#
#  Created by Alan D. Snow
#  License: BSD-3 Clause

import datetime
from glob import glob
import os
import re
from shutil import rmtree
import sys


# ----------------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------------------------
class CaptureStdOutToLog(object):
    def __init__(self, log_file_path, error_file_path=None):
        self.log_file_path = log_file_path
        self.error_file_path = error_file_path
        if error_file_path is None:
            self.error_file_path = "{0}.err".format(os.path.splitext(log_file_path)[0])

    def __enter__(self):
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = open(self.log_file_path, 'w')
        sys.stderr = open(self.error_file_path, 'w')
        return self

    def __exit__(self, *args):
        sys.stdout.close()
        sys.stdout = self._stdout
        sys.stderr = self._stderr


def case_insensitive_file_search(directory, pattern):
    """
    Looks for file with pattern with case insensitive search
    """
    try:
        return os.path.join(directory,
                            [filename for filename in os.listdir(directory) \
                             if re.search(pattern, filename, re.IGNORECASE)][0])
    except IndexError:
        print("{0} not found".format(pattern))
        raise


def clean_logs(condor_log_directory, main_log_directory, prepend="rapid_", log_file_path=""):
    """
    This removes all logs older than one week old
    """
    date_today = datetime.datetime.utcnow()
    week_timedelta = datetime.timedelta(7)
    # clean up condor logs
    condor_dirs = [d for d in os.listdir(condor_log_directory) if os.path.isdir(os.path.join(condor_log_directory, d))]
    for condor_dir in condor_dirs:
        try:
            dir_datetime = datetime.datetime.strptime(condor_dir[:11], "%Y%m%d.%H")
            if date_today-dir_datetime > week_timedelta:
                rmtree(os.path.join(condor_log_directory, condor_dir))
        except Exception as ex:
            print(ex)
            pass

    clean_main_logs(main_log_directory, prepend, log_file_path)


def clean_main_logs(main_log_directory, prepend="rapid_", log_file_path=""):
    """
    This removes main logs older than one week old
    """
    date_today = datetime.datetime.utcnow()
    week_timedelta = datetime.timedelta(7)

    # clean up log files
    main_log_files = [f for f in os.listdir(main_log_directory) if
                      not os.path.isdir(os.path.join(main_log_directory, f))
                      and not log_file_path.endswith(f)
                      and (f.endswith('log') or f.endswith('err'))]

    for main_log_file in main_log_files:
        try:
            log_datetime = datetime.datetime.strptime(main_log_file[:18], "{0}%y%m%d%H%M%S".format(prepend))
            if date_today-log_datetime > week_timedelta:
                os.remove(os.path.join(main_log_directory, main_log_file))
        except Exception as ex:
            print(ex)
            pass


def find_current_rapid_output(forecast_directory, watershed, subbasin):
    """
    Finds the most current files output from RAPID
    """
    if os.path.exists(forecast_directory):
        basin_files = glob(os.path.join(forecast_directory,"Qout_{0}_{1}_*.nc".format(watershed, subbasin)))
        if len(basin_files) >0:
            return basin_files
    # there are none found
    return None


def get_valid_watershed_list(input_directory):
    """
    Get a list of folders formatted correctly for watershed-subbasin
    """
    valid_input_directories = []
    for directory in os.listdir(input_directory):
        if os.path.isdir(os.path.join(input_directory, directory)) \
                and len(directory.split("-")) == 2:
            valid_input_directories.append(directory)
        else:
            print("{0} incorrectly formatted. Skipping ...".format(directory))
    return valid_input_directories


def get_date_timestep_from_forecast_folder(forecast_folder):
    """
    Gets the datetimestep from forecast
    """
    # OLD: Runoff.20151112.00.netcdf.tar.gz
    # NEW: Runoff.20160209.0.exp69.Fgrid.netcdf.tar
    forecast_split = os.path.basename(forecast_folder).split(".")
    forecast_date_timestep = ".".join(forecast_split[1:3])
    return re.sub("[^\d.]+", "", forecast_date_timestep)


def get_datetime_from_date_timestep(date_timestep):
    """
    Gets the datetimestep from forecast
    """
    return datetime.datetime.strptime(date_timestep[:11], '%Y%m%d.%H')


def get_ensemble_number_from_forecast(forecast_name):
    """
    Gets the datetimestep from forecast
    """
    # OLD: 20151112.00.1.205.runoff.grib.runoff.netcdf
    # NEW: 52.Runoff.nc
    forecast_split = os.path.basename(forecast_name).split(".")
    if forecast_name.endswith(".205.runoff.grib.runoff.netcdf"):
        ensemble_number = int(forecast_split[2])
    else:
        ensemble_number = int(forecast_split[0])
    return ensemble_number


def get_watershed_subbasin_from_folder(folder_name):
    """
    Get's the watershed & subbasin name from folder
    """
    input_folder_split = folder_name.split("-")
    watershed = input_folder_split[0].lower()
    subbasin = input_folder_split[1].lower()
    return watershed, subbasin


def log(message, severity):
    """Logs, prints, or raises a message.

    Arguments:
        message -- message to report
        severity -- string of one of these values:
            CRITICAL|ERROR|WARNING|INFO|DEBUG
    """

    print_me = ['WARNING', 'INFO', 'DEBUG']
    if severity in print_me:
        print("{0} {1}".format(severity, message))
    else:
        raise Exception(message)

