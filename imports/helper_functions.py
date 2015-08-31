# -*- coding: utf-8 -*-
import csv
import datetime
from glob import glob
import netCDF4 as NET
import numpy as np
import os
from pytz import utc
import re
from shutil import rmtree

#local
from assimilate_stream_gage import StreamNetworkInitializer

#----------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#----------------------------------------------------------------------------------------
def case_insensitive_file_search(directory, pattern):
    """
    Looks for file with pattern with case insensitive search
    """
    try:
        return os.path.join(directory,
                            [filename for filename in os.listdir(directory) \
                             if re.search(pattern, filename, re.IGNORECASE)][0])
    except IndexError:
        print pattern, "not found"
        raise

def clean_logs(condor_log_directory, main_log_directory, prepend="rapid_"):
    """
    This removed logs older than one week old
    """
    date_today = datetime.datetime.utcnow()
    week_timedelta = datetime.timedelta(7)
    #clean up condor logs
    condor_dirs = [d for d in os.listdir(condor_log_directory) if os.path.isdir(os.path.join(condor_log_directory, d))]
    for condor_dir in condor_dirs:
        dir_datetime = datetime.datetime.strptime(condor_dir[:11], "%Y%m%d.%H")
        if (date_today-dir_datetime > week_timedelta):
            rmtree(os.path.join(condor_log_directory, condor_dir))

    #clean up log files
    main_log_files = [f for f in os.listdir(main_log_directory) if not os.path.isdir(os.path.join(main_log_directory, f))]
    for main_log_file in main_log_files:
        log_datetime = datetime.datetime.strptime(main_log_file, "{0}%y%m%d%H%M%S.log".format(prepend))
        if (date_today-log_datetime > week_timedelta):
            os.remove(os.path.join(main_log_directory, main_log_file))

def find_current_rapid_output(forecast_directory, watershed, subbasin):
    """
    Finds the most current files output from RAPID
    """
    if os.path.exists(forecast_directory):
        basin_files = glob(os.path.join(forecast_directory,"Qout_%s_%s_*.nc" % (watershed, subbasin)))
        if len(basin_files) >0:
            return basin_files
    #there are none found
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
            print directory, "incorrectly formatted. Skipping ..."
    return valid_input_directories

def get_date_timestep_ensemble_from_forecast(forecast_name):
    """
    Gets the datetimestep from forecast
    """
    forecast_split = os.path.basename(forecast_name).split(".")
    forecast_date_timestep = ".".join(forecast_split[:2])
    ensemble_number = int(forecast_split[2])
    return forecast_date_timestep, ensemble_number

def get_watershed_subbasin_from_folder(folder_name):
    """
    Get's the watershed & subbasin name from folder
    """
    input_folder_split = folder_name.split("-")
    watershed = input_folder_split[0].lower()
    subbasin = input_folder_split[1].lower()
    return watershed, subbasin

def csv_to_list(csv_file, delimiter=','):
    """
    Reads in a CSV file and returns the contents as list,
    where every row is stored as a sublist, and each element
    in the sublist represents 1 cell in the table.

    """
    with open(csv_file, 'rb') as csv_con:
        reader = csv.reader(csv_con, delimiter=delimiter)
        return list(reader)

def get_comids_in_netcdf_file(reach_id_list, prediction_file):
    """
    Gets the subset comid_index_list, reordered_comid_list from the netcdf file
    """
    data_nc = NET.Dataset(prediction_file, mode="r")
    com_ids = data_nc.variables['COMID'][:]
    data_nc.close()
    try:
        #get where comids are in netcdf file
        netcdf_reach_indices_list = np.where(np.in1d(com_ids, reach_id_list))[0]
    except Exception as ex:
        print ex

    return netcdf_reach_indices_list, com_ids[netcdf_reach_indices_list]

def compute_initial_rapid_flows(prediction_files, input_directory, forecast_date_timestep):
    """
    Gets mean of all 52 ensembles 12-hrs in future and prints to csv as initial flow
    Qinit_file (BS_opt_Qinit)
    The assumptions are that Qinit_file is ordered the same way as rapid_connect_file
    if subset of list, add zero where there is no flow
    """
    #remove old init files for this basin
    past_init_flow_files = glob(os.path.join(input_directory, 'Qinit_*.csv'))
    for past_init_flow_file in past_init_flow_files:
        try:
            os.remove(past_init_flow_file)
        except:
            pass
    current_forecast_date = datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H")
    current_forecast_date_string = current_forecast_date.strftime("%Y%m%dt%H")
    init_file_location = os.path.join(input_directory,'Qinit_%s.csv' % current_forecast_date_string)
    #check to see if exists and only perform operation once
    if prediction_files:
        sni = StreamNetworkInitializer(connectivity_file=os.path.join(input_directory,'rapid_connect.csv'))
        sni.compute_init_flows_from_past_forecast(prediction_files)
        sni.write_init_flow_file(init_file_location)        
    else:
        print "No current forecasts found. Skipping ..."

def update_inital_flows_usgs(input_directory, forecast_date_timestep):
    """
    Update initial flows with USGS data
    """
    gage_flow_info = os.path.join(input_directory, 'usgs_gages.csv')
    current_forecast_date = datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H").replace(tzinfo=utc)
    past_date = (datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H") - \
                 datetime.timedelta(hours=12)).replace(tzinfo=utc).strftime("%Y%m%dt%H")

    qinit_file = os.path.join(input_directory, 'Qinit_%s.csv' % past_date)

    if os.path.exists(gage_flow_info) and os.path.exists(qinit_file):
        print "Updating initial flows with USGS data for:", \
              input_directory, forecast_date_timestep , "..."
              
        sni = StreamNetworkInitializer(connectivity_file=os.path.join(input_directory,'rapid_connect.csv'),
                                       gage_ids_natur_flow_file=gage_flow_info)
        sni.read_init_flows_from_past_forecast(qinit_file)
        sni.add_usgs_flows(current_forecast_date)
        sni.modify_init_flows_from_gage_flows()
        try:
            os.remove(qinit_file)
        except OSError:
            pass
        
        sni.write_init_flow_file(qinit_file)        

def log(message, severity):
    """Logs, prints, or raises a message.

    Arguments:
        message -- message to report
        severity -- string of one of these values:
            CRITICAL|ERROR|WARNING|INFO|DEBUG
    """

    print_me = ['WARNING', 'INFO', 'DEBUG']
    if severity in print_me:
        print severity, message
    else:
        raise Exception(message)


if __name__=="__main__":
    #update_inital_flows_usgs('/home/alan/work/rapid-io/input/erdc_texas_gulf_region-huc_2_12/', '20150826.0')
    print datetime.datetime(2015, 9, 9, 18) - datetime.datetime(2015, 8, 26, 0)