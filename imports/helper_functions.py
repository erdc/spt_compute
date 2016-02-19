# -*- coding: utf-8 -*-
import datetime
from glob import glob
import os
from pytz import utc
import re
from shutil import rmtree

#local
from RAPIDpy.rapid import RAPID
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
        try:
            dir_datetime = datetime.datetime.strptime(condor_dir[:11], "%Y%m%d.%H")
            if (date_today-dir_datetime > week_timedelta):
                rmtree(os.path.join(condor_log_directory, condor_dir))
        except Exception as ex:
            print ex
            pass

    #clean up log files
    main_log_files = [f for f in os.listdir(main_log_directory) if not os.path.isdir(os.path.join(main_log_directory, f))]
    for main_log_file in main_log_files:
        try:
            log_datetime = datetime.datetime.strptime(main_log_file, "{0}%y%m%d%H%M%S.log".format(prepend))
            if (date_today-log_datetime > week_timedelta):
                os.remove(os.path.join(main_log_directory, main_log_file))
        except Exception as ex:
            print ex
            pass

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

def get_date_timestep_from_forecast_folder(forecast_folder):
    """
    Gets the datetimestep from forecast
    """
    #OLD: Runoff.20151112.00.netcdf.tar.gz
    #NEW: Runoff.20160209.0.exp69.Fgrid.netcdf.tar
    forecast_split = os.path.basename(forecast_folder).split(".")
    forecast_date_timestep = ".".join(forecast_split[1:3])
    return re.sub("[^\d.]+", "", forecast_date_timestep)

def get_ensemble_number_from_forecast(forecast_name):
    """
    Gets the datetimestep from forecast
    """
    #OLD: 20151112.00.1.205.runoff.grib.runoff.netcdf
    #NEW: 52.Runoff.nc
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

def compute_seasonal_initial_rapid_flows(historical_qout_file, input_directory, forecast_date_timestep):
    """
    Gets the seasonal average from historical file to initialize from
    """
    current_forecast_date = datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H")
    #move the date back a forecast (12 hrs) to be used in this forecast
    forecast_date_string = (current_forecast_date-datetime.timedelta(seconds=12*3600)).strftime("%Y%m%dt%H")
    init_file_location = os.path.join(input_directory,'Qinit_%s.csv' % forecast_date_string)
    if not os.path.exists(init_file_location):
        #check to see if exists and only perform operation once
        if historical_qout_file and os.path.exists(historical_qout_file):
            rapid_manager = RAPID(Qout_file=historical_qout_file,
                                  rapid_connect_file=os.path.join(input_directory,'rapid_connect.csv'))
            rapid_manager.generate_seasonal_intitialization(init_file_location)
        else:
            print "No seasonal streamflow file found. Skipping ..."

def compute_seasonal_initial_rapid_flows_multicore_worker(args):
    """
    Worker function using mutliprocessing for compute_seasonal_initial_rapid_flows
    """
    compute_seasonal_initial_rapid_flows(args[0], args[1], args[2])
    
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