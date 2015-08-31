#!/usr/bin/env python
import datetime
import os
from RAPIDpy.rapid import RAPID
import sys

#local imports
from spt_ecmwf_autorapid_process.imports.CreateInflowFileFromECMWFRunoff import CreateInflowFileFromECMWFRunoff
from spt_ecmwf_autorapid_process.imports.helper_functions import (case_insensitive_file_search,
                                                                  get_date_timestep_ensemble_from_forecast)
#------------------------------------------------------------------------------
#functions
#------------------------------------------------------------------------------
def run_RAPID_single_watershed(forecast, watershed, subbasin,
                               rapid_executable_location, node_path, init_flow):
    """
    run RAPID on single watershed after ECMWF prepared
    """
    forecast_date_timestep, ensemble_number = get_date_timestep_ensemble_from_forecast(forecast)

    #run RAPID
    print "Running RAPID for:", subbasin, "Ensemble:", ensemble_number

    rapid_input_directory = os.path.join(node_path, "rapid_input")
    #default duration of 15 days
    duration = 15*24*60*60
    #default interval of 6 hrs
    interval = 6*60*60
    #if it is high res
    if(int(ensemble_number) == 52):
        #duration of 10 days
        duration = 10*24*60*60
        #interval of 3 hrs
        #interval = 3*60*60

    qinit_file = ""
    BS_opt_Qinit = False
    if(init_flow):
        #check for qinit file
        past_date = (datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H") - \
                     datetime.timedelta(hours=12)).strftime("%Y%m%dt%H")
        qinit_file = os.path.join(rapid_input_directory, 'Qinit_%s.csv' % past_date)
        BS_opt_Qinit = qinit_file and os.path.exists(qinit_file)
        if not BS_opt_Qinit:
            qinit_file = ""
            print "Error:", qinit_file, "not found. Not initializing ..."



    rapid_manager = RAPID(rapid_executable_location=rapid_executable_location,
                          ZS_TauR=interval, #duration of routing procedure (time step of runoff data)
                          ZS_dtR=15*60, #internal routing time step
                          ZS_TauM=duration, #total simulation time 
                          ZS_dtM=interval, #input time step
                          rapid_connect_file=case_insensitive_file_search(rapid_input_directory,
                                                                          r'rapid_connect\.csv'),
                          Vlat_file=os.path.join(node_path,
                                                 'm3_riv_bas_%s.nc' % ensemble_number),
                          riv_bas_id_file=case_insensitive_file_search(rapid_input_directory,
                                                                       r'riv_bas_id.*?\.csv'),
                          k_file=case_insensitive_file_search(rapid_input_directory,
                                                              r'k\.csv'),
                          x_file=case_insensitive_file_search(rapid_input_directory,
                                                              r'x\.csv'),
                          Qout_file=os.path.join(node_path,
                                                 'Qout_%s_%s_%s.nc' % (watershed.lower(),
                                                                       subbasin.lower(),
                                                                       ensemble_number)),
                          Qinit_file=qinit_file,
                          BS_opt_Qinit=BS_opt_Qinit
                         )

    rapid_manager.update_reach_number_data()
    rapid_manager.run()
    rapid_manager.make_output_CF_compliant(simulation_start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"),
                                           comid_lat_lon_z_file=case_insensitive_file_search(rapid_input_directory,
                                                                                             r'comid_lat_lon_z.*?\.csv'),
                                           project_name="ECMWF-RAPID Predicted flows by US Army ERDC")


def process_upload_ECMWF_RAPID(ecmwf_forecast, watershed, subbasin,
                               rapid_executable_location, init_flow):
    """
    prepare all ECMWF files for rapid
    """
    node_path = os.path.dirname(os.path.realpath(__file__))

    forecast_date_timestep, ensemble_number = get_date_timestep_ensemble_from_forecast(ecmwf_forecast)
    forecast_basename = os.path.basename(ecmwf_forecast)

    old_rapid_input_directory = os.path.join(node_path, "%s-%s" % (watershed, subbasin))
    rapid_input_directory = os.path.join(node_path, "rapid_input")

    #rename rapid input directory
    os.rename(old_rapid_input_directory, rapid_input_directory)

    inflow_file_name = 'm3_riv_bas_%s.nc' % ensemble_number

    #determine weight table from resolution
    if ensemble_number == 52:
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_high_res.csv')
    else:
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_low_res.csv')

    time_start_all = datetime.datetime.utcnow()

    def remove_inflow_file(inflow_file_name):
        """
        remove inflow file generated from ecmwf downscaling
        """
        print "Cleaning up"
        #remove inflow file
        try:
            os.remove(inflow_file_name)
        except OSError:
            pass

    #RUN CALCULATIONS
    try:
        #prepare ECMWF file for RAPID
        print "Running all ECMWF downscaling for watershed:", watershed, subbasin, \
            forecast_date_timestep, ensemble_number

        print "Converting ECMWF inflow"
        #optional argument ... time interval?
        RAPIDinflowECMWF_tool = CreateInflowFileFromECMWFRunoff()
        RAPIDinflowECMWF_tool.execute(forecast_basename, weight_table_file, inflow_file_name)

        time_finish_ecmwf = datetime.datetime.utcnow()
        print "Time to convert ECMWF: %s" % (time_finish_ecmwf-time_start_all)

        run_RAPID_single_watershed(forecast_basename, watershed, subbasin,
                                   rapid_executable_location, node_path, init_flow)
    except Exception:
        remove_inflow_file(inflow_file_name)
        raise

    #CLEAN UP
    remove_inflow_file(inflow_file_name)

    time_stop_all = datetime.datetime.utcnow()
    print "Total time to compute: %s" % (time_stop_all-time_start_all)

if __name__ == "__main__":   
    process_upload_ECMWF_RAPID(sys.argv[1],sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
