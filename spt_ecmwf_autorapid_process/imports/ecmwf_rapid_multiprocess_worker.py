# -*- coding: utf-8 -*-
#
#  ecmwf_rapid_multiprocess_worker.py
#  spt_ecmwf_autorapid_process
#
#  Created by Alan D. Snow
#  License: BSD 3-Clause

import datetime
import os
from RAPIDpy import RAPID
from RAPIDpy.postprocess import ConvertRAPIDOutputToCF
from shutil import move, rmtree

# local imports
from .CreateInflowFileFromECMWFRunoff import CreateInflowFileFromECMWFRunoff
from .helper_functions import (case_insensitive_file_search,
                               get_datetime_from_date_timestep,
                               get_ensemble_number_from_forecast,
                               CaptureStdOutToLog)


# ------------------------------------------------------------------------------
# functions
# ------------------------------------------------------------------------------
def ecmwf_rapid_multiprocess_worker(node_path, rapid_input_directory,
                                    ecmwf_forecast, forecast_date_timestep, 
                                    watershed, subbasin, rapid_executable_location, 
                                    init_flow):
    """
    Multiprocess worker function
    """
    time_start_all = datetime.datetime.utcnow()

    os.chdir(node_path)

    ensemble_number = get_ensemble_number_from_forecast(ecmwf_forecast)

    def remove_file(file_name):
        """
        remove file
        """
        try:
            os.remove(file_name)
        except OSError:
            pass

    # prepare ECMWF file for RAPID
    print("Running all ECMWF downscaling for watershed: {0}-{1} {2} {3}".format(watershed, 
                                                                                subbasin,
                                                                                forecast_date_timestep,
                                                                                ensemble_number))

    # set up RAPID manager
    rapid_connect_file=case_insensitive_file_search(rapid_input_directory,
                                                    r'rapid_connect\.csv')

    rapid_manager = RAPID(rapid_executable_location=rapid_executable_location,
                          rapid_connect_file=rapid_connect_file,
                          riv_bas_id_file=case_insensitive_file_search(rapid_input_directory,
                                                                       r'riv_bas_id.*?\.csv'),
                          k_file=case_insensitive_file_search(rapid_input_directory,
                                                              r'k\.csv'),
                          x_file=case_insensitive_file_search(rapid_input_directory,
                                                              r'x\.csv'),
                          ZS_dtM=3*60*60,  # RAPID internal loop time interval
                         )
    
    rapid_manager.update_reach_number_data()
    
    outflow_file_name = os.path.join(node_path, 
                                     'Qout_{0}_{1}_{2}.nc'.format(watershed.lower(),
                                                                  subbasin.lower(),
                                                                  ensemble_number))

    qinit_file = ""
    BS_opt_Qinit = False
    if(init_flow):
        # check for qinit file
        past_date = (get_datetime_from_date_timestep(forecast_date_timestep) -
                     datetime.timedelta(hours=12)).strftime("%Y%m%dt%H")
        qinit_file = os.path.join(rapid_input_directory, 'Qinit_{0}.csv'.format(past_date))
        BS_opt_Qinit = qinit_file and os.path.exists(qinit_file)
        if not BS_opt_Qinit:
            qinit_file = ""
            print("Error: {0} not found. Not initializing ...".format(qinit_file))
            
            
    try:
        comid_lat_lon_z_file = case_insensitive_file_search(rapid_input_directory,
                                                            r'comid_lat_lon_z.*?\.csv')
    except Exception:
        comid_lat_lon_z_file = ""
        print("comid_lat_lon_z_file not found. Not adding lat/lon/z to output file ...")

    RAPIDinflowECMWF_tool = CreateInflowFileFromECMWFRunoff()
    forecast_resolution = RAPIDinflowECMWF_tool.dataIdentify(ecmwf_forecast)

    # determine weight table from resolution
    if forecast_resolution == "HighRes":
        # HIGH RES
        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=True)
        # generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))
                                                         
        inflow_file_name_1hr = os.path.join(node_path, 'm3_riv_bas_1hr_{0}.nc'.format(ensemble_number))
        inflow_file_name_3hr = os.path.join(node_path, 'm3_riv_bas_3hr_{0}.nc'.format(ensemble_number))
        inflow_file_name_6hr = os.path.join(node_path, 'm3_riv_bas_6hr_{0}.nc'.format(ensemble_number))
        qinit_3hr_file = os.path.join(node_path, 'Qinit_3hr.csv')
        qinit_6hr_file = os.path.join(node_path, 'Qinit_6hr.csv')
        
        
        try:
        
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_1hr,
                                          grid_name,
                                          "1hr")

            # from Hour 0 to 90 (the first 91 time points) are of 1 hr time interval
            interval_1hr = 1*60*60  # 1hr
            duration_1hr = 90*60*60  # 90hrs

            # ZS_TauR is the duration of routing procedure (time step of runoff data)
            rapid_manager.update_parameters(ZS_TauR=interval_1hr,
                                            ZS_dtR=15*60,  # internal routing time step
                                            ZS_TauM=duration_1hr,  # total simulation time
                                            ZS_dtM=interval_1hr,  # RAPID internal loop time interval
                                            Vlat_file=inflow_file_name_1hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit)
            rapid_manager.run()
    
            # generate Qinit from 1hr
            rapid_manager.generate_qinit_from_past_qout(qinit_3hr_file)

            # then from Hour 90 to 144 (19 time points) are of 3 hour time interval
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_3hr,
                                          grid_name,
                                          "3hr_subset")
            interval_3hr = 3*60*60  # 3hr
            duration_3hr = 54*60*60  # 54hrs
            qout_3hr = os.path.join(node_path,'Qout_3hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_3hr,
                                            ZS_dtR=15*60,
                                            ZS_TauM=duration_3hr,
                                            ZS_dtM=interval_3hr,
                                            Vlat_file=inflow_file_name_3hr,
                                            Qout_file=qout_3hr)
            rapid_manager.run()

            # generate Qinit from 3hr
            rapid_manager.generate_qinit_from_past_qout(qinit_6hr_file)
            # from Hour 144 to 240 (15 time points) are of 6 hour time interval
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_6hr,
                                          grid_name,
                                          "6hr_subset")
            interval_6hr = 6*60*60  # 6hr
            duration_6hr = 96*60*60  # 96hrs
            qout_6hr = os.path.join(node_path,'Qout_6hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_6hr,
                                            ZS_dtR=15*60,
                                            ZS_TauM=duration_6hr,
                                            ZS_dtM=interval_6hr,
                                            Vlat_file=inflow_file_name_6hr,
                                            Qout_file=qout_6hr)
            rapid_manager.run()

            # Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name, qout_3hr, qout_6hr], 
                                        start_datetime=get_datetime_from_date_timestep(forecast_date_timestep),
                                        time_step=[interval_1hr, interval_3hr, interval_6hr], 
                                        qinit_file=qinit_file, 
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file, 
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC", 
                                        output_id_dim_name='rivid',
                                        output_flow_var_name='Qout',
                                        print_debug=False)
            cv.convert()
    
        except Exception:
            remove_file(qinit_3hr_file)
            remove_file(qinit_6hr_file)
            remove_file(inflow_file_name_1hr)
            remove_file(inflow_file_name_3hr)
            remove_file(inflow_file_name_6hr)
            raise
            
        remove_file(qinit_3hr_file)
        remove_file(qinit_6hr_file)
        remove_file(inflow_file_name_1hr)
        remove_file(inflow_file_name_3hr)
        remove_file(inflow_file_name_6hr)

    elif forecast_resolution == "LowResFull":
        # LOW RES - 3hr and 6hr timesteps
        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=False)
        # generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))
                                                         
        inflow_file_name_3hr = os.path.join(node_path, 'm3_riv_bas_3hr_{0}.nc'.format(ensemble_number))
        inflow_file_name_6hr = os.path.join(node_path, 'm3_riv_bas_6hr_{0}.nc'.format(ensemble_number))
        qinit_6hr_file = os.path.join(node_path, 'Qinit_6hr.csv')
        
        try:
        
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_3hr,
                                          grid_name,
                                          "3hr_subset")

            # from Hour 0 to 144 (the first 49 time points) are of 3 hr time interval
            interval_3hr = 3*60*60  # 3hr
            duration_3hr = 144*60*60  # 144hrs
            rapid_manager.update_parameters(ZS_TauR=interval_3hr,
                                            ZS_dtR=15*60,
                                            ZS_TauM=duration_3hr,
                                            ZS_dtM=interval_3hr,
                                            Vlat_file=inflow_file_name_3hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit)
            rapid_manager.run()
    
            # generate Qinit from 3hr
            rapid_manager.generate_qinit_from_past_qout(qinit_6hr_file)
            # from Hour 144 to 360 (36 time points) are of 6 hour time interval
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_6hr,
                                          grid_name,
                                          "6hr_subset")
            interval_6hr = 6*60*60  # 6hr
            duration_6hr = 216*60*60  # 216hrs
            qout_6hr = os.path.join(node_path,'Qout_6hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_6hr,
                                            ZS_dtR=15*60,
                                            ZS_TauM=duration_6hr,
                                            ZS_dtM=interval_6hr,
                                            Vlat_file=inflow_file_name_6hr,
                                            Qout_file=qout_6hr)
            rapid_manager.run()

            # Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name, qout_6hr], 
                                        start_datetime=get_datetime_from_date_timestep(forecast_date_timestep),
                                        time_step=[interval_3hr, interval_6hr], 
                                        qinit_file=qinit_file, 
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file, 
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC", 
                                        output_id_dim_name='rivid',
                                        output_flow_var_name='Qout',
                                        print_debug=False)
            cv.convert()
    
        except Exception:
            remove_file(qinit_6hr_file)
            remove_file(inflow_file_name_3hr)
            remove_file(inflow_file_name_6hr)
            raise
            
        remove_file(qinit_6hr_file)
        remove_file(inflow_file_name_3hr)
        remove_file(inflow_file_name_6hr)
        
    elif forecast_resolution == "LowRes":
        # LOW RES - 6hr only
        inflow_file_name = os.path.join(node_path, 'm3_riv_bas_%s.nc' % ensemble_number)

        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=False)
        # generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))

        try:

            print("Converting ECMWF inflow ...")
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name,
                                          grid_name)
    
            interval = 6*60*60  # 6hr
            duration = 15*24*60*60  # 15 days
            rapid_manager.update_parameters(ZS_TauR=interval,
                                            ZS_dtR=15*60,
                                            ZS_TauM=duration,
                                            Vlat_file=inflow_file_name,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit)
    
            rapid_manager.run()
            rapid_manager.make_output_CF_compliant(simulation_start_datetime=get_datetime_from_date_timestep(
                                                                                forecast_date_timestep),
                                                   comid_lat_lon_z_file=comid_lat_lon_z_file,
                                                   project_name="ECMWF-RAPID Predicted flows by US Army ERDC")

        except Exception:
            remove_file(inflow_file_name)
            raise
            
        # clean up
        remove_file(inflow_file_name)

    else:
        raise Exception("ERROR: invalid forecast resolution ...")
        
    time_stop_all = datetime.datetime.utcnow()
    print("Total time to compute: {0}".format(time_stop_all-time_start_all))


def run_ecmwf_rapid_multiprocess_worker(args):
    """
    Duplicate HTCondor behavior for multiprocess worker
    """

    ecmwf_forecast = args[0]
    forecast_date_timestep = args[1]
    watershed = args[2]
    subbasin = args[3]
    rapid_executable_location = args[4]
    initialize_flows = args[5]
    job_name = args[6]
    master_rapid_outflow_file = args[7]
    rapid_input_directory = args[8] 
    mp_execute_directory = args[9]
    subprocess_forecast_log_dir = args[10]
    watershed_job_index = args[11]

    with CaptureStdOutToLog(os.path.join(subprocess_forecast_log_dir, "{0}.log".format(job_name))):
        # create folder to run job
        execute_directory = os.path.join(mp_execute_directory, job_name)
        try:
            os.mkdir(execute_directory)
        except OSError:
            pass
        
        try:
            ecmwf_rapid_multiprocess_worker(execute_directory, rapid_input_directory,
                                            ecmwf_forecast, forecast_date_timestep, 
                                            watershed, subbasin, rapid_executable_location, 
                                            initialize_flows)
             
            # move output file from compute node to master location
            node_rapid_outflow_file = os.path.join(execute_directory, 
                                                   os.path.basename(master_rapid_outflow_file))
                                                   
            move(node_rapid_outflow_file, master_rapid_outflow_file)
            rmtree(execute_directory)                                   
        except Exception:
            rmtree(execute_directory)
            raise

    return watershed_job_index
