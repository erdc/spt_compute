#!/usr/bin/env python
import datetime
from netCDF4 import Dataset as NETDataset
import os
from RAPIDpy.rapid import RAPID
from RAPIDpy.make_CF_RAPID_output import ConvertRAPIDOutputToCF
import sys

#local imports
from spt_ecmwf_autorapid_process.imports.CreateInflowFileFromECMWFRunoff import CreateInflowFileFromECMWFRunoff
from spt_ecmwf_autorapid_process.imports.helper_functions import (case_insensitive_file_search,
                                                                  get_ensemble_number_from_forecast)
#------------------------------------------------------------------------------
#functions
#------------------------------------------------------------------------------
def process_ECMWF_RAPID(ecmwf_forecast, forecast_date_timestep, watershed, subbasin,
                        rapid_executable_location, init_flow):
    """
    prepare all ECMWF files for rapid
    """
    time_start_all = datetime.datetime.utcnow()

    node_path = os.path.dirname(os.path.realpath(__file__))

    ensemble_number = get_ensemble_number_from_forecast(ecmwf_forecast)
    forecast_basename = os.path.basename(ecmwf_forecast)

    old_rapid_input_directory = os.path.join(node_path, "%s-%s" % (watershed, subbasin))
    rapid_input_directory = os.path.join(node_path, "rapid_input")

    #rename rapid input directory
    os.rename(old_rapid_input_directory, rapid_input_directory)

    def remove_file(file_name):
        """
        remove file
        """
        try:
            os.remove(file_name)
        except OSError:
            pass

    #prepare ECMWF file for RAPID
    print "Running all ECMWF downscaling for watershed:", watershed, subbasin, \
        forecast_date_timestep, ensemble_number

    #set up RAPID manager
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
                          ZS_dtM=3*60*60, #RAPID internal loop time interval
                         )
    
    rapid_manager.update_reach_number_data()
    
    outflow_file_name = os.path.join(node_path, 
                                     'Qout_%s_%s_%s.nc' % (watershed.lower(), 
                                                           subbasin.lower(), 
                                                           ensemble_number))

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
            
            
    try:
        comid_lat_lon_z_file = case_insensitive_file_search(rapid_input_directory,
                                                            r'comid_lat_lon_z.*?\.csv')
    except Exception:
        comid_lat_lon_z_file = ""
        print "comid_lat_lon_z_file not found. Not adding lat/lon/z to output file ..."

    RAPIDinflowECMWF_tool = CreateInflowFileFromECMWFRunoff()
    forecast_resolution = RAPIDinflowECMWF_tool.dataIdentify(forecast_basename)

    #determine weight table from resolution
    if forecast_resolution == "HighRes":
        #HIGH RES
        grid_name = RAPIDinflowECMWF_tool.getWeightTableName(forecast_basename, high_res=True)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}./csv'.format(grid_name))
                                                         
        inflow_file_name_1hr = 'm3_riv_bas_1hr_%s.nc' % ensemble_number
        inflow_file_name_3hr = 'm3_riv_bas_3hr_%s.nc' % ensemble_number
        inflow_file_name_6hr = 'm3_riv_bas_6hr_%s.nc' % ensemble_number
        qinit_3hr_file = os.path.join(rapid_input_directory, 'Qinit_3hr.csv')
        qinit_6hr_file = os.path.join(rapid_input_directory, 'Qinit_6hr.csv')
        
        
        try:
        
            RAPIDinflowECMWF_tool.execute(forecast_basename, 
                                          weight_table_file, 
                                          inflow_file_name_1hr,
                                          grid_name,
                                          "1hr")

            #from Hour 0 to 90 (the first 91 time points) are of 1 hr time interval
            interval_1hr = 1*60*60 #1hr
            duration_1hr = 90*60*60 #90hrs
            rapid_manager.update_parameters(ZS_TauR=interval_1hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_1hr, #total simulation time
                                            ZS_dtM=interval_1hr, #RAPID internal loop time interval
                                            Vlat_file=inflow_file_name_1hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit)
            rapid_manager.run()
    
            #generate Qinit from 1hr
            rapid_manager.generate_qinit_from_past_qout(qinit_3hr_file)

            #then from Hour 90 to 144 (19 time points) are of 3 hour time interval
            RAPIDinflowECMWF_tool.execute(forecast_basename, 
                                          weight_table_file, 
                                          inflow_file_name_3hr,
                                          grid_name,
                                          "3hr_subset")
            interval_3hr = 3*60*60 #3hr
            duration_3hr = 54*60*60 #54hrs
            qout_3hr = os.path.join(node_path,'Qout_3hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_3hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_3hr, #total simulation time 
                                            ZS_dtM=interval_3hr, #RAPID internal loop time interval
                                            Vlat_file=inflow_file_name_3hr,
                                            Qout_file=qout_3hr)
            rapid_manager.run()

            #generate Qinit from 3hr
            rapid_manager.generate_qinit_from_past_qout(qinit_6hr_file)
            #from Hour 144 to 240 (15 time points) are of 6 hour time interval
            RAPIDinflowECMWF_tool.execute(forecast_basename, 
                                          weight_table_file, 
                                          inflow_file_name_6hr,
                                          grid_name,
                                          "6hr_subset")
            interval_6hr = 6*60*60 #6hr
            duration_6hr = 96*60*60 #96hrs
            qout_6hr = os.path.join(node_path,'Qout_6hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_6hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_6hr, #total simulation time 
                                            ZS_dtM=interval_6hr, #RAPID internal loop time interval
                                            Vlat_file=inflow_file_name_6hr,
                                            Qout_file=qout_6hr)
            rapid_manager.run()

            #Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name, qout_3hr, qout_6hr], 
                                        start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"), 
                                        time_step=[interval_1hr, interval_3hr, interval_6hr], 
                                        qinit_file=qinit_file, 
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file, 
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC", 
                                        output_id_dim_name='COMID',
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
        #LOW RES - 3hr and 6hr timesteps
        grid_name = RAPIDinflowECMWF_tool.getGridName(forecast_basename, high_res=False)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}./csv'.format(grid_name))
                                                         
        inflow_file_name_3hr = 'm3_riv_bas_3hr_%s.nc' % ensemble_number
        inflow_file_name_6hr = 'm3_riv_bas_6hr_%s.nc' % ensemble_number
        qinit_6hr_file = os.path.join(rapid_input_directory, 'Qinit_6hr.csv')
        
        try:
        
            RAPIDinflowECMWF_tool.execute(forecast_basename, 
                                          weight_table_file, 
                                          inflow_file_name_3hr,
                                          grid_name,
                                          "3hr_subset")

            #from Hour 0 to 144 (the first 49 time points) are of 3 hr time interval
            interval_3hr = 3*60*60 #3hr
            duration_3hr = 144*60*60 #144hrs
            rapid_manager.update_parameters(ZS_TauR=interval_3hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_3hr, #total simulation time 
                                            ZS_dtM=interval_3hr, #RAPID internal loop time interval
                                            Vlat_file=inflow_file_name_3hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit)
            rapid_manager.run()
    
            #generate Qinit from 3hr
            rapid_manager.generate_qinit_from_past_qout(qinit_6hr_file)
            #from Hour 144 to 360 (36 time points) are of 6 hour time interval
            RAPIDinflowECMWF_tool.execute(forecast_basename, 
                                          weight_table_file, 
                                          inflow_file_name_6hr,
                                          grid_name,
                                          "6hr_subset")
            interval_6hr = 6*60*60 #6hr
            duration_6hr = 216*60*60 #216hrs
            qout_6hr = os.path.join(node_path,'Qout_6hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_6hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_6hr, #total simulation time 
                                            ZS_dtM=interval_6hr, #RAPID internal loop time interval
                                            Vlat_file=inflow_file_name_6hr,
                                            Qout_file=qout_6hr)
            rapid_manager.run()

            #Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name, qout_6hr], 
                                        start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"), 
                                        time_step=[interval_3hr, interval_6hr], 
                                        qinit_file=qinit_file, 
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file, 
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC", 
                                        output_id_dim_name='COMID',
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
        #LOW RES - 6hr only
        inflow_file_name = 'm3_riv_bas_%s.nc' % ensemble_number

        grid_name = RAPIDinflowECMWF_tool.getGridName(forecast_basename, high_res=False)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}./csv'.format(grid_name))

        try:

            print "Converting ECMWF inflow"
            RAPIDinflowECMWF_tool.execute(forecast_basename, 
                                          weight_table_file, 
                                          inflow_file_name,
                                          grid_name)
    
            interval = 6*60*60 #6hr
            duration = 15*24*60*60 #15 days
            rapid_manager.update_parameters(ZS_TauR=interval, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration, #total simulation time 
                                            Vlat_file=inflow_file_name,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit)
    
            rapid_manager.run()
            rapid_manager.make_output_CF_compliant(simulation_start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"),
                                                   comid_lat_lon_z_file=comid_lat_lon_z_file,
                                                   project_name="ECMWF-RAPID Predicted flows by US Army ERDC")

        except Exception:
            remove_file(inflow_file_name)
            raise
            
        #clean up
        remove_file(inflow_file_name)

    else:
        raise Exception("ERROR: invalid forecast resolution ...")
        
    time_stop_all = datetime.datetime.utcnow()
    print "Total time to compute: %s" % (time_stop_all-time_start_all)

if __name__ == "__main__":   
    process_ECMWF_RAPID(sys.argv[1],sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
