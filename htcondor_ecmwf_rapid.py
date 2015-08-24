#!/usr/bin/env python
import datetime
import os
from subprocess import Popen
import sys

#local imports
from spt_ecmwf_autorapid_process.imports.CreateInflowFileFromECMWFRunoff import CreateInflowFileFromECMWFRunoff
from spt_ecmwf_autorapid_process.imports.helper_functions import (case_insensitive_file_search,
                                                                   csv_to_list,
                                                                   get_date_timestep_ensemble_from_forecast)
from spt_ecmwf_autorapid_process.imports.make_CF_RAPID_output import convert_ecmwf_rapid_output_to_cf_compliant
#------------------------------------------------------------------------------
#functions
#------------------------------------------------------------------------------
def generate_namelist_file(rapid_io_files_location, watershed, subbasin,
                           ensemble_number, forecast_date_timestep, init_flow = False):
    """
    Generate RAPID namelist file with new input
    """
    rapid_input_directory = os.path.join(rapid_io_files_location, "rapid_input")
    watershed_namelist_file = os.path.join(rapid_io_files_location, 'rapid_namelist')
    template_namelist_file = case_insensitive_file_search(os.path.join(rapid_io_files_location, 'erfp_data_process_ubuntu_aws'),
                                                          'rapid_namelist_template\.dat')

    #get rapid connect info
    rapid_connect_file = case_insensitive_file_search(rapid_input_directory, r'rapid_connect\.csv')
    rapid_connect_table = csv_to_list(rapid_connect_file)
    is_riv_tot = len(rapid_connect_table)
    is_max_up = max([int(float(row[2])) for row in rapid_connect_table])

    #get riv_bas_id info
    riv_bas_id_file = case_insensitive_file_search(rapid_input_directory, r'riv_bas_id.*?\.csv')
    riv_bas_id_table = csv_to_list(riv_bas_id_file)
    is_riv_bas = len(riv_bas_id_table)


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

    qinit_file = None
    if(init_flow):
        #check for qinit file
        past_date = (datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H") - \
                     datetime.timedelta(hours=12)).strftime("%Y%m%dt%H")
        qinit_file = os.path.join(rapid_input_directory, 'Qinit_%s.csv' % past_date)
        init_flow = qinit_file and os.path.exists(qinit_file)
        if not init_flow:
            print "Error:", qinit_file, "not found. Not initializing ..."

    old_file = open(template_namelist_file)
    new_file = open(watershed_namelist_file,'w')
    for line in old_file:
        if line.strip().startswith('BS_opt_Qinit'):
            if (init_flow):
                new_file.write('BS_opt_Qinit       =.true.\n')
            else:
                new_file.write('BS_opt_Qinit       =.false.\n')
        elif line.strip().startswith('ZS_TauM'):
            new_file.write('ZS_TauM            =%s\n' % duration)
        elif line.strip().startswith('ZS_dtM'):
            new_file.write('ZS_dtM             =%s\n' % 86400)
        elif line.strip().startswith('ZS_TauR'):
            new_file.write('ZS_TauR            =%s\n' % interval)
        elif line.strip().startswith('IS_riv_tot'):
            new_file.write('IS_riv_tot         =%s\n' % is_riv_tot)
        elif line.strip().startswith('rapid_connect_file'):
            new_file.write('rapid_connect_file =\'%s\'\n' % rapid_connect_file)
        elif line.strip().startswith('IS_max_up'):
            new_file.write('IS_max_up          =%s\n' % is_max_up)
        elif line.strip().startswith('Vlat_file'):
            new_file.write('Vlat_file          =\'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                         'm3_riv_bas_%s.nc' % ensemble_number))
        elif line.strip().startswith('IS_riv_bas'):
            new_file.write('IS_riv_bas          =%s\n' % is_riv_bas)
        elif line.strip().startswith('riv_bas_id_file'):
            new_file.write('riv_bas_id_file    =\'%s\'\n' % riv_bas_id_file)
        elif line.strip().startswith('Qinit_file'):
            if (init_flow):
                new_file.write('Qinit_file         =\'%s\'\n' % qinit_file)
            else:
                new_file.write('Qinit_file         =\'\'\n')
        elif line.strip().startswith('k_file'):
            new_file.write('k_file             =\'%s\'\n' % case_insensitive_file_search(rapid_input_directory,
                                                                                         r'k\.csv'))
        elif line.strip().startswith('x_file'):
            new_file.write('x_file             =\'%s\'\n' % case_insensitive_file_search(rapid_input_directory,
                                                                                         r'x\.csv'))
        elif line.strip().startswith('Qout_file'):
            new_file.write('Qout_file          =\'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                         'Qout_%s_%s_%s.nc' % (watershed.lower(),
                                                                                               subbasin.lower(),
                                                                                               ensemble_number)))
        else:
            new_file.write(line)

    #close temp file
    new_file.close()
    old_file.close()

def run_RAPID_single_watershed(forecast, watershed, subbasin,
                               rapid_executable_location, node_path, init_flow):
    """
    run RAPID on single watershed after ECMWF prepared
    """
    forecast_date_timestep, ensemble_number = get_date_timestep_ensemble_from_forecast(forecast)
    rapid_namelist_file = os.path.join(node_path,'rapid_namelist')
    local_rapid_executable = os.path.join(node_path,'rapid')

    #create link to RAPID
    os.symlink(rapid_executable_location, local_rapid_executable)

    time_start_rapid = datetime.datetime.utcnow()

    #change the new RAPID namelist file
    print "Updating namelist file for:", watershed, subbasin, ensemble_number
    generate_namelist_file(node_path, watershed, subbasin, ensemble_number,
                           forecast_date_timestep, init_flow)

    def rapid_cleanup(local_rapid_executable, rapid_namelist_file):
        """
        Cleans up the rapid files generated by the process
        """
        #remove rapid link
        try:
            os.unlink(local_rapid_executable)
            os.remove(local_rapid_executable)
        except OSError:
            pass

        #remove namelist file
        try:
            os.remove(rapid_namelist_file)
        except OSError:
            pass


    #run RAPID
    print "Running RAPID for:", subbasin, "Ensemble:", ensemble_number
    try:
        process = Popen([local_rapid_executable], shell=True)
        process.communicate()
    except Exception:
        rapid_cleanup(local_rapid_executable, rapid_namelist_file)
        raise

    print "Time to run RAPID:",(datetime.datetime.utcnow()-time_start_rapid)

    rapid_cleanup(local_rapid_executable, rapid_namelist_file)

    #convert rapid output to be CF compliant
    convert_ecmwf_rapid_output_to_cf_compliant(datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"),
                                               node_path)

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
