#!/usr/bin/env python
from condorpy import Job as CJob
from condorpy import Templates as tmplt
import datetime
from glob import glob
import os
from shutil import rmtree
import tarfile

#local imports
from autorapid_process import run_autorapid_process
from imports.ftp_ecmwf_download import download_all_ftp
from imports.generate_warning_points_from_return_periods import generate_warning_points
from imports.helper_functions import (clean_logs,
                                      find_current_rapid_output,
                                      get_valid_watershed_list,
                                      get_date_timestep_ensemble_from_forecast,
                                      get_watershed_subbasin_from_folder,
                                      compute_initial_rapid_flows,
                                      update_inital_flows_usgs)
#package imports
from spt_dataset_manager.dataset_manager import (ECMWFRAPIDDatasetManager,
                                                 RAPIDInputDatasetManager)
                                                  
#----------------------------------------------------------------------------------------
# MAIN PROCESS
#----------------------------------------------------------------------------------------
def run_ecmwf_rapid_process(rapid_executable_location, #path to RAPID executable
                            rapid_io_files_location, #path ro RAPID input/output directory
                            ecmwf_forecast_location, #path to ECMWF forecasts
                            condor_log_directory, #path to store HTCondor logs
                            main_log_directory, #path to store main logs
                            data_store_url="", #CKAN API url
                            data_store_api_key="", #CKAN API Key,
                            data_store_owner_org="", #CKAN owner organization
                            app_instance_id="", #Streamflow Prediction tool instance ID
                            sync_rapid_input_with_ckan=False, #match Streamflow Prediciton tool RAPID input
                            download_ecmwf=True, #Download recent ECMWF forecast before running
                            ftp_host="", #ECMWF ftp site path
                            ftp_login="", #ECMWF ftp login name
                            ftp_passwd="", #ECMWF ftp password
                            ftp_directory="", #ECMWF ftp directory
                            upload_output_to_ckan=False, #upload data to CKAN and remove local copy
                            delete_output_when_done=False, #delete all output data from this code
                            initialize_flows=False, #use forecast to initialize next run
                            era_interim_data_location="", #path to ERA Interim return period data 
                            create_warning_points=False, #generate waring points for Streamflow Prediction Tool
                            autoroute_executable_location="", #location of AutoRoute executable
                            autoroute_io_files_location="", #path to AutoRoute input/outpuf directory
                            geoserver_url='', #url to API endpoint ending in geoserver/rest
                            geoserver_username='', #username for geoserver
                            geoserver_password='' #password for geoserver
                            ):
    """
    This it the main ECMWF RAPID process
    """
    time_begin_all = datetime.datetime.utcnow()
    date_string = time_begin_all.strftime('%Y%m%d')
    #date_string = datetime.datetime(2015,8,13).strftime('%Y%m%d')
    local_scripts_location = os.path.dirname(os.path.realpath(__file__))

    if sync_rapid_input_with_ckan and app_instance_id and data_store_url and data_store_api_key:
        #sync with data store
        ri_manager = RAPIDInputDatasetManager(data_store_url,
                                              data_store_api_key,
                                              'ecmwf',
                                              app_instance_id)
        ri_manager.sync_dataset(os.path.join(rapid_io_files_location,'input'))

    #clean up old log files
    clean_logs(condor_log_directory, main_log_directory)

    #get list of correclty formatted rapid input directories in rapid directory
    rapid_input_directories = get_valid_watershed_list(os.path.join(rapid_io_files_location, "input"))
    
    if download_ecmwf and ftp_host:
        #download all files for today
        ecmwf_folders = sorted(download_all_ftp(ecmwf_forecast_location,
                                                'Runoff.%s*.netcdf.tar.gz' % date_string,
                                                ftp_host,
                                                ftp_login,
                                                ftp_passwd,
                                                ftp_directory))
    else:
        ecmwf_folders = sorted(glob(os.path.join(ecmwf_forecast_location,
                                                 'Runoff.'+date_string+'*.netcdf')))

    if upload_output_to_ckan and data_store_url and data_store_api_key:
        #init data manager for CKAN
        data_manager = ECMWFRAPIDDatasetManager(data_store_url,
                                                data_store_api_key,
                                                data_store_owner_org)

    #prepare ECMWF files
    master_job_info_list = []
    for ecmwf_folder in ecmwf_folders:
        ecmwf_forecasts = glob(os.path.join(ecmwf_folder,'full_*.runoff.netcdf')) + \
                          glob(os.path.join(ecmwf_folder,'*.52.205.*.runoff.netcdf'))
        #make the largest files first
        ecmwf_forecasts.sort(key=os.path.getsize, reverse=True)

        forecast_date_timestep = get_date_timestep_ensemble_from_forecast(ecmwf_forecasts[0])[0]
        print forecast_date_timestep
        #submit jobs to downsize ecmwf files to watershed
        iteration = 0
        rapid_watershed_jobs = {}
        for rapid_input_directory in rapid_input_directories:
            #keep list of jobs
            rapid_watershed_jobs[rapid_input_directory] = {
                                                            'jobs': [], 
                                                            'jobs_info': []
                                                           }
            print "Running forecasts for:", rapid_input_directory, os.path.basename(ecmwf_folder)
            watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
            master_watershed_input_directory = os.path.join(rapid_io_files_location, "input", rapid_input_directory)
            master_watershed_outflow_directory = os.path.join(rapid_io_files_location, 'output',
                                                              rapid_input_directory, forecast_date_timestep)
            #add USGS gage data to initialization file
            if initialize_flows:
                #update intial flows with usgs data
                update_inital_flows_usgs(master_watershed_input_directory, 
                                         forecast_date_timestep)
            
            #create jobs for HTCondor
            for forecast in ecmwf_forecasts:
                ensemble_number = get_date_timestep_ensemble_from_forecast(forecast)[1]
                try:
                    os.makedirs(master_watershed_outflow_directory)
                except OSError:
                    pass
    
                #initialize HTCondor Directory
                condor_init_dir = os.path.join(condor_log_directory, forecast_date_timestep)
                try:
                    os.makedirs(condor_init_dir)
                except OSError:
                    pass
    
                #get basin names
                outflow_file_name = 'Qout_%s_%s_%s.nc' % (watershed.lower(), subbasin.lower(), ensemble_number)
                node_rapid_outflow_file = outflow_file_name
                master_rapid_outflow_file = os.path.join(master_watershed_outflow_directory, outflow_file_name)
    
                #create job to downscale forecasts for watershed
                job = CJob('job_%s_%s_%s' % (forecast_date_timestep, watershed, iteration), tmplt.vanilla_transfer_files)
                job.set('executable',os.path.join(local_scripts_location,'htcondor_ecmwf_rapid.py'))
                job.set('transfer_input_files', "%s, %s, %s" % (forecast, master_watershed_input_directory, local_scripts_location))
                job.set('initialdir',condor_init_dir)
                job.set('arguments', '%s %s %s %s %s' % (forecast, watershed.lower(), subbasin.lower(),
                                                            rapid_executable_location, initialize_flows))
                job.set('transfer_output_remaps',"\"%s = %s\"" % (node_rapid_outflow_file, master_rapid_outflow_file))
                job.submit()
                rapid_watershed_jobs[rapid_input_directory]['jobs'].append(job)
                rapid_watershed_jobs[rapid_input_directory]['jobs_info'].append({'watershed' : watershed,
                                                                                  'subbasin' : subbasin,
                                                                                  'outflow_file_name' : master_rapid_outflow_file,
                                                                                  'forecast_date_timestep' : forecast_date_timestep,
                                                                                  'ensemble_number': ensemble_number,
                                                                                  'master_watershed_outflow_directory': master_watershed_outflow_directory,
                                                                                  })
                iteration += 1
        
        
        for rapid_input_directory, watershed_job_info in rapid_watershed_jobs.iteritems():
            #add sub job list to master job list
            master_job_info_list = master_job_info_list + watershed_job_info['jobs_info']
            #wait for jobs to finish then upload files
            for index, job in enumerate(watershed_job_info['jobs']):
                job.wait()
                #upload file when done
                if upload_output_to_ckan and data_store_url and data_store_api_key:
                    job_info = watershed_job_info['jobs_info'][index]
                    print "Uploading", job_info['watershed'], job_info['subbasin'], \
                        job_info['forecast_date_timestep'], job_info['ensemble_number']
                    #Upload to CKAN
                    data_manager.initialize_run_ecmwf(job_info['watershed'], job_info['subbasin'], job_info['forecast_date_timestep'])
                    data_manager.update_resource_ensemble_number(job_info['ensemble_number'])
                    #upload file
                    try:
                        #tar.gz file
                        output_tar_file =  os.path.join(job_info['master_watershed_outflow_directory'], "%s.tar.gz" % data_manager.resource_name)
                        if not os.path.exists(output_tar_file):
                            with tarfile.open(output_tar_file, "w:gz") as tar:
                                tar.add(job_info['outflow_file_name'], arcname=os.path.basename(job_info['outflow_file_name']))
                        return_data = data_manager.upload_resource(output_tar_file)
                        if not return_data['success']:
                            print return_data
                            print "Attempting to upload again"
                            return_data = data_manager.upload_resource(output_tar_file)
                            if not return_data['success']:
                                print return_data
                            else:
                                print "Upload success"
                        else:
                            print "Upload success"
                    except Exception, e:
                        print e
                        pass
                    #remove tar.gz file
                    os.remove(output_tar_file)
            #when all jobs in watershed are done, generate warning points
            if create_warning_points:
                watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
                forecast_directory = os.path.join(rapid_io_files_location, 
                                                  'output', 
                                                  rapid_input_directory, 
                                                  forecast_date_timestep)

                era_interim_watershed_directory = os.path.join(era_interim_data_location, rapid_input_directory)
                if os.path.exists(era_interim_watershed_directory):
                    print "Generating Warning Points for", watershed, subbasin, "from", forecast_date_timestep
                    era_interim_files = glob(os.path.join(era_interim_watershed_directory, "*.nc"))
                    if era_interim_files:
                        try:
                            generate_warning_points(forecast_directory, era_interim_files[0], forecast_directory, threshold=10)
                            if upload_output_to_ckan and data_store_url and data_store_api_key:
                                data_manager.initialize_run_ecmwf(watershed, subbasin, forecast_date_timestep)
                                data_manager.zip_upload_warning_points_in_directory(forecast_directory)
                        except Exception, ex:
                            print ex
                            pass
                    else:
                        print "No ERA Interim file found. Skipping ..."
                else:
                    print "No ERA Interim directory found for", rapid_input_directory, ". Skipping warning point generation..."
            

        #initialize flows for next run
        if initialize_flows:
            #create new init flow files/generate warning point files
            for rapid_input_directory in rapid_input_directories:
                input_directory = os.path.join(rapid_io_files_location, 
                                               'input', 
                                               rapid_input_directory)
                forecast_directory = os.path.join(rapid_io_files_location, 
                                                  'output', 
                                                  rapid_input_directory, 
                                                  forecast_date_timestep)
                if os.path.exists(forecast_directory):
                    #loop through all the rapid_namelist files in directory
                    watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
                    if initialize_flows:
                        print "Initializing flows for", watershed, subbasin, "from", forecast_date_timestep
                        basin_files = find_current_rapid_output(forecast_directory, watershed, subbasin)
                        try:
                            compute_initial_rapid_flows(basin_files, input_directory, forecast_date_timestep)
                        except Exception, ex:
                            print ex
                            pass

    
        #run autoroute process if added                
        if autoroute_executable_location and autoroute_io_files_location:
            #run autoroute on all of the watersheds
            run_autorapid_process(autoroute_executable_location,
                                  autoroute_io_files_location,
                                  rapid_io_files_location,
                                  forecast_date_timestep,
                                  condor_log_directory,
                                  geoserver_url,
                                  geoserver_username,
                                  geoserver_password,
                                  app_instance_id)
                
    if delete_output_when_done:
        #delete local datasets
        for job_info in master_job_info_list:
            try:
                rmtree(job_info['master_watershed_outflow_directory'])
            except OSError:
                pass
        #delete watershed folder if empty
        for item in os.listdir(os.path.join(rapid_io_files_location, 'output')):
            try:
                os.rmdir(os.path.join(rapid_io_files_location, 'output', item))
            except OSError:
                pass

    #print info to user
    time_end = datetime.datetime.utcnow()
    print "Time Begin All: " + str(time_begin_all)
    print "Time Finish All: " + str(time_end)
    print "TOTAL TIME: "  + str(time_end-time_begin_all)