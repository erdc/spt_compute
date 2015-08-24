#!/usr/bin/env python
from condorpy import Job as CJob
from condorpy import Templates as tmplt
from glob import glob
import os

#local imports
from imports.helper_functions import (case_insensitive_file_search,
                                      get_valid_watershed_list, 
                                      get_watershed_subbasin_from_folder)

#package imports
from AutoRoutePy.autoroute_prepare import AutoRoutePrepare 
from AutoRoutePy.post_process import merge_shapefiles, rename_shapefiles
from spt_dataset_manager.dataset_manager import GeoServerDatasetManager

#----------------------------------------------------------------------------------------
# MAIN PROCESS
#----------------------------------------------------------------------------------------
def run_autorapid_process(autoroute_executable_location, #location of AutoRoute executable
                          autoroute_io_files_location, #path to AutoRoute input/outpuf directory
                          rapid_io_files_location, #path to AutoRoute input/outpuf directory
                          forecast_date_timestep,
                          condor_log_directory,
                          geoserver_url='',
                          geoserver_username='',
                          geoserver_password='',
                          app_instance_id=''     
                          ):
    """
    This it the main AutoRoute-RAPID process
    """
    local_scripts_location = os.path.dirname(os.path.realpath(__file__))

    #initialize HTCondor Directory
    condor_init_dir = os.path.join(condor_log_directory, forecast_date_timestep)
    try:
        os.makedirs(condor_init_dir)
    except OSError:
        pass

    #run autorapid for each watershed
    autoroute_watershed_jobs = {}
    #get most recent forecast date/timestep
    print "Running AutoRoute process for forecast:", forecast_date_timestep
    
    #loop through input watershed folders
    autoroute_input_folder = os.path.join(autoroute_io_files_location, "input")
    autoroute_output_folder = os.path.join(autoroute_io_files_location, "output")
    autoroute_input_directories = get_valid_watershed_list(autoroute_input_folder)
    for autoroute_input_directory in autoroute_input_directories:
        watershed, subbasin = get_watershed_subbasin_from_folder(autoroute_input_directory)
        
        #RAPID file paths
        master_watershed_rapid_input_directory = os.path.join(rapid_io_files_location, "input", autoroute_input_directory)
        master_watershed_rapid_output_directory = os.path.join(rapid_io_files_location, 'output',
                                                               autoroute_input_directory, forecast_date_timestep)
                                                               
        if not os.path.exists(master_watershed_rapid_input_directory):
            print "AutoRoute watershed", autoroute_input_directory, "not in RAPID IO folder. Skipping ..."
            continue
        if not os.path.exists(master_watershed_rapid_output_directory):
            print "AutoRoute watershed", autoroute_input_directory, "missing RAPID forecast folder. Skipping ..."
            continue
        
        #setup the output location
        master_watershed_autoroute_output_directory = os.path.join(autoroute_output_folder,
                                                                   autoroute_input_directory, 
                                                                   forecast_date_timestep)
        try:
            os.makedirs(master_watershed_autoroute_output_directory)
        except OSError:
            pass
        #keep list of jobs
        autoroute_watershed_jobs[autoroute_input_directory] = {
                                                                'jobs': [], 
                                                                'output_folder': master_watershed_autoroute_output_directory
                                                               }
        #loop through sub-directories
        autoroute_watershed_directory_path = os.path.join(autoroute_input_folder, autoroute_input_directory)
        for directory in os.listdir(autoroute_watershed_directory_path):
            print "Running AutoRoute for watershed:", autoroute_input_directory, "sub directory:", directory
            master_watershed_autoroute_input_directory = os.path.join(autoroute_watershed_directory_path, directory)
            if os.path.isdir(master_watershed_autoroute_input_directory):
                streamflow_raster_path = os.path.join(master_watershed_autoroute_input_directory, 'streamflow_raster.tif')
                #remove old streamflow raster if exists
                try:
                    os.remove(streamflow_raster_path)
                except OSError:
                    pass
                #create input streamflow raster for AutoRoute                        
                arp = AutoRoutePrepare(case_insensitive_file_search(master_watershed_autoroute_input_directory, r'elevation.tif'))
                arp.generate_streamflow_raster_from_rapid_output(streamid_rasterindex_file=case_insensitive_file_search(master_watershed_autoroute_input_directory,
                                                                                                                        r'streamid_rasterindex.csv'), 
                                                                 prediction_folder=master_watershed_rapid_output_directory, 
                                                                 out_streamflow_raster=streamflow_raster_path,
                                                                 method_x="mean_plus_std", method_y="max")
                
                #setup shapfile names
                output_shapefile_base_name = '%s-%s_%s' % (watershed, subbasin, directory)
                output_shapefile_shp_name = '%s.shp' % output_shapefile_base_name
                master_output_shapefile_shp_name = os.path.join(master_watershed_autoroute_output_directory, output_shapefile_shp_name)
                output_shapefile_shx_name = '%s.shx' % output_shapefile_base_name
                master_output_shapefile_shx_name = os.path.join(master_watershed_autoroute_output_directory, output_shapefile_shx_name)
                output_shapefile_prj_name = '%s.prj' % output_shapefile_base_name
                master_output_shapefile_prj_name = os.path.join(master_watershed_autoroute_output_directory, output_shapefile_prj_name)
                output_shapefile_dbf_name = '%s.dbf' % output_shapefile_base_name
                master_output_shapefile_dbf_name = os.path.join(master_watershed_autoroute_output_directory, output_shapefile_dbf_name)

                
                #create job to run autoroute for each raster in watershed
                job = CJob('job_autoroute_%s_%s' % (autoroute_input_directory, directory), tmplt.vanilla_transfer_files)
                job.set('executable', os.path.join(local_scripts_location,'htcondor_autorapid.py'))
                job.set('transfer_input_files', "%s, %s" % (master_watershed_autoroute_input_directory, 
                                                            local_scripts_location))
                job.set('initialdir', condor_init_dir)
                job.set('arguments', '%s %s %s' % (directory,
                                                   autoroute_executable_location,
                                                   output_shapefile_shp_name))
                job.set('transfer_output_remaps',"\"%s = %s; %s = %s; %s = %s; %s = %s\"" % (output_shapefile_shp_name, 
                                                                                             master_output_shapefile_shp_name,
                                                                                             output_shapefile_shx_name,
                                                                                             master_output_shapefile_shx_name,
                                                                                             output_shapefile_prj_name,
                                                                                             master_output_shapefile_prj_name,
                                                                                             output_shapefile_dbf_name,
                                                                                             master_output_shapefile_dbf_name))
                job.submit()
                autoroute_watershed_jobs[autoroute_input_directory]['jobs'].append(job)
        geoserver_manager = None
        if geoserver_url and geoserver_username and geoserver_password and app_instance_id:
            try:
                geoserver_manager = GeoServerDatasetManager(geoserver_url, 
                                                            geoserver_username, 
                                                            geoserver_password, 
                                                            app_instance_id)
            except Exception as ex:
                print ex
                print "Skipping geoserver upload ..."
                geoserver_manager = None
                pass 
        #wait for jobs to finish by watershed
        for autoroute_input_directory, autoroute_watershed_job in autoroute_watershed_jobs.iteritems():
            #time stamped layer name
            geoserver_resource_name = "%s-floodmap-%s" % (autoroute_input_directory, forecast_date_timestep)
            #geoserver_resource_name = "%s-floodmap" % (autoroute_input_directory)
            upload_shapefile = os.path.join(master_watershed_autoroute_output_directory, "%s%s" % (geoserver_resource_name, ".shp"))
            for autoroute_job in autoroute_watershed_job['jobs']:
                autoroute_job.wait()
            if len(autoroute_watershed_job['jobs'])> 1:
                # merge files
                merge_shapefiles(autoroute_watershed_job['output_folder'], 
                                 upload_shapefile, 
                                 reproject=True,
                                 remove_old=True)
            elif len(autoroute_watershed_job['jobs'])== 1:
                #rename files
                rename_shapefiles(master_watershed_autoroute_output_directory, 
                                  os.path.splitext(upload_shapefile)[0], 
                                  autoroute_input_directory)

            #upload to GeoServer
            if geoserver_manager:
                print "Uploading", upload_shapefile, "to GeoServer as", geoserver_resource_name
                shapefile_basename = os.path.splitext(upload_shapefile)[0]
                #remove past layer if exists
                geoserver_manager.purge_remove_geoserver_layer(geoserver_manager.get_layer_name(geoserver_resource_name))
                #upload updated layer
                shapefile_list = glob("%s*" % shapefile_basename)
                geoserver_manager.upload_shapefile(geoserver_resource_name, 
                                                   shapefile_list)
                                                   
                #remove local shapefile when done
                for shapefile in shapefile_list:
                    try:
                        os.remove(shapefile)
                    except OSError:
                        pass
                #remove local directories when done
                try:
                    os.remove(os.path.join(master_watershed_rapid_output_directory))
                except OSError:
                    pass
                #TODO: Upload to CKAN for historical floodmaps?
                
                
if __name__ == "__main__":
    run_autorapid_process(autoroute_executable_location='/home/alan/work/scripts/AutoRouteGDAL/source_code/autoroute',
                          autoroute_io_files_location='/home/alan/work/autoroute-io',
                          rapid_io_files_location='/home/alan/work/rapid-io',
                          forecast_date_timestep='20150813.0',
                          condor_log_directory='/home/alan/work/condor/',
                          geoserver_url='http://127.0.0.1:8181/geoserver/rest',
                          geoserver_username='admin',
                          geoserver_password='geoserver',
                          app_instance_id='9f7cb53882ed5820b3554a9d64e95273'                          
                          )