# -*- coding: utf-8 -*-
##
##  autorapid_process.py
##  spt_compute
##
##  Created by Alan D. Snow.
##  Copyright © 2015-2016 Alan D Snow. All rights reserved.
##  License: BSD-3 Clause

from glob import glob
import os
from geoserver.catalog import FailedRequestError as geo_cat_FailedRequestError

#local imports
from imports.helper_functions import (get_valid_watershed_list, 
                                      get_watershed_subbasin_from_folder)

#package imports
from AutoRoutePy.run import run_autoroute_multiprocess
from AutoRoutePy.post.post_process import get_shapefile_layergroup_bounds, rename_shapefiles
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

        #loop through sub-directories
        autoroute_watershed_directory_path = os.path.join(autoroute_input_folder, autoroute_input_directory)
        autoroute_watershed_jobs[autoroute_input_directory] = run_autoroute_multiprocess(autoroute_executable_location, #location of AutoRoute executable
                                                                                         autoroute_input_directory=autoroute_watershed_directory_path, #path to AutoRoute input directory
                                                                                         autoroute_output_directory=master_watershed_autoroute_output_directory, #path to AutoRoute output directory
                                                                                         log_directory=condor_init_dir,
                                                                                         rapid_output_directory=master_watershed_rapid_output_directory, #path to ECMWF RAPID input/output directory
                                                                                         mode="htcondor", #multiprocess or htcondor
                                                                                         wait_for_all_processes_to_finish=False
                                                                                      )
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
    for autoroute_watershed_directory, autoroute_watershed_job in autoroute_watershed_jobs.iteritems():
        master_watershed_autoroute_output_directory = os.path.join(autoroute_output_folder,
                                                                   autoroute_watershed_directory, 
                                                                   forecast_date_timestep)
        #time stamped layer name
        geoserver_layer_group_name = "%s-floodmap-%s" % (autoroute_watershed_directory, 
                                                         forecast_date_timestep)
        geoserver_resource_list = []
        upload_shapefile_list = []
        for job_index, job_handle in enumerate(autoroute_watershed_job['htcondor_job_list']):
            job_handle.wait()
            #time stamped layer name
            geoserver_resource_name = "%s-%s" % (geoserver_layer_group_name,
                                                 job_index)
            #upload each shapefile
            upload_shapefile = os.path.join(master_watershed_autoroute_output_directory, 
                                            "%s%s" % (geoserver_resource_name, ".shp"))
            #rename files
            rename_shapefiles(master_watershed_autoroute_output_directory, 
                              os.path.splitext(upload_shapefile)[0], 
                              autoroute_watershed_job['htcondor_job_info'][job_index]['output_shapefile_base_name'])

            #upload to GeoServer
            if geoserver_manager:
                if os.path.exists(upload_shapefile):
                    upload_shapefile_list.append(upload_shapefile)
                    print "Uploading", upload_shapefile, "to GeoServer as", geoserver_resource_name
                    shapefile_basename = os.path.splitext(upload_shapefile)[0]
                    #remove past layer if exists
                    #geoserver_manager.purge_remove_geoserver_layer(geoserver_manager.get_layer_name(geoserver_resource_name))
                    
                    #upload updated layer
                    shapefile_list = glob("%s*" % shapefile_basename)
                    #Note: Added try, except statement because the request search fails when the app
                    #deletes the layer after request is made (happens hourly), so the process may throw
                    #an exception even though it was successful.
                    """
                    ...
                      File "/home/alan/work/scripts/spt_compute/spt_dataset_manager/dataset_manager.py", line 798, in upload_shapefile
                        overwrite=True)
                      File "/usr/lib/tethys/local/lib/python2.7/site-packages/tethys_dataset_services/engines/geoserver_engine.py", line 1288, in create_shapefile_resource
                        new_resource = catalog.get_resource(name=name, workspace=workspace)
                      File "/usr/lib/tethys/local/lib/python2.7/site-packages/geoserver/catalog.py", line 616, in get_resource
                        resource = self.get_resource(name, store)
                      File "/usr/lib/tethys/local/lib/python2.7/site-packages/geoserver/catalog.py", line 606, in get_resource
                        candidates = [s for s in self.get_resources(store) if s.name == name]
                      File "/usr/lib/tethys/local/lib/python2.7/site-packages/geoserver/catalog.py", line 645, in get_resources
                        return store.get_resources()
                      File "/usr/lib/tethys/local/lib/python2.7/site-packages/geoserver/store.py", line 58, in get_resources
                        xml = self.catalog.get_xml(res_url)
                      File "/usr/lib/tethys/local/lib/python2.7/site-packages/geoserver/catalog.py", line 188, in get_xml
                        raise FailedRequestError("Tried to make a GET request to %s but got a %d status code: \n%s" % (rest_url, response.status, content))
                    geoserver.catalog.FailedRequestError: ...
                    """
                    try:
                        geoserver_manager.upload_shapefile(geoserver_resource_name, 
                                                           shapefile_list)
                    except geo_cat_FailedRequestError as ex:
                        print ex
                        print "Most likely OK, but always wise to check ..."
                        pass
                                                       
                    geoserver_resource_list.append(geoserver_manager.get_layer_name(geoserver_resource_name))
                    #TODO: Upload to CKAN for history of predicted floodmaps?
                else:
                    print upload_shapefile, "not found. Skipping upload to GeoServer ..."
        
        if geoserver_manager and geoserver_resource_list:
            print "Creating Layer Group:", geoserver_layer_group_name
            style_list = ['green' for i in range(len(geoserver_resource_list))]
            bounds = get_shapefile_layergroup_bounds(upload_shapefile_list)
            geoserver_manager.dataset_engine.create_layer_group(layer_group_id=geoserver_manager.get_layer_name(geoserver_layer_group_name), 
                                                                layers=tuple(geoserver_resource_list), 
                                                                styles=tuple(style_list),
                                                                bounds=tuple(bounds))
            #remove local shapefile when done
            for upload_shapefile in upload_shapefile_list:
                shapefile_parts = glob("%s*" % os.path.splitext(upload_shapefile)[0])
                for shapefile_part in shapefile_parts:
                    try:
                        os.remove(shapefile_part)
                    except OSError:
                        pass
                    
            #remove local directories when done
            try:
                os.rmdir(master_watershed_autoroute_output_directory)
            except OSError:
                pass
if __name__ == "__main__":
    run_autorapid_process(autoroute_executable_location='/home/alan/work/scripts/AutoRoute/source_code/autoroute',
                          autoroute_io_files_location='/home/alan/work/autoroute-io',
                          rapid_io_files_location='/home/alan/work/rapid-io',
                          forecast_date_timestep='20151217.0',
                          condor_log_directory='/home/alan/work/condor/',
                          #geoserver_url=',
                          #geoserver_username='',
                          #geoserver_password='',
                          #app_instance_id='',
                          )
