#!/usr/bin/env python
import os
import sys

#package imports
from erfp_data_process_ubuntu_aws.AutoRoutePy.autoroute import AutoRoute 

#local imports
from erfp_data_process_ubuntu_aws.imports.helper_functions import case_insensitive_file_search


#------------------------------------------------------------------------------
#MAIN PROCESS
#------------------------------------------------------------------------------
def process_run_AutoRoute(autoroute_input_directory,
                          autoroute_executable_location,
                          out_shapefile_name):
    """
    Run AutoRoute in HTCondor execute directory
    """
    os.rename(autoroute_input_directory, "autoroute_input")
    node_path = os.path.dirname(os.path.realpath(__file__))
    autoroute_input_path = os.path.join(node_path, "autoroute_input")
    shp_out_raster=os.path.join(node_path, "shp_out_raster.tif")
    auto_mng = AutoRoute(autoroute_executable_location,
                         stream_file=case_insensitive_file_search(autoroute_input_path, r'streamflow_raster.tif'),
                         dem_file=case_insensitive_file_search(autoroute_input_path, r'elevation.tif'),
                         shp_out_file=shp_out_raster,
                         shp_out_shapefile=os.path.join(node_path, out_shapefile_name),
                         )
               
    auto_mng.run_autoroute(autoroute_input_file=case_insensitive_file_search(autoroute_input_path,
                                                                             r'AUTOROUTE_INPUT_FILE.txt'))
    
    try:
        os.remove(shp_out_raster)
    except OSError:
        pass
    
    

if __name__ == "__main__":   
    process_run_AutoRoute(sys.argv[1],sys.argv[2], sys.argv[3])