#!/usr/bin/env python
"""Copies data from RAPID netCDF output to a CF-compliant netCDF file.

Remarks:
    A new netCDF file is created with data from RAPID [1] simulation model
    output. The result follows CF conventions [2] with additional metadata
    prescribed by the NODC timeSeries Orthogonal template [3] for time series
    at discrete point feature locations.

    This script was created for the National Flood Interoperability Experiment,
    and so metadata in the result reflects that.

Requires:
    netcdf4-python - https://github.com/Unidata/netcdf4-python

Inputs:
    Lookup CSV table with COMID, Lat, Lon, and Elev_m columns. Columns must
    be in that order and these must be the first four columns. The order of
    COMIDs in the table must match the order of features in the netCDF file.

    RAPID output netCDF file. File must be named *YYYYMMDDTHHMMZ.nc, e.g.,
    rapid_20150124T0000Z.nc. The ISO datetime indicates the first time
    coordinate in the file. An example CDL text representation of the file
    header is shown below. The file must be located in the 'input' folder.

    Input files are moved to the 'archive' upon completion.

///////////////////////////////////////////////////
netcdf result_2014100520141101 {
dimensions:
    Time = UNLIMITED ; // (224 currently)
    COMID = 61818 ;
variables:
    float Qout(Time, COMID) ;
///////////////////////////////////////////////////

Outputs:
    CF-compliant netCDF file of RAPID results, named with original filename
    with "_CF" appended to the filename. File is written to 'output' folder.

    Input netCDF file is archived or deleted, based on 'archive' config
    parameter.

Usage:
    Option 1: Run standalone. Script will use logger.
    Option 2: Run from another script.
        First, import the script, e.g., import make_CF_RAPID_output as cf.
        If you want to use this script's logger (optional):
            1. Call init_logger with the full path of a log filename to get a
               logger designed for use with this script.
            2. Call main() with the logger as the first argument.
        If you don't want to use the logger, just call main().

References:
    [1] http://rapid-hub.org/
    [2] http://cfconventions.org/
    [3] http://www.nodc.noaa.gov/data/formats/netcdf/v1.1/
"""

from datetime import datetime, timedelta
from glob import glob
import inspect
import os
import re
import shutil

from netCDF4 import Dataset
import numpy as np

from helper_functions import csv_to_list, log

def get_this_file():
    """Returns full filename of this script.

    Remarks: Inspect sometimes only gives filename without path if run from
    command prompt or as a Windows scheduled task with a Start in location
    specified.
    """

    f = inspect.stack()[0][1]
    if not os.path.isfile(f):
        f = os.path.realpath(__file__)
    return f


def get_this_path():
    """Returns path to this script."""

    return os.path.dirname(get_this_file())


def get_input_nc_files(folder):
    files = []
    for f in os.listdir(folder):
        if f.endswith('.nc'):
            files.append(f)
    return files


def validate_raw_nc(nc):
    """Checks that raw netCDF file has the right dimensions and variables.

    Arguments:
        nc -- netCDF dataset object representing raw RAPID output

    Returns:
        name of ID dimension,
        length of time dimension,
        name of flow variable

    Remarks: Raises exception if file doesn't validate.
    """

    dims = nc.dimensions
    if 'COMID' in dims:
        id_dim_name = 'COMID'
    elif 'FEATUREID' in dims:
        id_dim_name = 'FEATUREID'
    else:
        msg = 'Could not find ID dimension. Looked for COMID and FEATUREID.'
        raise Exception(msg)
    id_len = len(dims[id_dim_name])

    if 'Time' not in dims:
        msg = 'Could not find time dimension. Looked for Time.'
        raise Exception(msg)
    time_len = len(dims['Time'])

    variables = nc.variables
    id_var_name = None
    if 'COMID' in dims:
        id_var_name = 'COMID'
    elif 'FEATUREID' in dims:
        id_var_name = 'FEATUREID'
    if id_var_name is not None and id_var_name != id_dim_name:
        msg = ('ID dimension name (' + id_dim_name + ') does not equal ID ' +
               'variable name (' + id_var_name + ').')
        log(msg, 'WARNING')

    if 'Qout' in variables:
        q_var_name = 'Qout'
    elif 'm3_riv' in variables:
        q_var_name = 'm3_riv'
    else:
        msg = 'Could not find flow variable. Looked for Qout and m3_riv.'
        raise Exception(msg)

    return id_dim_name, id_len, time_len, q_var_name


def initialize_output(filename, id_dim_name, time_len,
                      id_len, time_step_seconds):
    """Creates netCDF file with CF dimensions and variables, but no data.

    Arguments:
        filename -- full path and filename for output netCDF file
        id_dim_name -- name of Id dimension and variable, e.g., COMID
        time_len -- (integer) length of time dimension (number of time steps)
        id_len -- (integer) length of Id dimension (number of time series)
        time_step_seconds -- (integer) number of seconds per time step
    """

    cf_nc = Dataset(filename, 'w', format='NETCDF3_CLASSIC')

    # Create global attributes
    log('    globals', 'DEBUG')
    cf_nc.featureType = 'timeSeries'
    cf_nc.Metadata_Conventions = 'Unidata Dataset Discovery v1.0'
    cf_nc.Conventions = 'CF-1.6'
    cf_nc.cdm_data_type = 'Station'
    cf_nc.nodc_template_version = (
        'NODC_NetCDF_TimeSeries_Orthogonal_Template_v1.1')
    cf_nc.standard_name_vocabulary = ('NetCDF Climate and Forecast (CF) ' +
                                      'Metadata Convention Standard Name ' +
                                      'Table v28')
    cf_nc.title = 'RAPID Result'
    cf_nc.summary = ("Results of RAPID river routing simulation. Each river " +
                     "reach (i.e., feature) is represented by a point " +
                     "feature at its midpoint, and is identified by the " +
                     "reach's unique NHDPlus COMID identifier.")
    cf_nc.time_coverage_resolution = 'point'
    cf_nc.geospatial_lat_min = 0.0
    cf_nc.geospatial_lat_max = 0.0
    cf_nc.geospatial_lat_units = 'degrees_north'
    cf_nc.geospatial_lat_resolution = 'midpoint of stream feature'
    cf_nc.geospatial_lon_min = 0.0
    cf_nc.geospatial_lon_max = 0.0
    cf_nc.geospatial_lon_units = 'degrees_east'
    cf_nc.geospatial_lon_resolution = 'midpoint of stream feature'
    cf_nc.geospatial_vertical_min = 0.0
    cf_nc.geospatial_vertical_max = 0.0
    cf_nc.geospatial_vertical_units = 'm'
    cf_nc.geospatial_vertical_resolution = 'midpoint of stream feature'
    cf_nc.geospatial_vertical_positive = 'up'
    cf_nc.project = 'National Flood Interoperability Experiment'
    cf_nc.processing_level = 'Raw simulation result'
    cf_nc.keywords_vocabulary = ('NASA/Global Change Master Directory ' +
                                 '(GCMD) Earth Science Keywords. Version ' +
                                 '8.0.0.0.0')
    cf_nc.keywords = 'DISCHARGE/FLOW'
    cf_nc.comment = 'Result time step (seconds): ' + str(time_step_seconds)

    timestamp = datetime.utcnow().isoformat() + 'Z'
    cf_nc.date_created = timestamp
    cf_nc.history = (timestamp + '; added time, lat, lon, z, crs variables; ' +
                     'added metadata to conform to NODC_NetCDF_TimeSeries_' +
                     'Orthogonal_Template_v1.1')

    # Create dimensions
    log('    dimming', 'DEBUG')
    cf_nc.createDimension('time', time_len)
    cf_nc.createDimension(id_dim_name, id_len)

    # Create variables
    log('    timeSeries_var', 'DEBUG')
    timeSeries_var = cf_nc.createVariable(id_dim_name, 'i4', (id_dim_name,))
    timeSeries_var.long_name = (
        'Unique NHDPlus COMID identifier for each river reach feature')
    timeSeries_var.cf_role = 'timeseries_id'

    log('    time_var', 'DEBUG')
    time_var = cf_nc.createVariable('time', 'i4', ('time',))
    time_var.long_name = 'time'
    time_var.standard_name = 'time'
    time_var.units = 'seconds since 1970-01-01 00:00:00 0:00'
    time_var.axis = 'T'

    log('    lat_var', 'DEBUG')
    lat_var = cf_nc.createVariable('lat', 'f8', (id_dim_name,),
                                   fill_value=-9999.0)
    lat_var.long_name = 'latitude'
    lat_var.standard_name = 'latitude'
    lat_var.units = 'degrees_north'
    lat_var.axis = 'Y'

    log('    lon_var', 'DEBUG')
    lon_var = cf_nc.createVariable('lon', 'f8', (id_dim_name,),
                                   fill_value=-9999.0)
    lon_var.long_name = 'longitude'
    lon_var.standard_name = 'longitude'
    lon_var.units = 'degrees_east'
    lon_var.axis = 'X'

    log('    z_var', 'DEBUG')
    z_var = cf_nc.createVariable('z', 'f8', (id_dim_name,),
                                 fill_value=-9999.0)
    z_var.long_name = ('Elevation referenced to the North American ' +
                       'Vertical Datum of 1988 (NAVD88)')
    z_var.standard_name = 'surface_altitude'
    z_var.units = 'm'
    z_var.axis = 'Z'
    z_var.positive = 'up'

    log('    crs_var', 'DEBUG')
    crs_var = cf_nc.createVariable('crs', 'i4')
    crs_var.grid_mapping_name = 'latitude_longitude'
    crs_var.epsg_code = 'EPSG:4269'  # NAD83, which is what NHD uses.
    crs_var.semi_major_axis = 6378137.0
    crs_var.inverse_flattening = 298.257222101

    return cf_nc


def write_comid_lat_lon_z(cf_nc, lookup_filename, id_var_name):
    """Add latitude, longitude, and z values for each netCDF feature

    Arguments:
        cf_nc -- netCDF Dataset object to be modified
        lookup_filename -- full path and filename for lookup table
        id_var_name -- name of Id variable

    Remarks:
        Lookup table is a CSV file with COMID, Lat, Lon, and Elev_m columns.
        Columns must be in that order and these must be the first four columns.
    """

    #get list of COMIDS
    lookup_table = csv_to_list(lookup_filename)
    lookup_comids = np.array([int(float(row[0])) for row in lookup_table[1:]])

    # Get relevant arrays while we update them
    nc_comids = cf_nc.variables[id_var_name][:]
    lats = cf_nc.variables['lat'][:]
    lons = cf_nc.variables['lon'][:]
    zs = cf_nc.variables['z'][:]

    lat_min = None
    lat_max = None
    lon_min = None
    lon_max = None
    z_min = None
    z_max = None

    # Process each row in the lookup table
    for nc_index, nc_comid in enumerate(nc_comids):
        try:
            lookup_index = np.where(lookup_comids == nc_comid)[0][0] + 1
        except Exception:
            log('COMID %s misssing in comid_lat_lon_z file' % nc_comid,
                'ERROR')

        lat = float(lookup_table[lookup_index][1])
        lats[nc_index] = lat
        if (lat_min) is None or lat < lat_min:
            lat_min = lat
        if (lat_max) is None or lat > lat_max:
            lat_max = lat

        lon = float(lookup_table[lookup_index][2])
        lons[nc_index] = lon
        if (lon_min) is None or lon < lon_min:
            lon_min = lon
        if (lon_max) is None or lon > lon_max:
            lon_max = lon

        z = float(lookup_table[lookup_index][3])
        zs[nc_index] = z
        if (z_min) is None or z < z_min:
            z_min = z
        if (z_max) is None or z > z_max:
            z_max = z

    # Overwrite netCDF variable values
    cf_nc.variables['lat'][:] = lats
    cf_nc.variables['lon'][:] = lons
    cf_nc.variables['z'][:] = zs

    # Update metadata
    if lat_min is not None:
        cf_nc.geospatial_lat_min = lat_min
    if lat_max is not None:
        cf_nc.geospatial_lat_max = lat_max
    if lon_min is not None:
        cf_nc.geospatial_lon_min = lon_min
    if lon_max is not None:
        cf_nc.geospatial_lon_max = lon_max
    if z_min is not None:
        cf_nc.geospatial_vertical_min = z_min
    if z_max is not None:
        cf_nc.geospatial_vertical_max = z_max

def convert_ecmwf_rapid_output_to_cf_compliant(start_date,
                                               start_folder=None,
                                               time_step=6*3600, #time step in seconds
                                               output_id_dim_name='COMID', #name of ID dimension in output file, typically COMID or FEATUREID
                                               output_flow_var_name='Qout' #name of streamflow variable in output file, typically Qout or m3_riv
                                               ):
    """
    Copies data from RAPID netCDF output to a CF-compliant netCDF file.
    """

    if start_folder:
        path = start_folder
    else:
        path = get_this_path()

    # Get files to process
    inputs = glob(os.path.join(path,"Qout*.nc"))
    if len(inputs) == 0:
        log('No files to process', 'INFO')
        return

    rapid_input_directory = os.path.join(path, "rapid_input")
    #make sure comid_lat_lon_z file exists before proceeding
    try:
        comid_lat_lon_z_lookup_filename = os.path.join(rapid_input_directory,
                                                       [filename for filename in os.listdir(rapid_input_directory) \
                                                        if re.search(r'comid_lat_lon_z.*?\.csv', filename, re.IGNORECASE)][0])
    except IndexError:
        comid_lat_lon_z_lookup_filename = ""
        pass

    if comid_lat_lon_z_lookup_filename:
        for rapid_nc_filename in inputs:
            try:
                cf_nc_filename = '%s_CF.nc' % os.path.splitext(rapid_nc_filename)[0]
                log('Processing %s' % rapid_nc_filename, 'INFO')
                log('New file %s' % cf_nc_filename, 'INFO')
                time_start_conversion = datetime.utcnow()

                # Validate the raw netCDF file
                rapid_nc = Dataset(rapid_nc_filename)
                log('validating input netCDF file', 'DEBUG')
                input_id_dim_name, id_len, time_len, input_flow_var_name = (
                    validate_raw_nc(rapid_nc))

                # Initialize the output file (create dimensions and variables)
                log('initializing output', 'DEBUG')
                cf_nc = initialize_output(cf_nc_filename, output_id_dim_name,
                                          time_len, id_len, time_step)

                # Populate time values
                log('writing times', 'DEBUG')
                total_seconds = time_step * time_len
                end_date = (start_date +
                            timedelta(seconds=(total_seconds - time_step)))
                d1970 = datetime(1970, 1, 1)
                secs_start = int((start_date - d1970).total_seconds())
                secs_end = secs_start + total_seconds
                cf_nc.variables['time'][:] = np.arange(
                    secs_start, secs_end, time_step)
                cf_nc.time_coverage_start = start_date.isoformat() + 'Z'
                cf_nc.time_coverage_end = end_date.isoformat() + 'Z'

                # Populate comid, lat, lon, z
                log('writing comid lat lon z', 'DEBUG')
                lookup_start = datetime.now()
                cf_nc.variables[output_id_dim_name][:] = rapid_nc.variables[input_id_dim_name][:]
                write_comid_lat_lon_z(cf_nc, comid_lat_lon_z_lookup_filename, output_id_dim_name)
                duration = str((datetime.now() - lookup_start).total_seconds())
                log('Lookup Duration (s): ' + duration, 'DEBUG')

                # Create a variable for streamflow. This is big, and slows down
                # previous steps if we do it earlier.
                log('Creating streamflow variable', 'DEBUG')
                q_var = cf_nc.createVariable(
                    output_flow_var_name, 'f4', (output_id_dim_name, 'time'))
                q_var.long_name = 'Discharge'
                q_var.units = 'm^3/s'
                q_var.coordinates = 'time lat lon z'
                q_var.grid_mapping = 'crs'
                q_var.source = ('Generated by the Routing Application for Parallel ' +
                                'computatIon of Discharge (RAPID) river routing model.')
                q_var.references = 'http://rapid-hub.org/'
                q_var.comment = ('lat, lon, and z values taken at midpoint of river ' +
                                 'reach feature')
                log('Copying streamflow values', 'DEBUG')
                q_var[:] = rapid_nc.variables[input_flow_var_name][:].transpose()
                rapid_nc.close()

                cf_nc.close()
                #delete original RAPID output
                try:
                    os.remove(rapid_nc_filename)
                except OSError:
                    pass

                #replace original with nc compliant file
                shutil.move(cf_nc_filename, rapid_nc_filename)
                log('Time to process %s' % (datetime.utcnow()-time_start_conversion), 'INFO')
            except Exception, e:
                #delete cf RAPID output
                try:
                    os.remove(cf_nc_filename)
                except OSError:
                    pass
                log('Error in main function %s' % e, 'WARNING')
                raise
    else:
        log("No comid_lat_lon_z file found. Skipping ...", "INFO")

    log('Files processed: ' + str(len(inputs)), 'INFO')

if __name__ == "__main__":
    convert_ecmwf_rapid_output_to_cf_compliant(start_date=datetime(1980,1,1),
                                               start_folder='/Users/Alan/Documents/RESEARCH/RAPID/input/nfie_texas_gulf_region/rapid_updated'
                                               )