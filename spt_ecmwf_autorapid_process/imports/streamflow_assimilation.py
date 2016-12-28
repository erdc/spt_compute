# -*- coding: utf-8 -*-
#
#  ftp_ecmwf_download.py
#  spt_ecmwf_autorapid_process
#
#  Created by Alan D. Snow
#  License: BSD-3 Clause

from calendar import isleap
import datetime
from dateutil.parser import parse
from glob import glob
from netCDF4 import Dataset
import numpy as np
import os
from pytz import utc
import requests
from time import gmtime

from RAPIDpy.rapid import RAPID
from RAPIDpy.dataset import RAPIDDataset
from RAPIDpy.helper_functions import csv_to_list

from .helper_functions import get_datetime_from_date_timestep


# -----------------------------------------------------------------------------------------------------
# StreamSegment Class
# -----------------------------------------------------------------------------------------------------
class StreamSegment(object):
    def __init__(self, stream_id, down_id, up_id_array, init_flow=0, 
                 station=None, station_flow=None, station_distance=None, natural_flow=None):
        self.stream_id = stream_id
        self.down_id = down_id  # downstream segment id
        self.up_id_array = up_id_array  # array of stream ids for upstream segments
        self.init_flow = init_flow
        self.station = station
        self.station_flow = station_flow
        self.station_distance = station_distance  # number of stream segments to station
        self.natural_flow = natural_flow


class StreamGage(object):
    """
    Base class for stream gage object
    """
    def __init__(self, station_id):
        self.station_id = station_id
        
    def get_gage_data(self, datetime_tzinfo_object):
        """
        Get gage data based on stream gage type
        """
        return None


class USGSStreamGage(StreamGage):
    """
    USGS Gage object
    """
    def __init__(self, station_id):
        if len(station_id) == 7:
            station_id = "0" + station_id
        super(USGSStreamGage, self).__init__(station_id)
    
    def get_gage_data(self, datetime_tzinfo_object):
        """
        Get USGS gage data 
        """
        datetime_end_string = datetime_tzinfo_object.strftime("%Y-%m-%d")
        datetime_start_string = (datetime_tzinfo_object-datetime.timedelta(1)).strftime("%Y-%m-%d")
        datetime_1970 = datetime.datetime(1970, 1, 1, tzinfo=utc)
        query_params = {
                        'format': 'json',
                        'sites': self.station_id,
                        'startDT': datetime_start_string,
                        'endDT': datetime_end_string,
                        'parameterCd': '00060',
                       }
        response = requests.get("http://waterservices.usgs.gov/nwis/iv/", params=query_params)
        if response.ok:
            data_valid = True
            try:
                requested_data = response.json()['value']['timeSeries'][0]['values'][0]['value']
            except IndexError:
                data_valid = False
                pass
            if data_valid:
                prev_time_step = None
                for time_step in requested_data:
                    datetime_obj = parse(time_step['dateTime'])
                    if datetime_obj == datetime_tzinfo_object:
                        if float(time_step['value']) > 0:
                            # get value and convert to metric
                            return float(time_step['value'])/35.3146667
                        break
                    elif datetime_obj > datetime_tzinfo_object:
                        if prev_time_step is not None:
                            prev_datetime = parse(prev_time_step['dateTime'])
                            if (datetime_obj - prev_datetime) < datetime.timedelta(hours=1):
                                # linear interpolation if less than 1 hour difference between points
                                needed_time = (datetime_tzinfo_object-datetime_1970).total_seconds()
                                prev_time = (prev_datetime - datetime_1970).total_seconds()
                                prev_flow = float(prev_time_step['value'])/35.3146667
                                next_time = (datetime_obj - datetime_1970).total_seconds()
                                next_flow = float(time_step['value'])/35.3146667
                                estimated_flow = (next_flow-prev_flow)*(needed_time-prev_time)/(next_time-prev_time) \
                                                 + prev_flow

                                return estimated_flow
                        break
                    prev_time_step = time_step

        return None
    

# -----------------------------------------------------------------------------------------------------
# StreamNetworkInitializer Class
# -----------------------------------------------------------------------------------------------------
class StreamNetworkInitializer(object):
    def __init__(self, connectivity_file, gage_ids_natur_flow_file=None):
        # files
        self.connectivity_file = connectivity_file
        self.gage_ids_natur_flow_file = gage_ids_natur_flow_file
        # variables
        self.stream_segments = []
        self.outlet_id_list = []
        self.stream_undex_with_usgs_station = []
        self.stream_id_array = None
        
        # generate the network
        self._generate_network_from_connectivity()
        
        # add gage id and natur flow to network
        if gage_ids_natur_flow_file is not None:
            if os.path.exists(gage_ids_natur_flow_file) and gage_ids_natur_flow_file:
                self._add_gage_ids_natur_flow_to_network()
        
    def _find_stream_segment_index(self, stream_id):
        """
        Finds the index of a stream segment in 
        the list of stream segment ids
        """
        try:
            # get where stream index is in list
            stream_index = np.where(self.stream_id_array==stream_id)[0][0]
            # return the stream segment index
            return stream_index
        except Exception:
            # stream_id not found in list.
            return None

    def _generate_network_from_connectivity(self):
        """
        Generate river network from connectivity file
        """
        print("Generating river network from connectivity file ...")
        connectivity_table = csv_to_list(self.connectivity_file)
        self.stream_id_array = np.array([row[0] for row in connectivity_table], dtype=np.int)
        # add each stream segment to network
        for connectivity_info in connectivity_table:
            stream_id = int(connectivity_info[0])
            downstream_id = int(connectivity_info[1])
            # add outlet to list of outlets if downstream id is zero
            if downstream_id == 0:
                self.outlet_id_list.append(stream_id)
                
            self.stream_segments.append(StreamSegment(stream_id=stream_id,
                                                      down_id=downstream_id,
                                                      up_id_array=connectivity_info[2:2+int(connectivity_info[2])]))

    def _add_gage_ids_natur_flow_to_network(self):
        """
        This adds gage and natural flow information 
        to the network from the file
        """
        print("Adding Gage Station and Natur Flow info from: {0}".format(self.gage_ids_natur_flow_file))
        gage_id_natur_flow_table = csv_to_list(self.gage_ids_natur_flow_file)
        for stream_info in gage_id_natur_flow_table[1:]:
            if stream_info[0] != "":
                stream_index = self._find_stream_segment_index(int(float(stream_info[0])))
                if stream_index is not None:
                    # add natural flow
                    self.stream_segments[stream_index].natural_flow = int(float(stream_info[1]))
                    # add station id
                    try:
                        station_id = str(int(float(stream_info[2])))
                    except Exception:
                        continue
                        pass
                    if station_id != "":
                        self.stream_undex_with_usgs_station.append(stream_index)
                        self.stream_segments[stream_index].station = USGSStreamGage(station_id)
                        # removed: don't add unless valid data aquired
                        # self.stream_segments[stream_index].station_distance = 0
    
    def add_usgs_flows(self, datetime_tzinfo_object):
        """
        Based on the stream_id, query USGS to get the flows for the date of interest
        """
        print("Adding USGS flows to network ...")
        # datetime_end = datetime.datetime(2015, 8, 20, tzinfo=utc)
        for stream_index in self.stream_undex_with_usgs_station:
            station_flow = self.stream_segments[stream_index].station.get_gage_data(datetime_tzinfo_object)
            if station_flow is not None:
                self.stream_segments[stream_index].station_flow = station_flow
                self.stream_segments[stream_index].station_distance = 0
        
    def read_init_flows_from_past_forecast(self, init_flow_file_path):
        """
        Read in initial flows from the past ECMWF forecast ensemble
        """
        print("Reading in initial flows from forecast ...")
        with open(init_flow_file_path, 'r') as init_flow_file:
            for index, line in enumerate(init_flow_file):
                line = line.strip()
                if line:
                    self.stream_segments[index].init_flow = float(line) 
                
    def compute_init_flows_from_past_forecast(self, forecasted_streamflow_files):
        """
        Compute initial flows from the past ECMWF forecast ensemble
        """
        if forecasted_streamflow_files:
            # get list of COMIDS
            print("Computing initial flows from the past ECMWF forecast ensemble ...")
            with RAPIDDataset(forecasted_streamflow_files[0]) as qout_nc:
                comid_index_list, reordered_comid_list, ignored_comid_list = \
                    qout_nc.get_subset_riverid_index_list(self.stream_id_array)
            print("Extracting data ...")
            reach_prediciton_array = np.zeros((len(self.stream_id_array),len(forecasted_streamflow_files),1))
            # get information from datasets
            for file_index, forecasted_streamflow_file in enumerate(forecasted_streamflow_files):
                try:
                    ensemble_index = int(os.path.basename(forecasted_streamflow_file).split(".")[0].split("_")[-1])
                    try:
                        # Get hydrograph data from ECMWF Ensemble
                        with RAPIDDataset(forecasted_streamflow_file) as predicted_qout_nc:
                            time_length = predicted_qout_nc.size_time
                            if not predicted_qout_nc.is_time_variable_valid():
                                # data is raw rapid output
                                data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list, 
                                                                                        time_index=1)
                            else:
                                # the data is CF compliant and has time=0 added to output
                                if ensemble_index == 52:
                                    if time_length == 125:
                                        data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list, 
                                                                                                time_index=12)
                                    else:
                                        data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list, 
                                                                                                time_index=2)
                                else:
                                    if time_length == 85:
                                        data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list, 
                                                                                                time_index=4)
                                    else:
                                        data_values_2d_array = predicted_qout_nc.get_qout_index(comid_index_list, 
                                                                                                time_index=2)
                    except Exception:
                        print("Invalid ECMWF forecast file {0}".format(forecasted_streamflow_file))
                        continue
                    # organize the data
                    for comid_index, comid in enumerate(reordered_comid_list):
                        reach_prediciton_array[comid_index][file_index] = data_values_2d_array[comid_index]
                except Exception as e:
                    print(e)
                    # pass
    
            print("Analyzing data ...")
            for index in range(len(self.stream_segments)):
                try:
                    # get where comids are in netcdf file
                    data_index = np.where(reordered_comid_list==self.stream_segments[index].stream_id)[0][0]
                    self.stream_segments[index].init_flow = np.mean(reach_prediciton_array[data_index])
                except Exception:
                    # stream id not found in list. Adding zero init flow ...
                    self.stream_segments[index].init_flow = 0
                    pass
                    continue
            
            print("Initialization Complete!")
        
    def generate_qinit_from_seasonal_average(self, seasonal_average_file):
        """
        Generate initial flows from seasonal average file
        """
        var_time = gmtime()
        yday_index = var_time.tm_yday - 1  # convert from 1-366 to 0-365
        # move day back one past because of leap year adds
        # a day after feb 29 (day 60, but index 59)
        if isleap(var_time.tm_year) and yday_index > 59:
            yday_index -= 1

        seasonal_nc = Dataset(seasonal_average_file)
        nc_rivid_array = seasonal_nc.variables['rivid'][:]
        seasonal_qout_average_array = seasonal_nc.variables['average_flow'][:,yday_index]
        
        for index in range(len(self.stream_segments)):
            try:
                # get where comids are in netcdf file
                data_index = np.where(nc_rivid_array==self.stream_segments[index].stream_id)[0][0]
                self.stream_segments[index].init_flow = seasonal_qout_average_array[data_index]
            except Exception:
                # stream id not found in list. Adding zero init flow ...
                self.stream_segments[index].init_flow = 0
                pass
                continue

    def modify_flow_connected(self, stream_id, master_station_flow, master_error, master_natur_flow):
        """
        IModify connected stream segment with gage data
        """
        connected_segment_index = self._find_stream_segment_index(stream_id)
        if connected_segment_index is not None:
            if self.stream_segments[connected_segment_index].station_distance != 0:
                connected_natur_flow = self.stream_segments[connected_segment_index].natural_flow 
                if connected_natur_flow is not None and master_natur_flow:
                    self.stream_segments[connected_segment_index].station_flow = \
                        max(0, self.stream_segments[connected_segment_index].init_flow +
                            master_error*connected_natur_flow/master_natur_flow)
                else:
                    self.stream_segments[connected_segment_index].station_flow = master_station_flow

    def modify_init_flows_from_gage_flows(self):
        """
        If gage flow data is available, use the gage data to modify surrounding 
        stream segments with error
        """
        print("Modifying surrounding sreams with gage data ...")
        for stream_index in self.stream_undex_with_usgs_station:
            if self.stream_segments[stream_index].station_distance == 0:
                master_natur_flow = self.stream_segments[stream_index].natural_flow
                master_station_flow = self.stream_segments[stream_index].station_flow
                master_init_flow = self.stream_segments[stream_index].init_flow
                master_error = 0
                if master_natur_flow:
                    master_error = master_station_flow - master_init_flow
                   
                # modify upstream segments
                for updtream_segment_id in self.stream_segments[stream_index].up_id_array:
                    self.modify_flow_connected(updtream_segment_id, 
                                               master_station_flow, 
                                               master_error,
                                               master_natur_flow)
                # modify downstream segments
                self.modify_flow_connected(self.stream_segments[stream_index].down_id, 
                                           master_station_flow, 
                                           master_error,
                                           master_natur_flow)

    def write_init_flow_file(self, out_file):
        """
        Write initial flow file
        """
        print("Writing to initial flow file: {0}".format(out_file))
        with open(out_file, 'wb') as init_flow_file:
            for stream_index, stream_segment in enumerate(self.stream_segments):
                if stream_segment.station_flow is not None:
                    init_flow_file.write("{0}\n".format(stream_segment.station_flow))
                else:                            
                    init_flow_file.write("{0}\n".format(stream_segment.init_flow))
        
        
# -----------------------------------------------------------------------------------------------------
# Streamflow Init Functions
# -----------------------------------------------------------------------------------------------------
def compute_initial_rapid_flows(prediction_files, input_directory, forecast_date_timestep):
    """
    Gets mean of all 52 ensembles 12-hrs in future and prints to csv as initial flow
    Qinit_file (BS_opt_Qinit)
    The assumptions are that Qinit_file is ordered the same way as rapid_connect_file
    if subset of list, add zero where there is no flow
    """
    # remove old init files for this basin
    past_init_flow_files = glob(os.path.join(input_directory, 'Qinit_*.csv'))
    for past_init_flow_file in past_init_flow_files:
        try:
            os.remove(past_init_flow_file)
        except:
            pass
    current_forecast_date = get_datetime_from_date_timestep(forecast_date_timestep)
    current_forecast_date_string = current_forecast_date.strftime("%Y%m%dt%H")
    init_file_location = os.path.join(input_directory, 'Qinit_{0}.csv'.format(current_forecast_date_string))
    # check to see if exists and only perform operation once
    if prediction_files:
        sni = StreamNetworkInitializer(connectivity_file=os.path.join(input_directory, 'rapid_connect.csv'))
        sni.compute_init_flows_from_past_forecast(prediction_files)
        sni.write_init_flow_file(init_file_location)        
    else:
        print("No current forecasts found. Skipping ...")


def compute_seasonal_initial_rapid_flows(historical_qout_file, input_directory, init_file_location):
    """
    Gets the seasonal average from historical file to initialize from
    """
    if not os.path.exists(init_file_location):
        # check to see if exists and only perform operation once
        if historical_qout_file and os.path.exists(historical_qout_file):
            rapid_manager = RAPID(Qout_file=historical_qout_file,
                                  rapid_connect_file=os.path.join(input_directory, 'rapid_connect.csv'))
            rapid_manager.generate_seasonal_intitialization(init_file_location)
        else:
            print("No historical streamflow file found. Skipping ...")


def generate_initial_rapid_flow_from_seasonal_average(seasonal_average_file, input_directory, init_file_location):
    """
    Generates a qinit file from seasonal average file
    """
    if not os.path.exists(init_file_location):
        # check to see if exists and only perform operation once
        if seasonal_average_file and os.path.exists(seasonal_average_file):
            # Generate initial flow from seasonal average file
            sni = StreamNetworkInitializer(connectivity_file=os.path.join(input_directory, 'rapid_connect.csv'))
            sni.generate_qinit_from_seasonal_average(seasonal_average_file)
            sni.write_init_flow_file(init_file_location)        
        else:
            print("No seasonal streamflow file found. Skipping ...")


def compute_seasonal_initial_rapid_flows_multicore_worker(args):
    """
    Worker function using mutliprocessing for compute_seasonal_initial_rapid_flows
    """
    input_directory = args[1]
    forecast_date_timestep = args[2]
    
    current_forecast_date = get_datetime_from_date_timestep(forecast_date_timestep)
    # move the date back a forecast (12 hrs) to be used in this forecast
    forecast_date_string = (current_forecast_date-datetime.timedelta(seconds=12*3600)).strftime("%Y%m%dt%H")
    init_file_location = os.path.join(input_directory, 'Qinit_{0}.csv'.format(forecast_date_string))

    if args[3] == "seasonal_average_file":
        generate_initial_rapid_flow_from_seasonal_average(args[0], input_directory, init_file_location)
        
    elif args[3] == "historical_streamflow_file":
        compute_seasonal_initial_rapid_flows(args[0], input_directory, init_file_location)


def update_inital_flows_usgs(input_directory, forecast_date_timestep):
    """
    Update initial flows with USGS data
    """
    gage_flow_info = os.path.join(input_directory, 'usgs_gages.csv')
    current_forecast_date = get_datetime_from_date_timestep(forecast_date_timestep).replace(tzinfo=utc)
    past_date = (current_forecast_date - datetime.timedelta(hours=12)).strftime("%Y%m%dt%H")

    qinit_file = os.path.join(input_directory, 'Qinit_{0}.csv'.format(past_date))

    if os.path.exists(gage_flow_info) and os.path.exists(qinit_file):
        print("Updating initial flows with USGS data for: {0} {1} ...".format(input_directory, 
                                                                              forecast_date_timestep))
              
        sni = StreamNetworkInitializer(connectivity_file=os.path.join(input_directory, 'rapid_connect.csv'),
                                       gage_ids_natur_flow_file=gage_flow_info)
        sni.read_init_flows_from_past_forecast(qinit_file)
        sni.add_usgs_flows(current_forecast_date)
        sni.modify_init_flows_from_gage_flows()
        try:
            os.remove(qinit_file)
        except OSError:
            pass
        
        sni.write_init_flow_file(qinit_file)        
