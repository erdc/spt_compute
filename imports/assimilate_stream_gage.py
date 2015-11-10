# -*- coding: utf-8 -*-
import csv
import datetime
from dateutil.parser import parse
from glob import glob
import netCDF4 as NET
import numpy as np
import os
from pytz import utc
import requests


#-----------------------------------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------------------------------
def csv_to_list(csv_file, delimiter=','):
    """
    Reads in a CSV file and returns the contents as list,
    where every row is stored as a sublist, and each element
    in the sublist represents 1 cell in the table.

    """
    with open(csv_file, 'rb') as csv_con:
        reader = csv.reader(csv_con, delimiter=delimiter)
        return list(reader)

def get_comids_in_netcdf_file(reach_id_list, prediction_file):
    """
    Gets the subset comid_index_list, reordered_comid_list from the netcdf file
    """
    data_nc = NET.Dataset(prediction_file, mode="r")
    com_ids = data_nc.variables['COMID'][:]
    data_nc.close()
    try:
        #get where comids are in netcdf file
        netcdf_reach_indices_list = np.where(np.in1d(com_ids, reach_id_list))[0]
    except Exception as ex:
        print ex

    return netcdf_reach_indices_list, com_ids[netcdf_reach_indices_list]


#-----------------------------------------------------------------------------------------------------
# StreamSegment Class
#-----------------------------------------------------------------------------------------------------
class StreamSegment(object):
    def __init__(self, stream_id, down_id, up_id_array, init_flow=0, 
                 station=None, station_flow=None, station_distance=None, natural_flow=None):
        self.stream_id = stream_id
        self.down_id = down_id #downstream segment id
        self.up_id_array = up_id_array #array of atream ids for upstream segments
        self.init_flow = init_flow
        self.station = station
        self.station_flow = station_flow
        self.station_distance = station_distance #number of tream segments to station
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
                            #get value and convert to metric
                            return float(time_step['value'])/35.3146667
                        break
                    elif datetime_obj > datetime_tzinfo_object:
                        if prev_time_step != None:
                            prev_datetime = parse(prev_time_step['dateTime'])
                            if (datetime_obj - prev_datetime) < datetime.timedelta(hours=1):
                                #linear interpolation if less than 1 hour difference between points
                                needed_time = (datetime_tzinfo_object-datetime_1970).total_seconds()
                                prev_time = (prev_datetime - datetime_1970).total_seconds()
                                prev_flow = float(prev_time_step['value'])/35.3146667
                                next_time = (datetime_obj - datetime_1970).total_seconds()
                                next_flow = float(time_step['value'])/35.3146667
                                estimated_flow = (next_flow-prev_flow)*(needed_time-prev_time)/(next_time-prev_time) + prev_flow
                                return estimated_flow
                        break
                    prev_time_step = time_step

        return None
    
#-----------------------------------------------------------------------------------------------------
# StreamNetworkInitializer Class
#-----------------------------------------------------------------------------------------------------
class StreamNetworkInitializer(object):
    def __init__(self, connectivity_file, gage_ids_natur_flow_file=None):
        #files
        self.connectivity_file = connectivity_file
        self.gage_ids_natur_flow_file = gage_ids_natur_flow_file
        #variables
        self.stream_segments = []
        self.outlet_id_list = []
        self.stream_undex_with_usgs_station = []
        self.stream_id_array = None
        
        #generate the network
        self._generate_network_from_connectivity()
        
        #add gage id and natur flow to network
        if gage_ids_natur_flow_file != None:
            if os.path.exists(gage_ids_natur_flow_file) and gage_ids_natur_flow_file:
                self._add_gage_ids_natur_flow_to_network()
        
    def _find_stream_segment_index(self, stream_id):
        """
        Finds the index of a stream segment in 
        the list of stream segment ids
        """
        try:
            #get where stream index is in list
            stream_index = np.where(self.stream_id_array==stream_id)[0][0]
            #return the stream segment index
            return stream_index
        except Exception:
            #stream_id not found in list.
            return None

    def _generate_network_from_connectivity(self):
        """
        Generate river network from connectivity file
        """
        print "Generating river network from connectivity file ..."
        connectivity_table = csv_to_list(self.connectivity_file)
        self.stream_id_array = np.array([row[0] for row in connectivity_table], dtype=np.int)
        #add each stream segment to network
        for connectivity_info in connectivity_table:
            stream_id = int(connectivity_info[0])
            downstream_id = int(connectivity_info[1])
            #add outlet to list of outlets if downstream id is zero
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
        print "Adding Gage Station and Natur Flow info from:" , self.gage_ids_natur_flow_file
        gage_id_natur_flow_table = csv_to_list(self.gage_ids_natur_flow_file)
        for stream_info in gage_id_natur_flow_table[1:]:
            if stream_info[0] != "":
                stream_index = self._find_stream_segment_index(int(float(stream_info[0])))
                if stream_index != None:
                    #add natural flow
                    self.stream_segments[stream_index].natural_flow = int(float(stream_info[1]))
                    #add station id
                    try:
                        station_id = str(int(float(stream_info[2])))
                    except Exception:
                        continue
                        pass
                    if station_id != "":
                        self.stream_undex_with_usgs_station.append(stream_index)
                        self.stream_segments[stream_index].station = USGSStreamGage(station_id)
                        #removed: don't add unless valid data aquired
                        #self.stream_segments[stream_index].station_distance = 0
    
    def add_usgs_flows(self, datetime_tzinfo_object):
        """
        Based on the stream_id, query USGS to get the flows for the date of interest
        """
        print "Adding USGS flows to network ..."
        #datetime_end = datetime.datetime(2015, 8, 20, tzinfo=utc)
        num_printed = 0
        for stream_index in self.stream_undex_with_usgs_station:
            station_flow = self.stream_segments[stream_index].station.get_gage_data(datetime_tzinfo_object)
            if station_flow != None:
                self.stream_segments[stream_index].station_flow = station_flow
                self.stream_segments[stream_index].station_distance = 0
                if num_printed < 10:
                    print stream_index, self.stream_segments[stream_index].stream_id, \
                        self.stream_segments[stream_index].station.station_id, station_flow, self.stream_segments[stream_index].init_flow
                num_printed += 1
        
    def read_init_flows_from_past_forecast(self, init_flow_file_path):
        """
        Read in initial flows from the past ECMWF forecast ensemble
        """
        print "Readin in initial flows from forecast ..."
        with open(init_flow_file_path, 'r') as init_flow_file:
            for index, line in enumerate(init_flow_file):
                line = line.strip()
                if line:
                    self.stream_segments[index].init_flow = float(line) 
                
                

    def compute_init_flows_from_past_forecast(self, prediction_files):
        """
        Compute initial flows from the past ECMWF forecast ensemble
        """
        if prediction_files:
            #get list of COMIDS
            print "Computing initial flows from the past ECMWF forecast ensemble ..."
            comid_index_list, reordered_comid_list = get_comids_in_netcdf_file(self.stream_id_array, prediction_files[0])
            print "Extracting data ..."
            reach_prediciton_array = np.zeros((len(self.stream_id_array),len(prediction_files),1))
            #get information from datasets
            for file_index, prediction_file in enumerate(prediction_files):
                try:
                    ensebmle_index_str = os.path.basename(prediction_file)[:-3].split("_")[-1]
                    ensemble_index = int(ensebmle_index_str)
                    #Get hydrograph data from ECMWF Ensemble
                    data_nc = NET.Dataset(prediction_file, mode="r")
                    time_length = len(data_nc.variables['time'][:])
                    qout_dimensions = data_nc.variables['Qout'].dimensions
                    if qout_dimensions[0].lower() == 'time' and qout_dimensions[1].lower() == 'comid':
                        #data is raw rapid output
                        data_values_2d_array = data_nc.variables['Qout'][1,comid_index_list].transpose()
                    elif qout_dimensions[1].lower() == 'time' and qout_dimensions[0].lower() == 'comid':
                        #the data is CF compliant and has time=0 added to output
                        if ensemble_index == 52:
                            if time_length == 125:
                                data_values_2d_array = data_nc.variables['Qout'][comid_index_list,12]
                            else:
                                data_values_2d_array = data_nc.variables['Qout'][comid_index_list,2]
                        else:
                            if time_length == 85:
                                data_values_2d_array = data_nc.variables['Qout'][comid_index_list,4]
                            else:
                                data_values_2d_array = data_nc.variables['Qout'][comid_index_list,2]
                    else:
                        print "Invalid ECMWF forecast file", prediction_file
                        data_nc.close()
                        continue
                    data_nc.close()
                    #organize the data
                    for comid_index, comid in enumerate(reordered_comid_list):
                        reach_prediciton_array[comid_index][file_index] = data_values_2d_array[comid_index]
                except Exception, e:
                    print e
                    #pass
    
            print "Analyzing data ..."
            for index in range(len(self.stream_segments)):
                try:
                    #get where comids are in netcdf file
                    data_index = np.where(reordered_comid_list==self.stream_segments[index].stream_id)[0][0]
                    self.stream_segments[index].init_flow = np.mean(reach_prediciton_array[data_index])
                except Exception:
                    #stream id not found in list. Adding zero init flow ...
                    self.stream_segments[index].init_flow = 0
                    pass
                    continue
            
            print "Initialization Complete!"
        
        
    def modify_flow_connected(self, stream_id, master_station_flow, master_error, master_natur_flow):
        """
        IModify connected stream segment with gage data
        """
        connected_segment_index = self._find_stream_segment_index(stream_id)
        if connected_segment_index != None:
            if self.stream_segments[connected_segment_index].station_distance != 0:
                connected_natur_flow = self.stream_segments[connected_segment_index].natural_flow 
                if connected_natur_flow != None and master_natur_flow:
                    self.stream_segments[connected_segment_index].station_flow = max(0, self.stream_segments[connected_segment_index].init_flow + master_error*connected_natur_flow/master_natur_flow)
                else:
                    self.stream_segments[connected_segment_index].station_flow = master_station_flow

    def modify_init_flows_from_gage_flows(self):
        """
        If gage flow data is available, use the gage data to modify surrounding 
        stream segments with error
        """
        print "Modifying surrounding sreams with gage data ..."
        for stream_index in self.stream_undex_with_usgs_station:
            if self.stream_segments[stream_index].station_distance == 0:
                master_natur_flow = self.stream_segments[stream_index].natural_flow
                master_station_flow = self.stream_segments[stream_index].station_flow
                master_init_flow = self.stream_segments[stream_index].init_flow
                master_error = 0
                if master_natur_flow:
                   master_error = master_station_flow - master_init_flow
                   
                #modify upstream segments
                for updtream_segment_id in self.stream_segments[stream_index].up_id_array:
                    self.modify_flow_connected(updtream_segment_id, 
                                               master_station_flow, 
                                               master_error,
                                               master_natur_flow)
                #modify downstream segments
                self.modify_flow_connected(self.stream_segments[stream_index].down_id, 
                                           master_station_flow, 
                                           master_error,
                                           master_natur_flow)


    def write_init_flow_file(self, out_file):
        """
        Print initial flow file
        """
        print "Writing to initial flow file:", out_file
        num_printed = 0
        with open(out_file, 'wb') as init_flow_file:
            for stream_index, stream_segment in enumerate(self.stream_segments):
                if stream_segment.station_flow != None:
                    init_flow_file.write("{}\n".format(stream_segment.station_flow))
                    if num_printed < 10:
                        print stream_index, stream_segment.stream_id, stream_segment.station_flow, stream_segment.init_flow
                    num_printed += 1
                else:                            
                    init_flow_file.write("{}\n".format(stream_segment.init_flow))
        
        
        
        
if __name__=="__main__":
        connect_file = '/home/alan/work/rapid-io/input/erdc_texas_gulf_region-huc_2_12/rapid_connect.csv'
        gage_flow_info = '/home/alan/work/rapid-io/input/erdc_texas_gulf_region-huc_2_12/usgs_gages.csv'
        sni = StreamNetworkInitializer(connectivity_file=connect_file, gage_ids_natur_flow_file=gage_flow_info)
        path_to_predictions = '/home/alan/tethysdev/tethysapp-erfp_tool/rapid_files/ecmwf_prediction/erdc_texas_gulf_region/huc_2_12/20150825.1200'
        prediction_files = glob(os.path.join(path_to_predictions, "*.nc"))
        sni.compute_init_flows_from_past_forecast(prediction_files)
        raw_initialization_file =  '/home/alan/work/rapid-io/input/erdc_texas_gulf_region-huc_2_12/Qinit_20150825t12_orig.csv'
        sni.write_init_flow_file(raw_initialization_file)
        #sni.read_init_flows_from_past_forecast(raw_initialization_file)
        sni.add_usgs_flows(datetime.datetime(2015,8,26,0, tzinfo=utc))
        sni.modify_init_flows_from_gage_flows()
        #usgs_initialization_file =  '/Users/rdchlads/Documents/nfie_texas_gulf_initialization_test/usgs_init.csv'
        #sni.write_init_flow_file(usgs_initialization_file)        
        usgs_mod_initialization_file =  '/home/alan/work/rapid-io/input/erdc_texas_gulf_region-huc_2_12/Qinit_20150825t12_usgs.csv'
        sni.write_init_flow_file(usgs_mod_initialization_file)        
        