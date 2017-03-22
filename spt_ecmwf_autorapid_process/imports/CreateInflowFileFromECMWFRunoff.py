'''-------------------------------------------------------------------------------
 Tool Name:   CreateInflowFileFromECMWFRunoff
 Source Name: CreateInflowFileFromECMWFRunoff.py
 Version:     ArcGIS 10.3
 Author:      Environmental Systems Research Institute Inc.
 Updated by:  Alan D. Snow, US Army ERDC
 Description: Creates RAPID inflow file based on the WRF_Hydro land model output
              and the weight table previously created.
 History:     Initial coding - 10/21/2014, version 1.0
 Updated:     Version 1.0, 10/23/2014, modified names of tool and parameters
              Version 1.0, 10/28/2014, added data validation
              Version 1.0, 10/30/2014, initial version completed
              Version 1.1, 11/05/2014, modified the algorithm for extracting runoff
                variable from the netcdf dataset to improve computation efficiency
              Version 1.2, 02/03/2015, bug fixing - output netcdf3-classic instead
                of netcdf4 as the format of RAPID inflow file
              Version 1.2, 02/03/2015, bug fixing - calculate inflow assuming that
                ECMWF runoff data is cumulative instead of incremental through time
-------------------------------------------------------------------------------'''
import netCDF4 as NET
import numpy as NUM
import csv

class CreateInflowFileFromECMWFRunoff(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Inflow File From ECMWF Runoff"
        self.description = ("Creates RAPID NetCDF input of water inflow " +
                       "based on ECMWF runoff results and previously created weight table.")
        self.canRunInBackground = False
        self.header_wt = ['StreamID', 'area_sqm', 'lon_index', 'lat_index', 'npoints']
        self.dims_oi = [['lon', 'lat', 'time'], ['longitude', 'latitude', 'time']]
        self.vars_oi = [["lon", "lat", "time", "RO"], ['longitude', 'latitude', 'time', 'ro']]
        self.length_time = {"LowRes": 61, "LowResFull": 85,"HighRes": 125}
        self.length_time_opt = {"LowRes-6hr": 60,
                                "LowResFull-3hr-Sub": 48, "LowResFull-6hr-Sub": 36,
                                "HighRes-1hr": 90, "HighRes-3hr": 48, "HighRes-6hr": 40, 
                                "HighRes-3hr-Sub": 18, "HighRes-6hr-Sub": 16}
        self.errorMessages = ["Missing Variable 'time'",
                              "Incorrect dimensions in the input ECMWF runoff file.",
                              "Incorrect variables in the input ECMWF runoff file.",
                              "Incorrect time variable in the input ECMWF runoff file",
                              "Incorrect number of columns in the weight table",
                              "No or incorrect header in the weight table",
                              "Incorrect sequence of rows in the weight table"]


    def dataValidation(self, in_nc):
        """Check the necessary dimensions and variables in the input netcdf data"""
        vars_oi_index = None

        data_nc = NET.Dataset(in_nc)
        
        dims = data_nc.dimensions.keys()
        if dims not in self.dims_oi:
            raise Exception(self.errorMessages[1])

        vars = data_nc.variables.keys()
        if vars == self.vars_oi[0]:
            vars_oi_index = 0
        elif vars == self.vars_oi[1]:
            vars_oi_index = 1
        else:    
            raise Exception(self.errorMessages[2])

        return vars_oi_index


    def dataIdentify(self, in_nc):
        """Check if the data is Ensemble 1-51 (low resolution) or 52 (high resolution)"""
        data_nc = NET.Dataset(in_nc)
        time = data_nc.variables['time'][:]
        diff = NUM.unique(NUM.diff(time))
        data_nc.close()
        time_interval_highres = NUM.array([1.0,3.0,6.0],dtype=float)
        time_interval_lowres_full = NUM.array([3.0, 6.0],dtype=float)
        time_interval_lowres = NUM.array([6.0],dtype=float)
        if NUM.array_equal(diff, time_interval_highres):
            return "HighRes"
        elif NUM.array_equal(diff, time_interval_lowres_full):
            return "LowResFull"
        elif NUM.array_equal(diff, time_interval_lowres):
            return "LowRes"
        else:
            return None
            
    def getGridName(self, in_nc, high_res=False):
        """Return name of grid"""
        if high_res:
            return 'ecmwf_t1279'
        return 'ecmwf_tco639'


    def execute(self, in_nc, in_weight_table, out_nc, grid_name, in_time_interval="6hr"):
        """The source code of the tool."""

        # Validate the netcdf dataset
        vars_oi_index = self.dataValidation(in_nc)
        
        #get conversion factor
        conversion_factor = 1.0
        if grid_name == 'ecmwf_t1279' or grid_name == 'ecmwf_tco639':
            #new grids in mm instead of m
            conversion_factor = 0.001

        # identify if the input netcdf data is the High Resolution data with three different time intervals
        id_data = self.dataIdentify(in_nc)
        if id_data is None:
            raise Exception(self.errorMessages[3])

        ''' Read the netcdf dataset'''
        data_in_nc = NET.Dataset(in_nc)
        time = data_in_nc.variables['time'][:]

        # Check the size of time variable in the netcdf data
        if len(time) != self.length_time[id_data]:
            raise Exception(self.errorMessages[3])


        ''' Read the weight table '''
        print("Reading the weight table...")
        dict_list = {self.header_wt[0]:[], self.header_wt[1]:[], self.header_wt[2]:[],
                     self.header_wt[3]:[], self.header_wt[4]:[]}

        with open(in_weight_table, "rb") as csvfile:
            reader = csv.reader(csvfile)
            count = 0
            for row in reader:
                if count == 0:
                    #check number of columns in the weight table
                    if len(row) < len(self.header_wt):
                        raise Exception(self.errorMessages[4])
                    #check header
                    if row[1:len(self.header_wt)] != self.header_wt[1:]:
                        raise Exception(self.errorMessages[5])
                    count += 1
                else:
                    for i in xrange(len(self.header_wt)):
                       dict_list[self.header_wt[i]].append(row[i])
                    count += 1

        '''Calculate water inflows'''
        print("Calculating water inflows...")

        # Obtain size information
        if id_data == "LowRes":
            size_time = self.length_time_opt["LowRes-6hr"]
        elif id_data == "LowResFull":
            if in_time_interval == "3hr_subset":
                size_time = self.length_time_opt["LowResFull-3hr-Sub"]
            elif in_time_interval == "6hr_subset":
                size_time = self.length_time_opt["LowResFull-6hr-Sub"]
            else:
                size_time = self.length_time_opt["LowRes-6hr"]
        else: #HighRes
            if in_time_interval == "1hr":
                size_time = self.length_time_opt["HighRes-1hr"]
            elif in_time_interval == "3hr":
                size_time = self.length_time_opt["HighRes-3hr"]
            elif in_time_interval == "3hr_subset":
                size_time = self.length_time_opt["HighRes-3hr-Sub"]
            elif in_time_interval == "6hr_subset":
                size_time = self.length_time_opt["HighRes-6hr-Sub"]
            else:
                size_time = self.length_time_opt["HighRes-6hr"]

        size_streamID = len(set(dict_list[self.header_wt[0]]))

        # Create output inflow netcdf data
        # data_out_nc = NET.Dataset(out_nc, "w") # by default format = "NETCDF4"
        data_out_nc = NET.Dataset(out_nc, "w", format = "NETCDF3_CLASSIC")
        dim_Time = data_out_nc.createDimension('Time', size_time)
        dim_RiverID = data_out_nc.createDimension('rivid', size_streamID)
        var_m3_riv = data_out_nc.createVariable('m3_riv', 'f4', 
                                                ('Time', 'rivid'),
                                                fill_value=0)
                                                
        data_temp = NUM.empty(shape = [size_time, size_streamID])

        lon_ind_all = [long(i) for i in dict_list[self.header_wt[2]]]
        lat_ind_all = [long(j) for j in dict_list[self.header_wt[3]]]

        # Obtain a subset of  runoff data based on the indices in the weight table
        min_lon_ind_all = min(lon_ind_all)
        max_lon_ind_all = max(lon_ind_all)
        min_lat_ind_all = min(lat_ind_all)
        max_lat_ind_all = max(lat_ind_all)


        data_subset_all = data_in_nc.variables[self.vars_oi[vars_oi_index][3]][:, min_lat_ind_all:max_lat_ind_all+1, min_lon_ind_all:max_lon_ind_all+1]
        len_time_subset_all = data_subset_all.shape[0]
        len_lat_subset_all = data_subset_all.shape[1]
        len_lon_subset_all = data_subset_all.shape[2]
        data_subset_all = data_subset_all.reshape(len_time_subset_all, (len_lat_subset_all * len_lon_subset_all))


        # compute new indices based on the data_subset_all
        index_new = []
        for r in range(0,count-1):
            ind_lat_orig = lat_ind_all[r]
            ind_lon_orig = lon_ind_all[r]
            index_new.append((ind_lat_orig - min_lat_ind_all)*len_lon_subset_all + (ind_lon_orig - min_lon_ind_all))

        # obtain a new subset of data
        data_subset_new = data_subset_all[:,index_new]*conversion_factor

        # start compute inflow
        pointer = 0
        for s in range(0, size_streamID):
            npoints = int(dict_list[self.header_wt[4]][pointer])
            # Check if all npoints points correspond to the same streamID
            if len(set(dict_list[self.header_wt[0]][pointer : (pointer + npoints)])) != 1:
                print("ROW INDEX {0}".format(pointer))
                print("RIVID {0}".format(dict_list[self.header_wt[0]][pointer]))
                raise Exception(self.errorMessages[2])

            area_sqm_npoints = [float(k) for k in dict_list[self.header_wt[1]][pointer : (pointer + npoints)]]
            area_sqm_npoints = NUM.array(area_sqm_npoints)
            area_sqm_npoints = area_sqm_npoints.reshape(1, npoints)
            data_goal = data_subset_new[:, pointer:(pointer + npoints)]
            
            
            #remove noise from data
            data_goal[data_goal<=0.00001] = 0

            ''''IMPORTANT NOTE: runoff variable in ECMWF dataset is cumulative instead of incremental through time'''
            # For data with Low Resolution, there's only one time interval 6 hrs
            if id_data == "LowRes":
                #ro_stream = data_goal * area_sqm_npoints
                ro_stream = NUM.subtract(data_goal[1:,],data_goal[:-1,]) * area_sqm_npoints

            #For data with the full version of Low Resolution, from Hour 0 to 144 (the first 49 time points) are of 3 hr time interval,
            # then from Hour 144 to 360 (36 time points) are of 6 hour time interval
            elif id_data == "LowResFull":
                if in_time_interval == "3hr_subset":
                    #use only the 3hr time interval
                    ro_stream = NUM.subtract(data_goal[1:49,], data_goal[:48,]) * area_sqm_npoints
                elif in_time_interval == "6hr_subset":
                    #use only the 6hr time interval
                    ro_stream = NUM.subtract(data_goal[49:,], data_goal[48:-1,]) * area_sqm_npoints
                else: #"LowRes-6hr"
                    #convert all to 6hr
                    # calculate time series of 6 hr data from 3 hr data
                    ro_6hr_a = NUM.subtract(data_goal[2:49:2,], data_goal[:48:2,])
                    # get the time series of 6 hr data
                    ro_6hr_b = NUM.subtract(data_goal[49:,], data_goal[48:-1,])
                    # concatenate all time series
                    ro_stream = NUM.concatenate([ro_6hr_a, ro_6hr_b]) * area_sqm_npoints
            #For data with High Resolution, from Hour 0 to 90 (the first 91 time points) are of 1 hr time interval,
            # then from Hour 90 to 144 (18 time points) are of 3 hour time interval, and from Hour 144 to 240 (16 time points)
            # are of 6 hour time interval
            else:
                if in_time_interval == "1hr":
                    ro_stream = NUM.subtract(data_goal[1:91,],data_goal[:90,]) * area_sqm_npoints
                elif in_time_interval == "3hr":
                    # calculate time series of 3 hr data from 1 hr data
                    ro_3hr_a = NUM.subtract(data_goal[3:91:3,],data_goal[:88:3,])
                    # get the time series of 3 hr data
                    ro_3hr_b = NUM.subtract(data_goal[91:109,], data_goal[90:108,])
                    # concatenate all time series
                    ro_stream = NUM.concatenate([ro_3hr_a, ro_3hr_b]) * area_sqm_npoints
                elif in_time_interval == "3hr_subset":
                    #use only the 3hr time interval
                    ro_stream = NUM.subtract(data_goal[91:109,], data_goal[90:108,]) * area_sqm_npoints
                elif in_time_interval == "6hr_subset":
                    #use only the 6hr time interval
                    ro_stream = NUM.subtract(data_goal[109:,], data_goal[108:-1,]) * area_sqm_npoints
                else: # in_time_interval == "6hr"
                    #arcpy.AddMessage("6hr")
                    # calculate time series of 6 hr data from 1 hr data
                    ro_6hr_a = NUM.subtract(data_goal[6:91:6,], data_goal[:85:6,])
                    # calculate time series of 6 hr data from 3 hr data
                    ro_6hr_b = NUM.subtract(data_goal[92:109:2,], data_goal[90:107:2,])
                    # get the time series of 6 hr data
                    ro_6hr_c = NUM.subtract(data_goal[109:,], data_goal[108:-1,])
                    # concatenate all time series
                    ro_stream = NUM.concatenate([ro_6hr_a, ro_6hr_b, ro_6hr_c]) * area_sqm_npoints
                    
            #remove negative values
            ro_stream[ro_stream<0] = 0
            data_temp[:,s] = ro_stream.sum(axis = 1)

            pointer += npoints


        '''Write inflow data'''
        print("Writing inflow data...")
        var_m3_riv[:] = data_temp
        # close the input and output netcdf datasets
        data_in_nc.close()
        data_out_nc.close()


        return
