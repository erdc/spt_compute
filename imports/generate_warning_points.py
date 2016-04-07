# -*- coding: utf-8 -*-
##
##  generate_warning_points.py
##  spt_ecmwf_autorapid_process
##
##  Created by Alan D. Snow and Scott D. Christensen.
##  Copyright © 2015-2016 Alan D Snow and Scott D. Christensen. All rights reserved.
##  License: BSD-3 Clause

from datetime import datetime
import netCDF4 as nc
import numpy as np
import os
from json import dumps
from RAPIDpy.dataset import RAPIDDataset

def calc_daily_peak(daily_time_index_array, idx, qout_arr, size_time):
    """
    retrieves daily qout
    """
    len_daily_time_array = len(daily_time_index_array)
    time_index_start = daily_time_index_array[idx]
    if idx+1 < len_daily_time_array:
        next_time_index = daily_time_index_array[idx+1]
        return np.max(qout_arr[time_index_start:next_time_index])
    elif idx+1 == len_daily_time_array:
        if time_index_start < size_time - 1:
            return np.max(qout_arr[time_index_start:-1])	
        else:
            return qout_arr[time_index_start]
    return 0
    
def generate_warning_points(ecmwf_prediction_folder, return_period_file, out_directory, threshold=1):
    """
    Create warning points from return periods and ECMWD prediction data

    """

    #Get list of prediciton files
    prediction_files = sorted([os.path.join(ecmwf_prediction_folder,f) for f in os.listdir(ecmwf_prediction_folder) \
                              if not os.path.isdir(os.path.join(ecmwf_prediction_folder, f)) and f.lower().endswith('.nc')])

    #get the comids in ECMWF files
    with RAPIDDataset(prediction_files[0]) as qout_nc:
        prediction_comids = qout_nc.get_river_id_array()
        comid_list_length = qout_nc.size_river_id
        size_time = qout_nc.size_time
        first_half_size = 40 #run 6-hr resolution for all
        if qout_nc.is_time_variable_valid():
            if size_time == 41 or size_time == 61:
                #run at full or 6-hr resolution for high res and 6-hr for low res
                first_half_size = 41
            elif size_time == 85 or size_time == 125:
                #run at full resolution for all
                first_half_size = 65
        forecast_date_timestep = os.path.basename(ecmwf_prediction_folder)
        forecast_start_date = datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H")
        time_array = qout_nc.get_time_array(datetime_simulation_start=forecast_start_date,
                                            simulation_time_step_seconds=6*3600,
                                            return_datetime=True)
        current_day = forecast_start_date
        daily_time_index_array = [0]
        for idx, var_time in enumerate(time_array):
            if current_day.day != var_time.day:
                 daily_time_index_array.append(idx)
                 current_day = var_time


    print("Extracting Forecast Data ...")
    #get information from datasets
    reach_prediciton_array_first_half = np.zeros((comid_list_length,len(prediction_files),first_half_size))
    reach_prediciton_array_second_half = np.zeros((comid_list_length,len(prediction_files),20))
    for file_index, prediction_file in enumerate(prediction_files):
        data_values_2d_array = []
        try:
            ensemble_index = int(os.path.basename(prediction_file)[:-3].split("_")[-1])
            #Get hydrograph data from ECMWF Ensemble
            with RAPIDDataset(prediction_file) as qout_nc:
                data_values_2d_array = qout_nc.get_qout()
        except Exception, e:
            print(e)
            
        #add data to main arrays and order in order of interim comids
        if len(data_values_2d_array) > 0:
            for comid_index, comid in enumerate(prediction_comids):
                if(ensemble_index < 52):
                    reach_prediciton_array_first_half[comid_index][file_index] = data_values_2d_array[comid_index][:first_half_size]
                    reach_prediciton_array_second_half[comid_index][file_index] = data_values_2d_array[comid_index][first_half_size:]
                if(ensemble_index == 52):
                    if first_half_size == 65:
                        #convert to 3hr-6hr
                        streamflow_1hr = data_values_2d_array[comid_index][:90:3]
                        # get the time series of 3 hr/6 hr data
                        streamflow_3hr_6hr = data_values_2d_array[comid_index][90:]
                        # concatenate all time series
                        reach_prediciton_array_first_half[comid_index][file_index] = np.concatenate([streamflow_1hr, streamflow_3hr_6hr])
                    elif len(data_values_2d_array[comid_index]) == 125:
                        #convert to 6hr
                        streamflow_1hr = data_values_2d_array[comid_index][:90:6]
                        # calculate time series of 6 hr data from 3 hr data
                        streamflow_3hr = data_values_2d_array[comid_index][90:109:2]
                        # get the time series of 6 hr data
                        streamflow_6hr = data_values_2d_array[comid_index][109:]
                        # concatenate all time series
                        reach_prediciton_array_first_half[comid_index][file_index] = np.concatenate([streamflow_1hr, streamflow_3hr, streamflow_6hr])
                    else:
                        reach_prediciton_array_first_half[comid_index][file_index] = data_values_2d_array[comid_index][:]

    print("Extracting Return Period Data ...")
    return_period_nc = nc.Dataset(return_period_file, mode="r")
    riverid_var_name = 'COMID'
    if 'rivid' in return_period_nc.variables:
        riverid_var_name = 'rivid'
    return_period_comids = return_period_nc.variables[riverid_var_name][:]
    return_period_20_data = return_period_nc.variables['return_period_20'][:]
    return_period_10_data = return_period_nc.variables['return_period_10'][:]
    return_period_2_data = return_period_nc.variables['return_period_2'][:]
    return_period_lat_data = return_period_nc.variables['lat'][:]
    return_period_lon_data = return_period_nc.variables['lon'][:]
    return_period_nc.close()

    print("Analyzing Forecast Data with Return Periods ...")
    return_20_points = []
    return_10_points = []
    return_2_points = []
    for prediction_comid_index, prediction_comid in enumerate(prediction_comids):
        #get interim comid index
        return_period_comid_index = np.where(return_period_comids==prediction_comid)[0][0]
        #perform analysis on datasets
        all_data_first = reach_prediciton_array_first_half[prediction_comid_index]
        all_data_second = reach_prediciton_array_second_half[prediction_comid_index]

        return_period_20 = return_period_20_data[return_period_comid_index]
        return_period_10 = return_period_10_data[return_period_comid_index]
        return_period_2 = return_period_2_data[return_period_comid_index]
        #get mean
        mean_data_first = np.mean(all_data_first, axis=0)
        mean_data_second = np.mean(all_data_second, axis=0)
        mean_series = np.concatenate([mean_data_first,mean_data_second])
        #get max
        max_data_first = np.amax(all_data_first, axis=0)
        max_data_second = np.amax(all_data_second, axis=0)
        max_series = np.concatenate([max_data_first,max_data_second])
        #get std dev
        std_dev_first = np.std(all_data_first, axis=0)
        std_dev_second = np.std(all_data_second, axis=0)
        std_dev = np.concatenate([std_dev_first,std_dev_second])
        #mean plus std
        mean_plus_std_series = mean_series + std_dev
        for idx, daily_time_index in enumerate(daily_time_index_array):
            daily_mean_peak = calc_daily_peak(daily_time_index_array, idx, mean_series, size_time)
            if daily_mean_peak > threshold:
                if daily_mean_peak > return_period_20:
                    return_20_points.append({ "lat" : return_period_lat_data[return_period_comid_index],
                                              "lon" : return_period_lon_data[return_period_comid_index],
                                              "size": 1,
                                              "mean_peak": float("{0:.2f}".format(daily_mean_peak)),
                                              "peak_date": time_array[daily_time_index].strftime("%Y-%m-%d"),
                                              })
                elif daily_mean_peak > return_period_10:
                    return_10_points.append({ "lat" : return_period_lat_data[return_period_comid_index],
                                              "lon" : return_period_lon_data[return_period_comid_index],
                                              "size": 1,
                                              "mean_peak": float("{0:.2f}".format(daily_mean_peak)),
                                              "peak_date": time_array[daily_time_index].strftime("%Y-%m-%d"),
                                              })
                elif daily_mean_peak > return_period_2:
                    return_2_points.append({ "lat" : return_period_lat_data[return_period_comid_index],
                                              "lon" : return_period_lon_data[return_period_comid_index],
                                              "size": 1,
                                              "mean_peak": float("{0:.2f}".format(daily_mean_peak)),
                                              "peak_date": time_array[daily_time_index].strftime("%Y-%m-%d"),
                                              })
    
            daily_mean_plus_std_peak = min(calc_daily_peak(daily_time_index_array, idx, mean_plus_std_series, size_time),
                                           calc_daily_peak(daily_time_index_array, idx, max_series, size_time))
            if daily_mean_plus_std_peak > threshold:
                if daily_mean_plus_std_peak > return_period_20:
                    return_20_points.append({ "lat" : return_period_lat_data[return_period_comid_index],
                                              "lon" : return_period_lon_data[return_period_comid_index],
                                              "size": 0,
                                              "mean_plus_std_peak": float("{0:.2f}".format(daily_mean_plus_std_peak)),
                                              "peak_date": time_array[daily_time_index].strftime("%Y-%m-%d"),
                                              })
                elif daily_mean_plus_std_peak > return_period_10:
                    return_10_points.append({ "lat" : return_period_lat_data[return_period_comid_index],
                                              "lon" : return_period_lon_data[return_period_comid_index],
                                              "size": 0,
                                              "mean_plus_std_peak": float("{0:.2f}".format(daily_mean_plus_std_peak)),
                                              "peak_date": time_array[daily_time_index].strftime("%Y-%m-%d"),
                                              })
                elif daily_mean_plus_std_peak > return_period_2:
                    return_2_points.append({ "lat" : return_period_lat_data[return_period_comid_index],
                                              "lon" : return_period_lon_data[return_period_comid_index],
                                              "size": 0,
                                              "mean_plus_std_peak": float("{0:.2f}".format(daily_mean_plus_std_peak)),
                                              "peak_date": time_array[daily_time_index].strftime("%Y-%m-%d"),
                                              })

    print("Writing Output ...")
    with open(os.path.join(out_directory, "return_20_points.txt"), 'wb') as outfile:
        outfile.write(dumps(return_20_points))
    with open(os.path.join(out_directory, "return_10_points.txt"), 'wb') as outfile:
        outfile.write(dumps(return_10_points))
    with open(os.path.join(out_directory, "return_2_points.txt"), 'wb') as outfile:
        outfile.write(dumps(return_2_points))