__author__ = 'Alan Snow'

import netCDF4 as NET
import numpy as np
import os
from json import dumps

def generate_warning_points(ecmwf_prediction_folder, era_interim_file, out_directory):
    """
    Create warning points from era interim data and ECMWD prediction data

    """

    #Get list of prediciton files

    prediction_files = [os.path.join(ecmwf_prediction_folder,f) for f in os.listdir(ecmwf_prediction_folder) \
                        if not os.path.isdir(os.path.join(ecmwf_prediction_folder, f)) and f.endswith(".nc")]

    #get the comids in ECMWF files
    data_nc = NET.Dataset(prediction_files[0], mode="r")
    prediction_comids = data_nc.variables['COMID'][:]
    comid_list_length = len(prediction_comids)
    data_nc.close()
    #get the comids in ERA Interim file
    data_nc = NET.Dataset(era_interim_file, mode="r")
    era_interim_comids = data_nc.variables['COMID'][:]
    data_nc.close()

    print "Extracting Data ..."
    #get information from datasets
    reach_prediciton_array_first_half = np.zeros((comid_list_length,len(prediction_files),40))
    reach_prediciton_array_second_half = np.zeros((comid_list_length,len(prediction_files),20))
    for file_index, prediction_file in enumerate(prediction_files):
        data_values_2d_array = []
        try:
            ensemble_index = int(os.path.basename(prediction_file)[:-3].split("_")[-1])
            #Get hydrograph data from ECMWF Ensemble
            data_nc = NET.Dataset(prediction_file, mode="r")
            qout_dimensions = data_nc.variables['Qout'].dimensions
            if qout_dimensions[0].lower() == 'time' and qout_dimensions[1].lower() == 'comid':
                data_values_2d_array = data_nc.variables['Qout'][:].transpose()
            elif qout_dimensions[0].lower() == 'comid' and qout_dimensions[1].lower() == 'time':
                data_values_2d_array = data_nc.variables['Qout'][:]
            else:
                print "Invalid ECMWF forecast file", prediction_file
                data_nc.close()
                continue
            data_nc.close()

        except Exception, e:
            print e
            #pass
        #add data to main arrays and order in order of interim comids
        if len(data_values_2d_array) > 0:
            for comid_index, comid in enumerate(prediction_comids):
                reach_prediciton_array_first_half[comid_index][file_index] = data_values_2d_array[comid_index][:40]
                if(ensemble_index < 52):
                    reach_prediciton_array_second_half[comid_index][file_index] = data_values_2d_array[comid_index][40:]

    print "Extracting and Sorting ERA Interim Data ..."
    #get ERA Interim Data Analyzed
    era_data_nc = NET.Dataset(era_interim_file, mode="r")
    era_flow_data = era_data_nc.variables['Qout'][:]
    num_years = int(len(era_flow_data[0])/365)
    era_interim_data_2d_array = np.sort(era_flow_data, axis=1)[:,:num_years:-1]
    era_interim_lat_data = era_data_nc.variables['lat'][:]
    era_interim_lon_data = era_data_nc.variables['lon'][:]
    era_data_nc.close()

    print "Analyzing Data with Return Periods ..."
    return_25_points = []
    return_10_points = []
    return_2_points = []
    for prediction_comid_index, prediction_comid in enumerate(prediction_comids):
        #get interim comid index
        era_interim_comid_index = np.where(era_interim_comids==prediction_comid)[0][0]
        #perform analysis on datasets
        all_data_first = reach_prediciton_array_first_half[prediction_comid_index]
        all_data_second = reach_prediciton_array_second_half[prediction_comid_index]

        return_period_25 = era_interim_data_2d_array[era_interim_comid_index, num_years-25]
        return_period_10 = era_interim_data_2d_array[era_interim_comid_index, num_years-10]
        return_period_2 = era_interim_data_2d_array[era_interim_comid_index, num_years-2]
        #get mean
        mean_data_first = np.mean(all_data_first, axis=0)
        mean_data_second = np.mean(all_data_second, axis=0)
        mean_series = np.concatenate([mean_data_first,mean_data_second])
        mean_peak = np.amax(mean_series)
        if mean_peak > return_period_25:
            return_25_points.append({ "lat" : era_interim_lat_data[era_interim_comid_index],
                                      "lon" : era_interim_lon_data[era_interim_comid_index],
                                      "size": 1,
                                      })
        elif mean_peak > return_period_10:
            return_10_points.append({ "lat" : era_interim_lat_data[era_interim_comid_index],
                                      "lon" : era_interim_lon_data[era_interim_comid_index],
                                      "size": 1,
                                      })
        elif mean_peak > return_period_2:
            return_2_points.append({ "lat" : era_interim_lat_data[era_interim_comid_index],
                                      "lon" : era_interim_lon_data[era_interim_comid_index],
                                      "size": 1,
                                      })

        #get max
        max_data_first = np.amax(all_data_first, axis=0)
        max_data_second = np.amax(all_data_second, axis=0)
        max_series = np.concatenate([max_data_first,max_data_second])
        max_peak = np.amax(max_series)
        #get std dev
        std_dev_first = np.std(all_data_first, axis=0)
        std_dev_second = np.std(all_data_second, axis=0)
        std_dev = np.concatenate([std_dev_first,std_dev_second])
        #mean plus std
        mean_plus_std_series = mean_series + std_dev
        mean_plus_std_peak = min(np.amax(mean_plus_std_series), max_peak)
        if mean_plus_std_peak > return_period_25:
            return_25_points.append({ "lat" : era_interim_lat_data[era_interim_comid_index],
                                      "lon" : era_interim_lon_data[era_interim_comid_index],
                                      "size": 0,
                                      })
        elif mean_plus_std_peak > return_period_10:
            return_10_points.append({ "lat" : era_interim_lat_data[era_interim_comid_index],
                                      "lon" : era_interim_lon_data[era_interim_comid_index],
                                      "size": 0,
                                      })
        elif mean_plus_std_peak > return_period_2:
            return_2_points.append({ "lat" : era_interim_lat_data[era_interim_comid_index],
                                      "lon" : era_interim_lon_data[era_interim_comid_index],
                                      "size": 0,
                                      })
            
    print "Writing Output ..."
    with open(os.path.join(out_directory, "return_25_points.txt"), 'wb') as outfile:
        outfile.write(dumps(return_25_points))
    with open(os.path.join(out_directory, "return_10_points.txt"), 'wb') as outfile:
        outfile.write(dumps(return_10_points))
    with open(os.path.join(out_directory, "return_2_points.txt"), 'wb') as outfile:
        outfile.write(dumps(return_2_points))


if __name__ == "__main__":
    ecmwf_prediction_folder = '/home/alan/tethysdev/tethysapp-erfp_tool/rapid_files/ecmwf_prediction/korean_peninsula/korea/20150724.0'
    era_interim_file = '/home/alan/tethysdev/tethysapp-erfp_tool/rapid_files/era_interim_historical_data/korean_peninsula/korea/Qout_erai_runoff.nc'
    generate_warning_points(ecmwf_prediction_folder, era_interim_file, out_directory=ecmwf_prediction_folder)

