# -*- coding: utf-8 -*-
"""generate_warning_points.py

    This file containse functions to
    generate GeoJSON warning point
    files based on historical return period data
    and the most recent forecast.

    
    Original version created by Alan D. Snow and
    Scott D. Christensen, 2015-2017.
    
    Updated by Chase Hamilton, 2022
    
    License: BSD-3 Clause
"""
# pylint: disable=superfluous-parens, too-many-locals, too-many-statements
import json
import os

import numpy as np
import pandas as pd
import xarray as xr


def generate_lsm_warning_points(*args, **kwargs):
    pass


def geojson_feature(prefix, lat, lon, peak, date, rivid, ):
    return {"type": "Feature",
            "geometry": {"type": "Point",
                         "coordinates": [
                             float("{0:.6f}".format(lon)),
                             float("{0:.6f}".format(lat))]},
             "properties": {prefix: float("{0:.6f}".format(peak)),
                           "peak_date": date,
                           "rivid": int(rivid),
                           "size": 1}}


def geojson_features_to_collection(geojson_features):
    """
    Adds the feature collection wrapper for geojson
    """
    return {
        'type': 'FeatureCollection',
        'crs': {
            'type': 'name',
            'properties': {
                'name': 'EPSG:4326'
            }
        },
        'features': geojson_features
    }


def warning_points_worker(args):
    generate_ecmwf_warning_points(*args)


def generate_ecmwf_warning_points(ecmwf_prediction_folder, return_period_file, out_directory, threshold):
    prediction_files = os.listdir(ecmwf_prediction_folder)
    prediction_files = [os.path.join(ecmwf_prediction_folder, f) for f in prediction_files]
    prediction_files = [f for f in prediction_files if os.path.isfile(f)]
    prediction_files = [f for f in prediction_files if f.lower().endswith(".nc")]
    
    ensemble_index_list = []
    qout_datasets = []

    for forecast_nc in prediction_files:
        ensemble_index_list.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
        qout_datasets.append(xr.open_dataset(forecast_nc).Qout)
        

    merged_dataset = xr.concat(qout_datasets, pd.Index(ensemble_index_list, name="ensemble"))

#    merged_dataset = merged_dataset.resample(time="1D", skipna=True).max() # Compatible with current (SEP 2022) Xarray
    merged_dataset = merged_dataset.resample("D", dim="time", how="max", skipna=True) # Compatible with Xarray version in legacy use

    mean_dataset = merged_dataset.mean(dim="ensemble")
    stdev_dataset = merged_dataset.std(dim="ensemble")
    max_dataset = merged_dataset.max(dim="ensemble")

    print("Extracting return period data ...")
    return_period_dataset = xr.open_dataset(return_period_file)
    return_period_rivids = return_period_dataset["rivid"].values
    return_period_10_data = return_period_dataset["return_period_10"].values
    return_period_2_data = return_period_dataset["return_period_2"].values
    return_period_20_data = return_period_dataset["return_period_20"].values
    return_period_lats = return_period_dataset["lat"].values
    return_period_lons = return_period_dataset["lon"].values
    del(return_period_dataset)

    print("Analyzing forecast data with return periods ...")
    return_2_features = []
    return_10_features = []
    return_20_features = []

    for index, rivid in enumerate(merged_dataset.rivid.values):
        return_rivid_index = np.where(return_period_rivids == rivid)[0][0]
        return_period_2 = return_period_2_data[return_rivid_index]
        return_period_10 = return_period_10_data[return_rivid_index]
        return_period_20 = return_period_20_data[return_rivid_index]
        lat_coord = return_period_lats[return_rivid_index]
        lon_coord = return_period_lons[return_rivid_index]

        if return_period_20 < threshold:
            return_period_20 = threshold * 10
            return_period_10 = threshold * 5
            return_period_2 = threshold

        mean_array = mean_dataset.isel(rivid=index)
        stdev_array = stdev_dataset.isel(rivid=index)
        stdev_upper_array = mean_array + stdev_array        
#         max_array = max_dataset.isel(rivid=index)
#         stdev_upper_array[stdev_upper_array > max_array] = max_array
        length = mean_array.values.shape[0]
        
        for i in range(length):
            peak_mean = mean_array.values[i]
            peak_stdupper = stdev_upper_array.values[i]
            peak_date = str(mean_array["time"].values[i])[:10]
            
            if peak_mean > return_period_20:
                return_20_features.append(geojson_feature("mean_peak",
                                                          lat_coord,
                                                          lon_coord,
                                                          peak_mean,
                                                          peak_date,
                                                          rivid))
            elif peak_mean > return_period_10:
                return_10_features.append(geojson_feature("mean_peak",
                                                          lat_coord,
                                                          lon_coord,
                                                          peak_mean,
                                                          peak_date,
                                                          rivid))
            elif peak_mean > return_period_2:
                return_2_features.append(geojson_feature("mean_peak",
                                                         lat_coord,
                                                         lon_coord,
                                                         peak_mean,
                                                         peak_date,
                                                         rivid))
           
            if peak_stdupper > return_period_20:
                return_20_features.append(geojson_feature("std_upper_peak",
                                                          lat_coord,
                                                          lon_coord,
                                                          peak_stdupper,
                                                          peak_date,
                                                          rivid))
            elif peak_stdupper > return_period_10:
                return_10_features.append(geojson_feature("std_upper_peak",
                                                          lat_coord,
                                                          lon_coord,
                                                          peak_stdupper,
                                                          peak_date,
                                                          rivid))
            elif peak_stdupper > return_period_2:
                return_2_features.append(geojson_feature("std_upper_peak",
                                                         lat_coord,
                                                         lon_coord,
                                                         peak_stdupper,
                                                         peak_date,
                                                         rivid))
            
    out_path = os.path.join(out_directory, "{}")

    with open(out_path.format("return_20_points.geojson"), "w") as out_file:
        json.dump(geojson_features_to_collection(return_20_features), out_file)
    with open(out_path.format("return_10_points.geojson"), "w") as out_file:
        json.dump(geojson_features_to_collection(return_10_features), out_file) 
    with open(out_path.format("return_2_points.geojson"), "w") as out_file:
        json.dump(geojson_features_to_collection(return_2_features), out_file)
