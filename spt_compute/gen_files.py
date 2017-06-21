import xarray as xr
import numpy as np
from datetime import datetime

path_to_qout = "/home/rdchlads/scripts/spt_compute/tests/compare/rapid_output/m-s/20080601t01/Qout_wrf_wrf_1hr_20080601to20080601.nc"
out_return_nc = "/home/rdchlads/scripts/spt_compute/tests/compare/return_periods.nc"
out_seasonal_nc = "/home/rdchlads/scripts/spt_compute/tests/compare/seasonal_averages.nc"
out_init_nc = "/home/rdchlads/scripts/spt_compute/tests/input/qinit.nc"
with xr.open_dataset(path_to_qout) as qds:
    qds_slice = qds.sel(time=str(datetime(2008, 6, 1, 12))).Qout
    print(qds_slice.time)
    qds_slice.time.encoding['units'] = "seconds since 1970-01-01 00:00:00+00:00"
    qds_slice.time.encoding['calendar'] = "gregorian"
    print(qds_slice.time)
    qds_slice.to_netcdf(out_init_nc)

    """
    qds_info = qds.Qout.to_pandas().describe()
    #print(qds_info)

    # SEASONAL
    day_of_year = range(1, 366)
    mean_array = np.zeros((qds.rivid.size, 365))
    std_array = np.zeros((qds.rivid.size, 365))
    min_array = np.zeros((qds.rivid.size, 365))
    max_array = np.zeros((qds.rivid.size, 365))
    for i in range(365):
        mult = 1
        if i>90 and i<120:
            mult = 5
        elif i>120 and i<150:
            mult = 7
        mean_array[:, i] = mult*qds_info.loc['mean'].values
        std_array[:, i] = mult*qds_info.loc['std'].values
        min_array[:, i] = mult*qds_info.loc['min'].values
        max_array[:, i] = mult*qds_info.loc['max'].values

    ds = xr.Dataset({
                     'average_flow': (['rivid', 'day_of_year'], mean_array),
                     'std_dev_flow': (['rivid', 'day_of_year'], std_array),
                     'min_flow': (['rivid', 'day_of_year'], min_array),
                     'max_flow': (['rivid', 'day_of_year'], max_array)},
                     coords = {'lon': (['rivid'], qds.lon),
                               'lat': (['rivid'], qds.lat),
                               'rivid': qds.rivid,
                               'day_of_year': day_of_year})
    ds.average_flow.attrs['long_name'] = "seasonal average streamflow"
    ds.average_flow.attrs['units'] = "m3/s"

    ds.std_dev_flow.attrs['long_name'] = "seasonal std. dev. streamflow"
    ds.std_dev_flow.attrs['units'] = "m3/s"

    ds.min_flow.attrs['long_name'] = "seasonal min streamflow"
    ds.min_flow.attrs['units'] = "m3/s"

    ds.max_flow.attrs['long_name'] = "seasonal max streamflow"
    ds.max_flow.attrs['units'] = "m3/s"

    ds.rivid.attrs['long_name'] = "unique identifier for each river reach"

    ds.lat.attrs['long_name'] = "latitude"
    ds.lat.attrs['standard_name'] = "latitude"
    ds.lat.attrs['units'] = "degrees_north"
    ds.lat.attrs['axis'] = "Y"

    ds.lon.attrs['long_name'] = "longitude"
    ds.lon.attrs['standard_name'] = "longitude"
    ds.lon.attrs['units'] = "degrees_east"
    ds.lon.attrs['axis'] = "X"
    print(ds)
    ds.to_netcdf(out_seasonal_nc)

    '''
    netcdf seasonal_averages_erai_t511_24hr_19800101to19861231 {
    dimensions:
        rivid = 9 ;
        day_of_year = 365 ;
    variables:
        int rivid(rivid) ;
            rivid:long_name = "unique identifier for each river reach" ;
        double average_flow(rivid, day_of_year) ;
            average_flow:long_name = "seasonal average streamflow" ;
            average_flow:units = "m3/s" ;
        double std_dev_flow(rivid, day_of_year) ;
            std_dev_flow:long_name = "seasonal std. dev. streamflow" ;
            std_dev_flow:units = "m3/s" ;
        double max_flow(rivid, day_of_year) ;
            max_flow:long_name = "seasonal max streamflow" ;
            max_flow:units = "m3/s" ;
        double min_flow(rivid, day_of_year) ;
            min_flow:long_name = "seasonal min streamflow" ;
            min_flow:units = "m3/s" ;
        double lat(rivid) ;
            lat:_FillValue = -9999. ;
            lat:long_name = "latitude" ;
            lat:standard_name = "latitude" ;
            lat:units = "degrees_north" ;
            lat:axis = "Y" ;
        double lon(rivid) ;
            lon:_FillValue = -9999. ;
            lon:long_name = "longitude" ;
            lon:standard_name = "longitude" ;
            lon:units = "degrees_east" ;
            lon:axis = "X" ;
    }
    '''
    """
    """
    # RETURN PERIODS
    ds = xr.Dataset({
                     'return_period_2': (['rivid'], qds_info.loc['25%'].values),
                     'return_period_10': (['rivid'], qds_info.loc['50%'].values),
                     'return_period_20': (['rivid'], qds_info.loc['75%'].values),
                     'max_flow': (['rivid'], qds_info.loc['max'].values)},
                     coords = {'lon': (['rivid'], qds.lon),
                               'lat': (['rivid'], qds.lat),
                               'rivid': qds.rivid})
    ds.return_period_2.attrs['long_name'] = "2 year return period flow"
    ds.return_period_2.attrs['units'] = "m3/s"

    ds.return_period_10.attrs['long_name'] = "10 year return period flow"
    ds.return_period_10.attrs['units'] = "m3/s"

    ds.return_period_20.attrs['long_name'] = "20 year return period flow"
    ds.return_period_20.attrs['units'] = "m3/s"

    ds.max_flow.attrs['long_name'] = "maximum streamflow"
    ds.max_flow.attrs['units'] = "m3/s"

    ds.rivid.attrs['long_name'] = "unique identifier for each river reach"

    ds.lat.attrs['long_name'] = "latitude"
    ds.lat.attrs['standard_name'] = "latitude"
    ds.lat.attrs['units'] = "degrees_north"
    ds.lat.attrs['axis'] = "Y"

    ds.lon.attrs['long_name'] = "longitude"
    ds.lon.attrs['standard_name'] = "longitude"
    ds.lon.attrs['units'] = "degrees_east"
    ds.lon.attrs['axis'] = "X"

    ds.to_netcdf(out_return_nc)
    '''
    dimensions:
        rivid = 9 ;
    variables:
        int rivid(rivid) ;
            rivid:long_name = "unique identifier for each river reach" ;
        double max_flow(rivid) ;
            max_flow:long_name = "maximum streamflow" ;
            max_flow:units = "m3/s" ;
        double return_period_20(rivid) ;
            return_period_20:long_name = "20 year return period flow" ;
            return_period_20:units = "m3/s" ;
        double return_period_10(rivid) ;
            return_period_10:long_name = "10 year return period flow" ;
            return_period_10:units = "m3/s" ;
        double return_period_2(rivid) ;
            return_period_2:long_name = "2 year return period flow" ;
            return_period_2:units = "m3/s" ;
        double lat(rivid) ;
            lat:_FillValue = -9999. ;
            lat:long_name = "latitude" ;
            lat:standard_name = "latitude" ;
            lat:units = "degrees_north" ;
            lat:axis = "Y" ;
        double lon(rivid) ;
            lon:_FillValue = -9999. ;
            lon:long_name = "longitude" ;
            lon:standard_name = "longitude" ;
            lon:units = "degrees_east" ;
            lon:axis = "X" ;
    
    // global attributes:
            :return_period_method = "weibull" ;
    '''
    """
    print(ds)