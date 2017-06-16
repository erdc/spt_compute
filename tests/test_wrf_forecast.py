from datetime import datetime, timedelta
from glob import glob
import os

import pytest
from spt_compute import run_lsm_forecast_process

from .conftest import RAPID_EXE_PATH, SetupForecast


@pytest.fixture(scope="module")
def wrf_setup(request, tclean):
    return SetupForecast(tclean, "m-s", "wrf")

def test_wrf_forecast(wrf_setup):
    """
    Test basic WRF forecast process.
    """
    start_datetime = datetime.utcnow()
    run_lsm_forecast_process(rapid_executable_location=RAPID_EXE_PATH,
                             rapid_io_files_location=wrf_setup.rapid_io_folder,
                             lsm_forecast_location=wrf_setup.lsm_folder,
                             main_log_directory=wrf_setup.log_folder,
                             timedelta_between_forecasts=timedelta(seconds=0),
                             historical_data_location="")
    output_folder = os.path.join(wrf_setup.rapid_io_folder, 'output', 'm-s', '20080601t01')
    # check log file exists
    log_files = glob(os.path.join(wrf_setup.log_folder,
                                  "spt_compute_lsm_{0:%y%m%d%H%M}*.log".format(start_datetime)))
    assert len(log_files) == 1
    # check Qout file
    qout_file = os.path.join(output_folder, 'Qout_wrf_wrf_1hr_20080601to20080601.nc')
    assert os.path.exists(qout_file)
    # make sure no m3 file exists
    m3_files = glob(os.path.join(output_folder, "m3_riv*.nc"))
    assert len(m3_files) == 0
    # check Qinit file
    assert os.path.exists(os.path.join(wrf_setup.watershed_input_folder, 'Qinit_20080601t01.csv'))

