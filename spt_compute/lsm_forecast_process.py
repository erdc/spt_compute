from datetime import datetime, timedelta
from glob import glob
from multiprocessing import Pool as mp_Pool
import os

from RAPIDpy.inflow import run_lsm_rapid_process
from RAPIDpy.inflow.lsm_rapid_process import determine_start_end_timestep

from .imports.generate_warning_points import generate_lsm_warning_points
from .imports.helper_functions import (CaptureStdOutToLog,
                                       clean_main_logs,
                                       get_valid_watershed_list,
                                       get_watershed_subbasin_from_folder, )

from .imports.streamflow_assimilation import (compute_initial_flows_lsm,
                                              compute_seasonal_average_initial_flows_multiprocess_worker)

# ----------------------------------------------------------------------------------------
# MAIN PROCESS
# ----------------------------------------------------------------------------------------
def run_lsm_forecast_process(rapid_executable_location,
                             rapid_io_files_location,
                             lsm_forecast_location,
                             main_log_directory,
                             timedelta_between_forecasts=timedelta(seconds=12 * 3600),
                             historical_data_location="",
                             warning_flow_threshold=None):
    """
    Parameters
    ----------
    rapid_executable_location: str
        Path to RAPID executable.
    rapid_io_files_location: str
        Path ro RAPID input/output directory.
    lsm_forecast_location: str
        Path to WRF forecast directory.
    main_log_directory: str
        Path to directory to store main logs.
    timedelta_between_forecasts: :obj:`datetime.timedelta`
        Time difference between forecasts.
    historical_data_location: str, optional
        Path to return period and seasonal data.
    warning_flow_threshold: float, optional
        Minimum value for return period in m3/s to generate warning.
        Default is None.
    """
    time_begin_all = datetime.utcnow()

    log_file_path = os.path.join(
        main_log_directory,
        "spt_compute_lsm_{0}.log".format(time_begin_all.strftime("%y%m%d%H%M%S"))
    )

    with CaptureStdOutToLog(log_file_path):
        clean_main_logs(main_log_directory, prepend="spt_compute_lsm_")
        # get list of correclty formatted rapid input directories in rapid directory
        rapid_input_directories = get_valid_watershed_list(os.path.join(rapid_io_files_location, "input"))

        current_forecast_start_datetime = \
            determine_start_end_timestep(sorted(glob(os.path.join(lsm_forecast_location, "*.nc"))))[0]

        forecast_date_string = current_forecast_start_datetime.strftime("%Y%m%dt%H")
        # look for past forecast qinit
        past_forecast_date_string = (current_forecast_start_datetime - timedelta_between_forecasts).strftime("%Y%m%dt%H")
        init_file_name = 'Qinit_{0}.csv'.format(past_forecast_date_string)

        # PHASE 1: SEASONAL INITIALIZATION ON FIRST RUN
        if historical_data_location and os.path.exists(historical_data_location):
            seasonal_init_job_list = []
            # iterate over models
            for rapid_input_directory in rapid_input_directories:
                seasonal_master_watershed_input_directory = os.path.join(rapid_io_files_location, "input",
                                                                         rapid_input_directory)
                init_file_path = os.path.join(seasonal_master_watershed_input_directory, init_file_name)
                historical_watershed_directory = os.path.join(historical_data_location, rapid_input_directory)
                if os.path.exists(historical_watershed_directory):
                    seasonal_streamflow_file = glob(
                        os.path.join(historical_watershed_directory, "seasonal_average*.nc"))
                    if seasonal_streamflow_file and not os.path.exists(init_file_path):
                        seasonal_init_job_list.append((
                            seasonal_streamflow_file[0],
                            seasonal_master_watershed_input_directory,
                            init_file_path,
                        ))

            if seasonal_init_job_list:
                if len(seasonal_init_job_list) > 1:
                    seasonal_pool = mp_Pool()
                    seasonal_pool.imap(compute_seasonal_average_initial_flows_multiprocess_worker,
                                       seasonal_init_job_list,
                                       chunksize=1)
                    seasonal_pool.close()
                    seasonal_pool.join()
                else:
                    compute_seasonal_average_initial_flows_multiprocess_worker(seasonal_init_job_list[0])

        # PHASE 2: MAIN RUN
        for rapid_input_directory in rapid_input_directories:
            master_watershed_input_directory = os.path.join(rapid_io_files_location, "input",
                                                            rapid_input_directory)
            master_watershed_output_directory = os.path.join(rapid_io_files_location, 'output',
                                                             rapid_input_directory, forecast_date_string)
            watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)

            # PHASE 2.1 RUN RAPID
            output_file_information = run_lsm_rapid_process(
                rapid_executable_location=rapid_executable_location,
                lsm_data_location=lsm_forecast_location,
                rapid_input_location=master_watershed_input_directory,
                rapid_output_location=master_watershed_output_directory,
                initial_flows_file=os.path.join(master_watershed_input_directory, init_file_name),
            )

            forecast_file = output_file_information[0][rapid_input_directory]['qout']
            m3_riv_file = output_file_information[0][rapid_input_directory]['m3_riv']

            try:
                os.remove(m3_riv_file)
            except OSError:
                pass

            # PHASE 2.2: GENERATE WARNINGS
            forecast_directory = os.path.join(rapid_io_files_location,
                                              'output',
                                              rapid_input_directory,
                                              forecast_date_string)

            historical_watershed_directory = os.path.join(historical_data_location, rapid_input_directory)
            if os.path.exists(historical_watershed_directory):
                return_period_files = glob(os.path.join(historical_watershed_directory, "return_period*.nc"))
                if return_period_files:
                    print("Generating warning points for {0}-{1} from {2}"
                          .format(watershed, subbasin, forecast_date_string))
                    try:
                        generate_lsm_warning_points(forecast_file,
                                                    return_period_files[0],
                                                    forecast_directory,
                                                    warning_flow_threshold)
                    except Exception as ex:
                        print(ex)
                        pass

            # PHASE 2.3: GENERATE INITIALIZATION FOR NEXT RUN
            print("Initializing flows for {0}-{1} from {2}"
                  .format(watershed, subbasin, forecast_date_string))
            try:
                compute_initial_flows_lsm(forecast_file,
                                          master_watershed_input_directory,
                                          current_forecast_start_datetime +
                                          timedelta_between_forecasts)
            except Exception as ex:
                print(ex)
                pass

        # print info to user
        time_end = datetime.utcnow()
        print("Time Begin: {0}".format(time_begin_all))
        print("Time Finish: {0}".format(time_end))
        print("TOTAL TIME: {0}".format(time_end - time_begin_all))