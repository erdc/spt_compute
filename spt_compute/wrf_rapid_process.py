from RAPIDpy.inflow import run_lsm_rapid_process

# ----------------------------------------------------------------------------------------
# MAIN PROCESS
# ----------------------------------------------------------------------------------------
def run_wrf_rapid_process(rapid_executable_location,
                          rapid_io_files_location,
                          wrf_forecast_location,
                          main_log_directory,
                          historical_data_location=""):
    """

    :param rapid_executable_location: path to RAPID executable
    :param rapid_io_files_location: path ro RAPID input/output directory
    :param wrf_forecast_location: path to WRF forecasts
    :param main_log_directory: path to store main logs
    :param historical_data_location: path to return period and seasonal data
    :return:
    """
    time_begin_all = datetime.datetime.utcnow()

    log_file_path = os.path.join(
        main_log_directory,
        "spt_compute_wrf_{0}.log".format(time_begin_all.strftime("%y%m%d%H%M%S"))
    )

    with CaptureStdOutToLog(log_file_path):
        # get list of correclty formatted rapid input directories in rapid directory
        rapid_input_directories = get_valid_watershed_list(os.path.join(rapid_io_files_location, "input"))

        # PHASE 1: SEASONAL INITIALIZATION ON FIRST RUN
        if historical_data_location and os.path.exists(historical_data_location):
            initial_forecast_date_timestep = get_date_timestep_from_forecast_folder(wrf_forecast_location)
            seasonal_init_job_list = []
            # iterate over models
            for rapid_input_directory in rapid_input_directories:
                seasonal_master_watershed_input_directory = os.path.join(rapid_io_files_location, "input",
                                                                         rapid_input_directory)
                historical_watershed_directory = os.path.join(historical_data_location, rapid_input_directory)
                if os.path.exists(historical_watershed_directory):
                    seasonal_streamflow_file = glob(
                        os.path.join(era_interim_watershed_directory, "seasonal_average*.nc"))
                    if seasonal_streamflow_file:
                        seasonal_init_job_list.append((seasonal_streamflow_file[0],
                                                       seasonal_master_watershed_input_directory,
                                                       initial_forecast_date_timestep,
                                                       "seasonal_average_file"))
            if seasonal_init_job_list:
                if len(seasonal_init_job_list) > 1:
                    seasonal_pool = mp_Pool()
                    seasonal_pool.imap(compute_seasonal_initial_rapid_flows_multicore_worker,
                                       seasonal_init_job_list,
                                       chunksize=1)
                    seasonal_pool.close()
                    seasonal_pool.join()
                else:
                    compute_seasonal_initial_rapid_flows_multicore_worker(seasonal_init_job_list[0])

        # PHASE 2: MAIN RUN
        for rapid_input_directory in rapid_input_directories:
            master_watershed_input_directory = os.path.join(rapid_io_files_location, "input",
                                                            rapid_input_directory)
            master_watershed_output_directory = os.path.join(rapid_io_files_location, 'output',
                                                             rapid_input_directory, forecast_date_timestep)

            # PHASE 2.1 RUN RAPID
            # TODO: Get name of output file
            run_lsm_rapid_process(rapid_executable_location=rapid_executable_location,
                                  lsm_data_location=wrf_forecast_location,
                                  rapid_input_location=master_watershed_input_directory,
                                  rapid_output_location=master_watershed_output_directory)

            # PHASE 2.2: GENERATE WARNINGS
            watershed, subbasin = get_watershed_subbasin_from_folder(rapid_input_directory)
            forecast_directory = os.path.join(rapid_io_files_location,
                                              'output',
                                              rapid_input_directory,
                                              forecast_date_timestep)

            historical_watershed_directory = os.path.join(historical_data_location, rapid_input_directory)
            if os.path.exists(historical_watershed_directory):
                return_period_files = glob(os.path.join(era_interim_watershed_directory, "return_period*.nc"))
                if return_period_files:
                    print("Generating warning points for {0}-{1} from {2}".format(watershed, subbasin,
                                                                                  forecast_date_timestep))
                    try:
                        generate_warning_points(forecast_directory, return_period_files[0],
                                                forecast_directory, threshold=10)

            # PHASE 2.3: GENERATE INITIALIZATION FOR NEXT RUN
            print("Initializing flows for {0}-{1} from {2}".format(watershed, subbasin,
                                                                   forecast_date_timestep))
            basin_files = find_current_rapid_output(forecast_directory, watershed, subbasin)
            try:
                compute_initial_rapid_flows(basin_files, input_directory, forecast_date_timestep)
            except Exception as ex:
                print(ex)
                pass

        # print info to user
        time_end = datetime.datetime.utcnow()
        print("Time Begin: {0}".format(time_begin_all))
        print("Time Finish: {0}".format(time_end))
        print("TOTAL TIME: {0}".format(time_end - time_begin_all))