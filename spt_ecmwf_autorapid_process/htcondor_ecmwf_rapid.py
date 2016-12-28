#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  htcondor_ecmwf_rapid.py
#  spt_ecmwf_autorapid_process
#
#  Created by Alan D. Snow
#  License: BSD-3 Clause

import os
import sys

from spt_ecmwf_autorapid_process.imports.ecmwf_rapid_multiprocess_worker \
     import ecmwf_rapid_multiprocess_worker


def htcondor_process_ECMWF_RAPID(ecmwf_forecast, forecast_date_timestep, 
                                 watershed, subbasin, rapid_executable_location, 
                                 init_flow):
    """
    HTCondor process to prepare all ECMWF forecast input and run RAPID
    """

    node_path = os.path.dirname(os.path.realpath(__file__))

    old_rapid_input_directory = os.path.join(node_path, "{0}-{1}".format(watershed, subbasin))
    rapid_input_directory = os.path.join(node_path, "rapid_input")
    # rename rapid input directory
    os.rename(old_rapid_input_directory, rapid_input_directory)

    forecast_basename = os.path.basename(ecmwf_forecast)
    ecmwf_rapid_multiprocess_worker(node_path, rapid_input_directory,
                                    forecast_basename, forecast_date_timestep, 
                                    watershed, subbasin, rapid_executable_location, 
                                    init_flow)


if __name__ == "__main__":   
    htcondor_process_ECMWF_RAPID(sys.argv[1],sys.argv[2], sys.argv[3], 
                                 sys.argv[4], sys.argv[5], sys.argv[6])
