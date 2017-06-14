# -*- coding: utf-8 -*-
#
#  process_lock.py
#  spt_process
#
#  Created by Alan D. Snow.
#  Copyright © 2015-2016 Alan D Snow. All rights reserved.
#  License: BSD-3 Clause
import json


def update_lock_info_file(lock_info_file_path, currently_running, last_forecast_date):
    """
    This function updates the lock info file
    """
    with open(lock_info_file_path, "w") as fp_lock_info:
        lock_info_data = {
            'running': currently_running,
            'last_forecast_date': last_forecast_date,
        }
        json.dump(lock_info_data, fp_lock_info)


def reset_lock_info_file(lock_info_file_path):
    """
    This function removes lock in file if the file exists.
    The purpose is for reboot of computer
    """
    if os.path.exists(lock_info_file_path):
        # read in last forecast date
        with open(lock_info_file_path) as fp_lock_info:
            previous_lock_info = json.load(fp_lock_info)
            last_forecast_date_str = previous_lock_info['last_forecast_date']

        # update lock to false
        update_lock_info_file(lock_info_file_path, False, last_forecast_date_str)