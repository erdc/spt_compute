# -*- coding: utf-8 -*-
##
##  create_cron.py
##  spt_ecmwf_autorapid_process
##
##  Created by Alan D. Snow.
##  Copyright © 2015-2016 Alan D Snow. All rights reserved.
##  License: BSD-3 Clause

from crontab import CronTab

def create_cron(execute_command, 
                job_1_start_hour,
                job_1_start_minute,
                job_2_start_hour,
                job_2_start_minute):
    """
    This creates a cron job for the ECMWF autorapid process

    Ex.

        ::
        from spt_ecmwf_autorapid_process.setup import create_cron
        
        create_cron(execute_command='/usr/bin/python /path/to/run_ecmwf_rapid.py', 
                    job_1_start_hour=5,
                    job_1_start_minute=45,
                    job_2_start_hour=17,
                    job_2_start_minute=45)

    """
    cron_manager = CronTab(user=True)
    cron_comment = "ECMWF RAPID PROCESS"
    cron_manager.remove_all(comment=cron_comment)
    #add new times   
    cron_job_morning = cron_manager.new(command=execute_command, 
                                        comment=cron_comment)
    cron_job_morning.minute.on(job_1_start_minute)
    cron_job_morning.hour.on(job_1_start_hour)
    cron_job_evening = cron_manager.new(command=execute_command, 
                                        comment=cron_comment)
    cron_job_evening.minute.on(job_2_start_minute)
    cron_job_evening.hour.on(job_2_start_hour)
    #writes content to crontab
    cron_manager.write()
