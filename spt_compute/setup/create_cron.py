# -*- coding: utf-8 -*-
#
#  create_cron.py
#  spt_compute
#
#  Created by Alan D. Snow.
#  Copyright © 2015-2016 Alan D Snow. All rights reserved.
#  License: BSD-3 Clause

from crontab import CronTab


def create_cron(execute_command):
    """
    This creates a cron job for the ECMWF autorapid process

    Ex.

        ::
        from spt_compute.setup import create_cron
        
        create_cron(execute_command='/usr/bin/env python /path/to/run_ecmwf_rapid.py')

    """
    cron_manager = CronTab(user=True)
    cron_comment = "ECMWF RAPID PROCESS"
    cron_manager.remove_all(comment=cron_comment)
    cron_job_morning = cron_manager.new(command=execute_command, 
                                        comment=cron_comment)
    cron_job_morning.every().hour()
    # writes content to crontab
    cron_manager.write()
