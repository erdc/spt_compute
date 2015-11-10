from crontab import CronTab
cron_manager = CronTab(user=True)
cron_comment = "ECMWF RAPID PROCESS"
cron_manager.remove_all(comment=cron_comment)
cron_command = '/home/alan/scripts/spt_ecmwf_autorapid_process/rapid_process.sh' 
#add new times   
cron_job_morning = cron_manager.new(command=cron_command, 
                                    comment=cron_comment)
cron_job_morning.minute.on(30)
cron_job_morning.hour.on(4)
cron_job_evening = cron_manager.new(command=cron_command, 
                                    comment=cron_comment)
cron_job_evening.minute.on(30)
cron_job_evening.hour.on(16)
#writes content to crontab
cron_manager.write()
