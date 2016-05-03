#!/bin/sh
/usr/bin/python /home/cecsr/scripts/spt_ecmwf_autorapid_process/run.py 1> /home/cecsr/logs/rapid_$(date +%y%m%d%H%M%S).log 2>&1
