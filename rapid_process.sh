#!/bin/sh
/usr/lib/tethys/bin/python /home/alan/work/scripts/spt_ecmwf_autorapid_process/rapid_process.py 1> /home/alan/work/logs/rapid_$(date +%y%m%d%H%M%S).log 2>&1
