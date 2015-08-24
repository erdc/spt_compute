#!/bin/sh
/usr/lib/tethys/bin/python $HOME/work/scripts/spt_ecmwf_autorapid_process/rapid_process.py 1> $HOME/work/logs/rapid_$(date +%y%m%d%H%M%S).log 2>&1
