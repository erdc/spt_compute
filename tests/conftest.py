# -*- coding: utf-8 -*-
#
#  conftest.py
#  spt_compute
#
#  Author : Alan D Snow, 2017.
#  License: BSD 3-Clause

import os
from shutil import copytree, rmtree
import pytest

from spt_compute.imports.extractnested import ExtractNested

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
RAPID_EXE_PATH = os.path.join(SCRIPT_DIR, "..", "..", "rapid", "src", "rapid")


class TestDirectories(object):
    input = os.path.join(SCRIPT_DIR, 'input')
    compare = os.path.join(SCRIPT_DIR, 'compare')
    output = os.path.join(SCRIPT_DIR, 'output')

    def clean(self):
        """
        Clean out test directory
        """
        original_dir = os.getcwd()
        os.chdir(self.output)

        # Clear out directory
        file_list = os.listdir(self.output)

        for afile in file_list:
            if not afile.endswith('.gitignore'):
                path = os.path.join(self.output, afile)
                if os.path.isdir(path):
                    rmtree(path)
                else:
                    os.remove(path)
        os.chdir(original_dir)


class SetupForecast(object):
    def __init__(self, tclean, watershed_folder, forecast_folder):
        tclean.clean()
        # make log folder
        self.log_folder = os.path.join(tclean.output, "logs")
        os.makedirs(self.log_folder)
        # copy RAPID model files
        self.rapid_io_folder = os.path.join(tclean.output, "rapid-io")
        rapid_input_folder = os.path.join(self.rapid_io_folder, "input")
        os.makedirs(rapid_input_folder)
        self.watershed_input_folder = os.path.join(rapid_input_folder, watershed_folder)
        copytree(os.path.join(tclean.input, "rapid_input", watershed_folder),
                 self.watershed_input_folder)
        # copy historical simulation_files
        self.historical_input_folder = os.path.join(tclean.output, "historical_input")
        os.makedirs(self.historical_input_folder)
        copytree(os.path.join(tclean.input, "historical_input", watershed_folder),
                 os.path.join(self.historical_input_folder, watershed_folder))
        # copy forecast grid files
        self.lsm_folder = os.path.join(tclean.output, forecast_folder)
        copytree(os.path.join(tclean.input, "forecast_grids", forecast_folder),
                 self.lsm_folder)
        # add path to comparison files
        self.watershed_compare_folder = os.path.join(tclean.compare,
                                                     'rapid_output',
                                                     watershed_folder)


class SetupECMWFForecast(SetupForecast):
    def __init__(self, tclean, watershed_folder, forecast_folder):
        super(SetupECMWFForecast, self).__init__(tclean, watershed_folder, forecast_folder)
        # make subprocess log folder
        self.subprocess_log_folder = os.path.join(tclean.output, "subprocess_logs")
        os.makedirs(self.subprocess_log_folder)
        # make multiprocess execute folder
        self.multiprocess_execute_folder = os.path.join(tclean.output, "mp_execute")
        os.makedirs(self.multiprocess_execute_folder)
        # extract the forecasts
        forecast_targz = os.path.join(self.lsm_folder, "Runoff.20170708.00.C.america.exp1.Fgrid.netcdf.tar.gz")
        ExtractNested(forecast_targz, True)


@pytest.fixture(scope="module")
def tclean(request):
    _td = TestDirectories()
    _td.clean()

    yield _td

    _td.clean()
