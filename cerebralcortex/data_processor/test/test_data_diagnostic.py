# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import unittest
import time
import pytz
import os
from datetime import datetime
from collections import OrderedDict
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.configuration import Configuration
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.data_processor.data_diagnostic.battery_data_marker import battery, mark_windows
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
import random


class TestDataDiagnostic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        configuration_file = os.path.join(os.path.dirname(__file__), '../../../cerebralcortex.yml')
        cls.CC = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", time_zone="US/Central")
        cls.config = Configuration(filepath="../data_diagnostic/data_diagnostic_config.yml").config

        cls.sample_battery_data = []
        for row in range(1,481):
            if row<61:
                battery = 87.0
            elif row>60 and row<120:
                battery = 0.0
            elif row>120 and row<240:
                battery = 87.0
            elif row>240 and row<300:
                battery = 7.0
            elif row>300 and row<360:
                battery = 0.0
            elif row>360:
                battery = 60.0

            tz = pytz.timezone("US/Central")
            start_time = tz.localize(datetime.fromtimestamp(int(round((time.time()+row) * 1000))/1e3))

            dp = DataPoint(start_time=start_time, sample=battery)
            cls.sample_battery_data.append(dp)
        cls.window_size = 60

    def test_phone_battery(self):
        """
        settings: powered-off = 0 and battery-low=10
        """
        merged_windows_samples_expect = ['charged', 'no-data', 'charged', 'low', 'no-data', 'charged']
        labelled_windows_samples_expect = [0,10]
        cc = [self.sample_battery_data,self.sample_battery_data]
        dd = self.CC.sc.parallelize(cc)

        windowed_data = window(self.sample_battery_data, self.window_size, True)
        results = OrderedDict()
        for key, data in windowed_data.items():
            dp = []
            for k in data:
                dp.append(float(k.sample))

            results[key] = battery(dp, self.config["sensor_types"]["phone_battery"], self.config)

        merged_windows = merge_consective_windows(results)
        merged_windows_samples = [mw.sample for mw in merged_windows]

        labelled_windows = mark_windows(merged_windows, self.config)
        labelled_windows_samples = [mw.sample for mw in labelled_windows]

        self.assertListEqual(merged_windows_samples, merged_windows_samples_expect)
        self.assertListEqual(labelled_windows_samples, labelled_windows_samples_expect)


if __name__ == '__main__':
    unittest.main()