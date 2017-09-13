# Copyright (c) 2016, MD2K Center of Excellence
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

import datetime
import gzip
import os
import unittest

import pytz

from cerebralcortex.data_processor.feature.rip import rip_cycle_feature_computation
from cerebralcortex.data_processor.feature.rip import window_rip
from cerebralcortex.data_processor.signalprocessing.rip import compute_peak_valley
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


class TestRIPFeatures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestRIPFeatures, cls).setUpClass()
        tz = pytz.timezone('US/Eastern')
        rip = []
        rip_sampling_frequency = 64.0 / 3
        with gzip.open(os.path.join(os.path.dirname(__file__), 'res/rip.csv.gz'), 'rt') as f:
            for l in f:
                values = list(map(int, l.split(',')))
                rip.append(
                    DataPoint.from_tuple(datetime.datetime.fromtimestamp(values[0] / 1000000.0, tz=tz), values[1]))

        rip_ds = DataStream(None, None)
        rip_ds.data = rip[:5000]

        cls.peak, cls.valley = compute_peak_valley(rip_ds, fs=rip_sampling_frequency)

    def test_rip_feature_computation(self):
        print("peak size = ", len(self.peak.data))
        print("valley size = ", len(self.valley.data))

        peaks_ds, valleys_ds, insp_datastream, expr_datastream, resp_datastream, ieRatio_datastream, stretch_datastream, uStretch_datastream, lStretch_datastream, \
        bd_insp_datastream, bd_expr_datastream, bd_resp_datastream, bd_stretch_datastream, fd_insp_datastream, fd_expr_datastream, \
        fd_resp_datastream, fd_stretch_datastream, d5_expr_datastream, d5_stretch_datastream = rip_cycle_feature_computation(
            self.peak, self.valley)

        # test all are DataStream
        self.assertIsInstance(insp_datastream, DataStream)
        self.assertIsInstance(expr_datastream, DataStream)
        self.assertIsInstance(resp_datastream, DataStream)
        self.assertIsInstance(ieRatio_datastream, DataStream)
        self.assertIsInstance(stretch_datastream, DataStream)
        self.assertIsInstance(uStretch_datastream, DataStream)
        self.assertIsInstance(lStretch_datastream, DataStream)
        self.assertIsInstance(bd_insp_datastream, DataStream)
        self.assertIsInstance(bd_expr_datastream, DataStream)
        self.assertIsInstance(bd_resp_datastream, DataStream)
        self.assertIsInstance(bd_stretch_datastream, DataStream)
        self.assertIsInstance(fd_insp_datastream, DataStream)
        self.assertIsInstance(fd_expr_datastream, DataStream)
        self.assertIsInstance(fd_resp_datastream, DataStream)
        self.assertIsInstance(fd_stretch_datastream, DataStream)
        self.assertIsInstance(d5_expr_datastream, DataStream)
        self.assertIsInstance(d5_stretch_datastream, DataStream)

        datastream_breath_rates, datastream_minuteVentilation, \
        inspiration_duration_mean, inspiration_duration_median, inspiration_duration_quartile_deviation, inspiration_duration_80percentile, \
        expiration_duration_mean, expiration_duration_median, expiration_duration_quartile_deviation, expiration_duration_80percentile, \
        respiration_duration_mean, respiration_duration_median, respiration_duration_quartile_deviation, respiration_duration_80percentile, \
        inspiration_expiration_ratio_mean, inspiration_expiration_ratio_median, inspiration_expiration_ratio_quartile_deviation, inspiration_expiration_ratio_80percentile, \
        stretch_mean, stretch_median, stretch_quartile_deviation, stretch_80percentile = window_rip(peaks_ds, valleys_ds, insp_datastream, expr_datastream, resp_datastream, ieRatio_datastream, stretch_datastream, window_size=60, window_offset=60)

        self.assertIsInstance(datastream_breath_rates, DataStream)
        self.assertIsInstance(datastream_minuteVentilation, DataStream)
        self.assertIsInstance(stretch_quartile_deviation, DataStream)
        self.assertEqual(len(datastream_breath_rates.data), len(datastream_minuteVentilation.data))
        self.assertEqual(len(datastream_breath_rates.data), len(inspiration_duration_mean.data))
        self.assertEqual(len(stretch_mean.data), len(respiration_duration_quartile_deviation.data))


if __name__ == '__main__':
    unittest.main()
