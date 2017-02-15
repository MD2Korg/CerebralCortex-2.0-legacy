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

from cerebralcortex.data_processor.feature.rip import rip_feature_computation
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

        insp_datastream, expr_datastream, resp_datastream, ieRatio_datastream, stretch_datastream, uStretch_datastream, lStretch_datastream, \
        bd_insp_datastream, bd_expr_datastream, bd_resp_datastream, bd_stretch_datastream, fd_insp_datastream, fd_expr_datastream, \
        fd_resp_datastream, fd_stretch_datastream, d5_expr_datastream, d5_stretch_datastream = rip_feature_computation(
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


if __name__ == '__main__':
    unittest.main()
