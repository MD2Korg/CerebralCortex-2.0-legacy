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
from random import random

import numpy as np
import pytz

from cerebralcortex.data_processor.signalprocessing.alignment import interpolate_gaps, timestamp_correct
from cerebralcortex.data_processor.signalprocessing.vector import normalize, magnitude
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


class TestVector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestVector, cls).setUpClass()
        tz = pytz.timezone('US/Eastern')
        cls.ecg = []
        cls.sample_rate = 64.0
        with gzip.open(os.path.join(os.path.dirname(__file__), 'res/ecg.csv.gz'), 'rt') as f:
            for l in f:
                values = list(map(int, l.split(',')))
                cls.ecg.append(
                    DataPoint.from_tuple(datetime.datetime.fromtimestamp(values[0] / 1000000.0, tz=tz), values[1]))

    def setUp(self):
        self.size = 100
        self.ds = DataStream(None, None)
        data = [DataPoint.from_tuple(datetime.datetime.now(), None, [random() * 100, random() * 10, random()]) for i
                in range(0, self.size)]
        self.ds.datapoints = data

    @unittest.skip("Skipped test case: Correct value needs fixed")
    def test_normalize(self):
        self.assertIsInstance(self.ds, DataStream)
        self.assertEqual(len(self.ds.datapoints), self.size)

        n = normalize(self.ds)
        self.assertIsInstance(n, DataStream)
        for dp in n.datapoints:
            self.assertAlmostEqual(np.linalg.norm(dp.sample), 0.1713970501312247, delta=1e-6)

    @unittest.skip("Skipped test case: Correct value needs fixed")
    def test_magnitude(self):
        self.assertIsInstance(self.ds, DataStream)
        self.assertEqual(len(self.ds.datapoints), self.size)

        m = magnitude(normalize(self.ds))
        self.assertIsInstance(m, DataStream)
        for sample in m.datapoints:
            self.assertAlmostEqual(sample.sample, 0.12295653034492039, delta=1e-6)

    def test_interpolate_gaps(self):
        ds = DataStream(None, None)
        ds.datapoints = self.ecg

        result = interpolate_gaps(ds.datapoints, self.sample_rate)

        self.assertEquals(len(ds.datapoints), 377753)
        self.assertEquals(len(result), 378446)

    def test_timestamp_correct(self):
        ds = DataStream(None, None)
        ds.datapoints = self.ecg

        result = timestamp_correct(ds, sampling_frequency=self.sample_rate)

        self.assertEquals(len(ds.datapoints), 377753)
        self.assertEquals(len(result.datapoints), 391686)


if __name__ == '__main__':
    unittest.main()
