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

from cerebralcortex.data_processor.signalprocessing.alignment import autosense_sequence_align
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
        cls.ds = DataStream(None, None, data=cls.ecg)

        accelx = []
        accel_sample_rate = 64.0 / 6
        with gzip.open(os.path.join(os.path.dirname(__file__), 'res/accelx.csv.gz'), 'rt') as f:
            for l in f:
                values = list(map(int, l.split(',')))
                accelx.append(
                    DataPoint.from_tuple(datetime.datetime.fromtimestamp(values[0] / 1000000.0, tz=tz), values[1]))
        accelx = DataStream(None, None, data=accelx)

        accely = []
        with gzip.open(os.path.join(os.path.dirname(__file__), 'res/accely.csv.gz'), 'rt') as f:
            for l in f:
                values = list(map(int, l.split(',')))
                accely.append(
                    DataPoint.from_tuple(datetime.datetime.fromtimestamp(values[0] / 1000000.0, tz=tz), values[1]))
        accely = DataStream(None, None, data=accely)

        accelz = []
        with gzip.open(os.path.join(os.path.dirname(__file__), 'res/accelz.csv.gz'), 'rt') as f:
            for l in f:
                values = list(map(int, l.split(',')))
                accelz.append(
                    DataPoint.from_tuple(datetime.datetime.fromtimestamp(values[0] / 1000000.0, tz=tz), values[1]))
        accelz = DataStream(None, None, data=accelz)

        cls.accel = autosense_sequence_align([accelx, accely, accelz], accel_sample_rate)

    def test_autosense_sequence_align(self):
        self.assertIsInstance(self.accel, DataStream)
        self.assertEqual(len(self.accel.data), 62870)

    def test_normalize(self):
        self.assertIsInstance(self.accel, DataStream)

        n = normalize(self.accel)
        self.assertIsInstance(n, DataStream)

        # for dp in n.data:
        #     self.assertAlmostEqual(np.linalg.norm(dp.sample), 0.14815058577882137, delta=1e-6)

    def test_magnitude(self):
        self.assertIsInstance(self.accel, DataStream)

        m = magnitude(normalize(self.accel))
        self.assertIsInstance(m, DataStream)
        # for sample in m.data:
        #     self.assertAlmostEqual(sample.sample, 0.1857005338535236, delta=1e-6)

    def test_magnitude_empty(self):
        ds = DataStream(None, None, data=[])
        self.assertIsInstance(ds, DataStream)

        m = magnitude(normalize(ds))
        self.assertIsInstance(m, DataStream)
        self.assertEqual(len(m.data), 0)

    def test_normalize_empty(self):
        ds = DataStream(None, None, data=[])
        self.assertIsInstance(ds, DataStream)

        m = normalize(ds)
        self.assertIsInstance(m, DataStream)
        self.assertEqual(len(m.data), 0)


if __name__ == '__main__':
    unittest.main()
