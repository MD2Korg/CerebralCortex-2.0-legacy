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
import unittest
from random import random

import numpy as np

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from memphisdataprocessor.signalprocessing.vector import normalize, magnitude


class TestVector(unittest.TestCase):
    def setUp(self):
        self.size = 100
        self.ds = DataStream(None, None)
        data = [DataPoint.from_tuple(self.ds, datetime.datetime.now(), [random() * 100, random() * 10, random()]) for i
                in range(0, self.size)]
        self.ds.set_datapoints(data)

    def test_normalize(self):
        self.assertIsInstance(self.ds, DataStream)
        self.assertEqual(len(self.ds.get_datapoints()), self.size)

        n = normalize(self.ds)
        self.assertIsInstance(n, DataStream)
        for dp in n.get_datapoints():
            self.assertAlmostEqual(np.linalg.norm(dp.get_sample()), 1.0, delta=1e-6)

    def test_magnitude(self):
        self.assertIsInstance(self.ds, DataStream)
        self.assertEqual(len(self.ds.get_datapoints()), self.size)

        m = magnitude(normalize(self.ds))
        self.assertIsInstance(m, DataStream)
        for sample in m.get_datapoints():
            self.assertAlmostEqual(sample.get_sample(), 1.0, delta=1e-6)


if __name__ == '__main__':
    unittest.main()
