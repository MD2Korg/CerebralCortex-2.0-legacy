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
from collections import OrderedDict
from datetime import datetime, timedelta
from random import random
from time import sleep

import pytz

from cerebralcortex.data_processor.signalprocessing.window import window_sliding, epoch_align
from cerebralcortex.kernel.datatypes.datapoint import DataPoint


class TestWindowing(unittest.TestCase):
    def setUp(self):
        self.timezone = pytz.timezone('US/Central')

    def test_Window_None(self):
        data = None
        self.assertRaises(TypeError, window_sliding, data, window_size=60.0, window_offset=10.0)

    def test_Window_Empty(self):
        data = []
        self.assertRaises(ValueError, window_sliding, data, window_size=60.0, window_offset=10.0)

    def test_Window_Valid(self):
        data = []
        for i in range(0, 100):
            data.append(DataPoint.from_tuple(datetime.now(tz=self.timezone), None, random()))
            sleep(0.01)

        self.assertEqual(100, len(data))
        result = window_sliding(data, window_size=0.25, window_offset=0.05)
        # TODO: check results of function output

        self.assertIsInstance(result, OrderedDict)

    def test_epoch_align(self):
        timestamps = [(datetime.fromtimestamp(123456789, tz=self.timezone), 0.01,
                       datetime.fromtimestamp(123456789, tz=self.timezone)),
                      (datetime.fromtimestamp(123456789, tz=self.timezone), 0.1,
                       datetime.fromtimestamp(123456789, tz=self.timezone)),
                      (datetime.fromtimestamp(123456789, tz=self.timezone), 1.0,
                       datetime.fromtimestamp(123456789, tz=self.timezone)),
                      (datetime.fromtimestamp(123456789, tz=self.timezone), 10.0,
                       datetime.fromtimestamp(123456780, tz=self.timezone)),
                      (datetime.fromtimestamp(123456789.5678, tz=self.timezone), 0.01,
                       datetime.fromtimestamp(123456789.5600, tz=self.timezone))
                      ]
        for ts, offset, correct in timestamps:
            with self.subTest(ts=ts, offset=offset, correct=correct):
                self.assertEqual(correct, epoch_align(ts, offset))
                self.assertEqual(correct + timedelta(seconds=offset), epoch_align(ts, offset, after=True))
                self.assertNotEqual(correct, epoch_align(ts, offset + 1))

    def test_epoch_align_intervals(self):

        timestamp = datetime.fromtimestamp(1484929672.918273, tz=self.timezone)
        for interval in [1000.0, 100.0, 10.0, 5.0, 1.0, 0.5, 0.1, 0.01, 0.001, 0.23, 0.45]:
            with self.subTest(interval=interval):
                aligned = epoch_align(timestamp, interval)
                reference = (int(int(timestamp.timestamp() * 1e6) / int(interval * 1e6)) * interval)
                self.assertAlmostEqual(aligned.timestamp(), reference, delta=1e-6)


if __name__ == '__main__':
    unittest.main()
