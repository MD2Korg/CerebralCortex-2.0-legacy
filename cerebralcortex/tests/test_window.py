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
from datetime import datetime
from random import random
from time import sleep

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.window import window_sliding


class TestWindowing(unittest.TestCase):
    def test_Window_None(self):
        data = None
        result = window_sliding(data, window_size=60000, window_offset=10000)
        self.assertIsNone(result)

    def test_Window_Empty(self):
        data = []
        result = window_sliding(data, window_size=60000, window_offset=10000)
        self.assertIsNone(result)

    def test_Window_Valid(self):
        data = []
        for i in range(0, 10):
            data.append(DataPoint(i, 1, datetime.now(), random()))
            sleep(0.1)

        self.assertEquals(10, len(data))
        result = window_sliding(data, window_size=500, window_offset=100)

        self.assertIsInstance(result, OrderedDict)


if __name__ == '__main__':
    unittest.main()
