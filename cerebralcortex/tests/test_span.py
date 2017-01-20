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

from cerebralcortex.kernel.datatypes.span import Span


class TestSpan(unittest.TestCase):
    def test_Span_None(self):
        dp = Span()

        (starttime, endtime) = dp.get_time_tuple()

        self.assertIsNone(starttime)
        self.assertIsNone(endtime)
        self.assertIsNone(dp.get_label())

    def test_Span(self):
        ts = datetime.datetime.now()
        dp = Span(spanstream=123, starttime=ts, endtime=ts + datetime.timedelta(hours=1), label='RED')

        (starttime, endtime) = dp.get_time_tuple()

        self.assertEqual(ts, starttime)
        self.assertEqual(ts + datetime.timedelta(hours=1), endtime)
        self.assertEqual(dp.get_label(), 'RED')


if __name__ == '__main__':
    unittest.main()
