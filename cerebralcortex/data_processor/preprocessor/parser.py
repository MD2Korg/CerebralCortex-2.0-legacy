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

from datetime import datetime

import pytz

from cerebralcortex.kernel.datatypes.datapoint import DataPoint


def data_processor(input_string):
    try:
        [val, ts] = input_string.split(' ')
        timestamp = datetime.fromtimestamp(float(ts) / 1000.0, pytz.timezone('US/Central'))
        return DataPoint.from_tuple(start_time=timestamp, sample=float(val))
    except ValueError:
        # Skip bad values and filter them later
        # print("ValueError: " + str(input))
        return

def ground_truth_data_processor(input_string):
    try:
        elements = [x.strip() for x in input_string.split(',')]
        start_timestamp = datetime.fromtimestamp(float(elements[2]) / 1000.0, pytz.timezone('US/Central'))
        end_timestamp = datetime.fromtimestamp(float(elements[3]) / 1000.0, pytz.timezone('US/Central'))
        return DataPoint.from_tuple(start_time=start_timestamp, sample=(elements[0], elements[1], elements[4]), end_time=end_timestamp)

    except ValueError:
        return