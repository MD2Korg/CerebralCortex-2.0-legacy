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

import numpy as np

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def interpolate_gaps(datastream: DataStream,
                     sampling_frequency: float = None) -> DataStream:
    sampling_interval = 1.0 / sampling_frequency

    max_interpolation_gap = 1.0 / (sampling_frequency * 0.1)

    data = datastream.get_datapoints()
    timedeltas = np.diff([dp.get_timestamp() for dp in data])

    gap_points = []
    low_limit = datetime.timedelta(seconds=2 * sampling_interval)
    high_limit = datetime.timedelta(seconds=max_interpolation_gap)
    for index, value in enumerate(timedeltas):

        if low_limit <= value <= high_limit:
            gap_points.append(data[index])

    print('gaps:', len(gap_points))

def timestamp_correct(datastream: DataStream,
                      sampling_frequency: float = None) -> DataStream:
    result = DataStream.from_datastream(input_streams=[datastream])
    result.set_datapoints(datastream.get_datapoints())
    return result


def timestamp_correct_and_sequence_align(datastream_array: list,
                                         sampling_frequency: float = None) -> DataStream:
    result = DataStream.from_datastream(input_streams=datastream_array)

    data = []
    for dp in datastream_array[0].get_datapoints():
        data.append(DataPoint.from_tuple(dp.get_timestamp(), [dp.get_sample(), dp.get_sample(),
                                                              dp.get_sample()]))  # TODO: Fix with a proper sequence alignment operation later

    result.set_datapoints(data)

    return result
