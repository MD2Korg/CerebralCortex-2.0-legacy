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
import datetime
from typing import List

import numpy as np
from numpy.linalg import norm
from sklearn import preprocessing

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def normalize(datastream: DataStream) -> DataStream:
    """

    :param datastream:
    :return:
    """
    result = DataStream.from_datastream(input_streams=[datastream])
    if datastream.data is None or len(datastream.data) == 0:
        result.data = []
        return result

    input_data = np.array([i.sample for i in datastream.data])

    data = preprocessing.normalize(input_data, axis=0)

    result.data = [DataPoint.from_tuple(start_time=v.start_time, sample=data[i])
                   for i, v in enumerate(datastream.data)]

    return result


def magnitude(datastream: DataStream) -> DataStream:
    """

    :param datastream:
    :return:
    """
    result = DataStream.from_datastream(input_streams=[datastream])
    if datastream.data is None or len(datastream.data) == 0:
        result.data = []
        return result

    input_data = np.array([i.sample for i in datastream.data])

    data = norm(input_data, axis=1).tolist()

    result.data = [DataPoint.from_tuple(start_time=v.start_time, sample=data[i])
                   for i, v in enumerate(datastream.data)]

    return result


def smooth(data: List[DataPoint],
           span: int = 5) -> List[DataPoint]:
    """
    Smooths data using moving average filter over a span.
    The first few elements of data_smooth are given by
    data_smooth(1) = data(1)
    data_smooth(2) = (data(1) + data(2) + data(3))/3
    data_smooth(3) = (data(1) + data(2) + data(3) + data(4) + data(5))/5
    data_smooth(4) = (data(2) + data(3) + data(4) + data(5) + data(6))/5

    for more details follow the below links:
    https://www.mathworks.com/help/curvefit/smooth.html
    http://stackoverflow.com/a/40443565

    :return: data_smooth
    :param data:
    :param span:
    """

    if data is None or len(data) == 0:
        return []

    sample = [i.sample for i in data]
    sample_middle = np.convolve(sample, np.ones(span, dtype=int), 'valid') / span
    divisor = np.arange(1, span - 1, 2)
    sample_start = np.cumsum(sample[:span - 1])[::2] / divisor
    sample_end = (np.cumsum(sample[:-span:-1])[::2] / divisor)[::-1]
    sample_smooth = np.concatenate((sample_start, sample_middle, sample_end))

    data_smooth = []

    if len(sample_smooth) == len(data):
        for i, item in enumerate(data):
            dp = DataPoint.from_tuple(sample=sample_smooth[i], start_time=item.start_time, end_time=item.end_time)
            data_smooth.append(dp)
    else:
        raise Exception("Smoothed data length does not match with original data length.")

    return data_smooth


def moving_average_curve(data: List[DataPoint],
                         window_length: int) -> List[DataPoint]:
    """
    Moving average curve from filtered (using moving average) samples.

    :return: mac
    :param data:
    :param window_length:
    """
    if data is None or len(data) == 0:
        return []

    sample = [i.sample for i in data]
    mac = []
    for i in range(window_length, len(sample) - (window_length + 1)):
        sample_avg = np.mean(sample[i - window_length:i + window_length + 1])
        mac.append(DataPoint.from_tuple(sample=sample_avg, start_time=data[i].start_time, end_time=data[i].end_time))

    return mac


def window_std_dev(data: List[DataPoint],
                   window_start: datetime) -> DataPoint:
    """

    :param data:
    :param window_start:
    :return:
    """

    if data is None or len(data) < 2:
        raise Exception('Standard deviation requires at least 2 values to compute')

    data_points = np.array([dp.sample for dp in data])
    return DataPoint.from_tuple(window_start, np.std(data_points))
