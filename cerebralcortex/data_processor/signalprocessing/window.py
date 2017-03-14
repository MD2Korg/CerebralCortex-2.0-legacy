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

import math
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import List

import pytz

from cerebralcortex.kernel.datatypes.datapoint import DataPoint


def epoch_align(ts: datetime,
                offset: float,
                after: bool = False,
                time_zone: pytz = pytz.timezone('US/Central'),
                time_base: int = 1e6) -> datetime:
    """
    Epoch timestamp alignment based on offset

    :param time_zone: Specifiy the timezone of the timestamps, default US/Central
    :param ts: datatime object representing the timestamp to start with
    :param offset: seconds as a float
    :param after: Flag designating if the result should be after ts
    :param time_base: specifies the precision with which the time base should be manipulated (1e6 -> microseconds)
    :return: aligned datetime object
    """
    new_timestamp = math.floor(ts.timestamp() * time_base / (offset * time_base)) * offset * time_base

    if after:
        new_timestamp += offset * time_base

    result = datetime.fromtimestamp(new_timestamp / time_base, time_zone)
    return result


def window(data: List[DataPoint],
           window_size: float) -> OrderedDict:
    """
    Special case of a sliding window with no overlaps
    :param data:
    :param window_size:
    :return:
    """
    return window_sliding(data, window_size=window_size, window_offset=window_size)


def window_sliding(data: List[DataPoint],
                   window_size: float,
                   window_offset: float) -> OrderedDict:
    """
    Sliding Window Implementation

    :param data: list
    :param window_size: float
    :param window_offset: float
    :return: OrderedDict representing [(st,et),[dp,dp,dp,dp...],
                                       (st,et),[dp,dp,dp,dp...],
                                        ...]
    """
    if data is None:
        raise TypeError('data is not List[DataPoint]')

    if len(data) == 0:
        raise ValueError('The length of data is zero')

    windowed_datastream = OrderedDict()

    for key, data in window_iter(data, window_size, window_offset):
        windowed_datastream[key] = data

    return windowed_datastream


def window_iter(iterable: List[DataPoint],
                window_size: float,
                window_offset: float):
    """
    Window iteration function that support various common implementations
    :param iterable:
    :param window_size:
    :param window_offset:
    """
    iterator = iter(iterable)

    win_size = timedelta(seconds=window_size)
    start_time = epoch_align(iterable[0].start_time, window_offset)
    end_time = start_time + win_size
    key = (start_time, end_time)

    data = []
    for element in iterator:
        timestamp = element.start_time
        if timestamp > end_time:
            yield key, data

            start_time = epoch_align(element.start_time, window_offset)
            end_time = start_time + win_size
            key = (start_time, end_time)

            data = [i for i in data if i.start_time > start_time]

        data.append(element)
    yield key, data

