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

import pytz


def epoch_align(ts: datetime, offset: float, after: bool = False) -> datetime:
    """
    Epoch timestamp alignment based on offset
    :param ts: datatime object representing the timestamp to start with
    :param offset: seconds as a float, supporting microsecond precision
    :param after: Flag designating if the result should be after ts
    :return: aligned datetime object
    """
    new_timestamp = math.floor(ts.timestamp() * 1e6 / (offset * 1e6)) * offset * 1e6

    if after:
        new_timestamp += offset * 1e6

    result = datetime.fromtimestamp(new_timestamp / 1e6, pytz.timezone('US/Central'))
    return result


def window(data: list,
           window_size: float):
    """
    Special case of a sliding window with no overlaps
    :param data:
    :param window_size:
    :return:
    """
    return window_sliding(data, window_size=window_size, window_offset=window_size)


def window_iter(iterable: list, window_size: float, window_offset: float):
    """
    Window interation function that support various common implementations
    :param iterable:
    :param window_size:
    :param window_offset:
    """
    iterator = iter(iterable)

    win_size = timedelta(seconds=window_size)
    start_time = epoch_align(iterable[0].get_timestamp(), window_offset)
    end_time = start_time + win_size
    key = (start_time, end_time)

    data = []
    for element in iterator:
        timestamp = element.get_timestamp()
        if timestamp > end_time:
            yield key, data

            start_time = epoch_align(element.get_timestamp(), window_offset)
            end_time = start_time + win_size
            key = (start_time, end_time)

            data = [i for i in data if i.get_timestamp() > start_time]

        data.append(element)
    yield key, data


def window_sliding(data: list,
                   window_size: float,
                   window_offset: float):
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
        return None

    if len(data) == 0:
        return None

    windowed_datastream = OrderedDict()

    count = 0
    for key, data in window_iter(data, window_size, window_offset):
        windowed_datastream[key] = data
    return windowed_datastream
