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
from datetime import datetime, timedelta
from typing import List

import pytz
from collections import OrderedDict

from cerebralcortex.kernel.datatypes.datapoint import DataPoint


def window(data: List[DataPoint],
           window_size: float, all_windows: bool = False) -> OrderedDict:
    """
    Special case of a sliding window with no overlaps
    :param data:
    :param window_size:
    :param all_windows: setting it to "True" will return a complete list (empty and non-empty) windows. Otherwise, it will only return windows that contain data (non-empty).
    :return:
    """
    return window_sliding(data, window_size=window_size, window_offset=window_size, all_windows=all_windows)


def window_sliding(data: List[DataPoint],
                   window_size: float,
                   window_offset: float, all_windows=False) -> OrderedDict:
    """
    Sliding Window Implementation

    :param all_windows:
    :param data: list
    :param window_size: float
    :param window_offset: float
    :param all_windows: setting it to "True" will return a complete list (empty and non-empty) windows. Otherwise, it will only return windows that contain data (non-empty).
    :return: OrderedDict representing [(st,et),[dp,dp,dp,dp...],
                                       (st,et),[dp,dp,dp,dp...],
                                        ...]
    """
    windowed_datastream = OrderedDict()

    if data is None or len(data) == 0:
        return windowed_datastream
        #raise TypeError('data is not List[DataPoint]')

    #if len(data) == 0:
        #raise ValueError('The length of data is zero')

    if all_windows:
        for _key, _data in create_all_windows(data, window_size, window_offset):
            windowed_datastream[_key] = _data
    else:
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


def create_all_windows(datapoint: List[DataPoint], window_size: float, window_offset: float):
    """
    This method will create a complete list of a windows between export_data and end time of the data provided.
    :param datapoint:
    :param window_size:
    :param window_offset:
    :return: a tupal of windowed data: [(st,et),[dp,dp,dp,dp...], {if a window contains data}
                                       (st,et),[], (if a window does not contain any data}
                                        ...]
    """
    window_start_time = epoch_align(datapoint[0].start_time, window_offset)
    window_end_time = window_start_time + timedelta(seconds=window_size)
    window_data = []
    for dp in datapoint:
        if window_start_time <= dp.start_time <= window_end_time:
            window_data.append(dp)
        else:
            key = (window_start_time, window_end_time)
            yield key, window_data
            window_data = []
            # when datapoint is not in current range, identify emtpy windows and yield.
            _w_start_time = window_end_time
            _w_end_time = _w_start_time + timedelta(seconds=window_size)
            while dp.start_time > _w_end_time:
                key = (_w_start_time, _w_end_time)
                yield key, []
                window_data = []
                _w_start_time = _w_end_time
                _w_end_time = _w_start_time + timedelta(seconds=window_size)

            window_data.append(dp)
            window_end_time = _w_end_time
            window_start_time = _w_start_time
    key = (window_start_time, window_end_time)
    yield key, window_data


def epoch_align(ts: datetime,
                offset: float,
                after: bool = False,
                time_zone: pytz = pytz.timezone('US/Central'),
                time_base: int = 1e6) -> datetime:
    """
    Epoch timestamp alignment based on offset

    :param time_zone: Specifiy the timezone of the timestamps, default US/Central
    :param ts: datatime object representing the timestamp to export_data with
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
