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
import time
from typing import List

import numpy as np
from fastdtw import fastdtw
from scipy.interpolate import pchip

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def interpolate_gaps(data: List[DataPoint],
                     sampling_frequency: float = None) -> List[DataPoint]:
    sampling_interval = 1.0 / sampling_frequency

    max_interpolation_gap = sampling_interval * 10

    time_deltas = np.diff([dp.start_time for dp in data])

    gap_points = []
    low_limit = datetime.timedelta(seconds=2 * sampling_interval)
    high_limit = datetime.timedelta(seconds=max_interpolation_gap)

    for index, value in enumerate(time_deltas):
        if low_limit <= value <= high_limit:
            gap_points.append((data[index], value))

    gap_index = 0
    in_gap = False
    gap_data = []
    low_time = gap_points[gap_index][0].start_time - datetime.timedelta(seconds=max_interpolation_gap)
    high_time = gap_points[gap_index][0].start_time + datetime.timedelta(seconds=max_interpolation_gap)

    current_timezone = data[0].start_time.tzinfo
    new_datapoints = []
    for dp in data:
        if low_time <= dp.start_time <= high_time:
            in_gap = True
            gap_data.append(dp)
        elif in_gap:
            in_gap = False

            fix_start = gap_points[gap_index][0].start_time.timestamp()
            fix_end = fix_start + gap_points[gap_index][1].total_seconds()

            x = np.array([dp.start_time.timestamp() for dp in gap_data])
            y = np.array([dp.sample for dp in gap_data])
            interpolated_signal = pchip(x, y)

            new_x = [i for i in frange(fix_start + sampling_interval, fix_end, sampling_interval)]
            new_y = interpolated_signal(new_x)

            for i, value in enumerate(new_y):
                new_datapoints.append(
                    DataPoint.from_tuple(datetime.datetime.fromtimestamp(new_x[i], tz=current_timezone), None, value))

            gap_index += 1
            if gap_index == len(gap_points):
                break
            low_time = gap_points[gap_index][0].start_time - datetime.timedelta(seconds=max_interpolation_gap)
            high_time = gap_points[gap_index][0].start_time + datetime.timedelta(seconds=max_interpolation_gap)

            gap_data = [dp for dp in gap_data if low_time <= dp.start_time <= high_time]

    result = []
    index = 0
    for dp in data:
        while index < len(new_datapoints) and dp.start_time > new_datapoints[index].start_time:
            result.append(new_datapoints[index])
            index += 1

        result.append(dp)

    return result


def frange(start, stop, step):
    i = start
    while i <= stop:
        yield i
        i += step


def split_by_gaps(datastream: DataStream,
                  min_available_gaps: int = 3600,  # TODO: Does this matter anymore?
                  min_split_gap: datetime.timedelta = datetime.timedelta(seconds=30),
                  max_data_points_per_segment: int = 100000000):
    data = datastream.datapoints
    time_deltas = np.diff([dp.start_time for dp in data])

    gap_points = [data[0]]
    for index, value in enumerate(time_deltas):
        if value > min_split_gap:
            gap_points.append(data[index])
    gap_points.append(data[-1])

    segments = []
    segment_data = []
    gap_index = 0
    low_time = gap_points[gap_index].start_time
    high_time = gap_points[gap_index + 1].start_time
    for dp in data:
        if len(segment_data) >= max_data_points_per_segment:
            segments.append(interpolate_gaps(segment_data, 64.0))
            segment_data = []

        if low_time <= dp.start_time <= high_time:
            segment_data.append(dp)
        else:
            segments.append(interpolate_gaps(segment_data, 64.0))
            gap_index += 1
            low_time = gap_points[gap_index].start_time
            high_time = gap_points[gap_index + 1].start_time
            segment_data = []

    segments.append(interpolate_gaps(segment_data, 64.0))

    start = time.time()
    for s in segments:
        loop_start = time.time()
        begin_time = s[0].start_time.timestamp()
        end_time = s[-1].start_time.timestamp()

        x = np.array([i for i in frange(begin_time, end_time, 1.0 / 64.0)], dtype='float')
        y = np.array([dp.start_time.timestamp() for dp in s], dtype='float')

        distance, path = fastdtw(x, y, radius=1)
        loop_end = time.time()
        print('loop_time', loop_end - loop_start)

        xx = [0 for i in y]
        for si, ei in path:
            xx[ei] = x[si]

            # plt.plot(y, [a.sample for a in s], '-x')
            # plt.plot(xx, [a.sample for a in s], '-o')
            # plt.plot(x, [1800 for a in x], '+')

    end = time.time()
    print('total_time:', end - start)
    # plt.show()


def timestamp_correct(datastream: DataStream,
                      sampling_frequency: float = None) -> DataStream:
    result = DataStream.from_datastream(input_streams=[datastream])
    result.datapoints = datastream.datapoints
    return result


def timestamp_correct_and_sequence_align(datastream_array: list,
                                         sampling_frequency: float = None) -> DataStream:
    result = DataStream.from_datastream(input_streams=datastream_array)

    data = []
    for dp in datastream_array[0].datapoints:
        data.append(DataPoint.from_tuple(dp.start_time, [dp.sample, dp.sample,
                                                         dp.sample]))  # TODO: Fix with a proper sequence alignment operation later

    result.datapoints = data

    return result
