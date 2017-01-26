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

import matplotlib.pyplot as plt
import numpy as np
from fastdtw import fastdtw

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def interpolate_gaps(datastream: DataStream,
                     sampling_frequency: float = None) -> DataStream:
    sampling_interval = 1.0 / sampling_frequency

    max_interpolation_gap = sampling_interval * 10

    data = datastream.get_datapoints()
    time_deltas = np.diff([dp.get_timestamp() for dp in data])

    gap_points = []
    low_limit = datetime.timedelta(seconds=2 * sampling_interval)
    high_limit = datetime.timedelta(seconds=max_interpolation_gap)

    for index, value in enumerate(time_deltas):
        if low_limit <= value <= high_limit:
            gap_points.append(data[index])

    gap_index = 0
    in_gap = False
    gap_data = []
    low_time = gap_points[gap_index].get_timestamp() - datetime.timedelta(seconds=max_interpolation_gap)
    high_time = gap_points[gap_index].get_timestamp() + datetime.timedelta(seconds=max_interpolation_gap)

    # TODO: Fix pchip https://docs.scipy.org/doc/scipy-0.18.1/reference/generated/scipy.interpolate.PchipInterpolator.html#scipy.interpolate.PchipInterpolator
    # TODO: output proper values once timestamp corrected
    for dp in data:

        if low_time <= dp.get_timestamp() <= high_time:
            in_gap = True
            gap_data.append(dp)
        elif in_gap:
            in_gap = False
            gap_index += 1
            low_time = gap_points[gap_index].get_timestamp() - datetime.timedelta(seconds=max_interpolation_gap)
            high_time = gap_points[gap_index].get_timestamp() + datetime.timedelta(seconds=max_interpolation_gap)

            gap_data = [dp for dp in gap_data if low_time <= dp.get_timestamp() <= high_time]
            # print(len(gap_data))

    for gp in gap_points:
        low_time = gp.get_timestamp() - datetime.timedelta(seconds=max_interpolation_gap)
        high_time = gp.get_timestamp() + datetime.timedelta(seconds=max_interpolation_gap)
        # print(len([dp for dp in data if low_time <= dp.get_timestamp() <= high_time]))

    print('gaps:', len(gap_points))
    print('gap_i:', gap_index)


def frange(start, stop, step):
    i = start
    while i <= stop:
        yield i
        i += step


def split_by_gaps(datastream: DataStream,
                  min_available_gaps: int = 3600,
                  min_split_gap: datetime.timedelta = datetime.timedelta(seconds=30),
                  max_data_points_per_segment: int = 100000000):
    data = datastream.get_datapoints()
    time_deltas = np.diff([dp.get_timestamp() for dp in data])

    gap_points = [data[0]]
    for index, value in enumerate(time_deltas):
        if value > min_split_gap:
            gap_points.append(data[index])
    gap_points.append(data[-1])

    segments = []
    segment_data = []
    gap_index = 0
    low_time = gap_points[gap_index].get_timestamp()
    high_time = gap_points[gap_index + 1].get_timestamp()
    for dp in data:
        if len(segment_data) >= max_data_points_per_segment:
            segments.append(segment_data)
            segment_data = []

        if low_time <= dp.get_timestamp() <= high_time:
            segment_data.append(dp)
        else:
            segments.append(segment_data)
            gap_index += 1
            low_time = gap_points[gap_index].get_timestamp()
            high_time = gap_points[gap_index + 1].get_timestamp()
            segment_data = []
    segments.append(segment_data)

    start = time.time()
    for s in segments:
        loop_start = time.time()
        begin_time = s[0].get_timestamp().timestamp()
        end_time = s[-1].get_timestamp().timestamp()

        x = np.array([i for i in frange(begin_time, end_time, 1.0 / 64.0)], dtype='float')
        y = np.array([dp.get_timestamp().timestamp() for dp in s], dtype='float')

        distance, path = fastdtw(x, y, radius=25)  # dist=euclidean,
        loop_end = time.time()
        print('loop_time', loop_end - loop_start)

        yy = [0 for i in y]
        xx = [0 for i in y]
        for si, ei in path:
            xx[ei] = x[si]
            yy[ei] = y[ei]

        # plt.plot(y, xx, 'o')
        # plt.plot(y, xx2, '-')
        # plt.plot(x, [i/len(x) for i in range(0,len(x))], '-x')
        # plt.plot(y, [i/len(y) for i in range(0,len(y))], '-o')
        # plt.plot(yy, [i/len(yy) for i in range(0,len(yy))], '-+')
        plt.plot(xx, yy, '-+')

    end = time.time()
    print('total_time:', end - start)
    plt.show()


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
