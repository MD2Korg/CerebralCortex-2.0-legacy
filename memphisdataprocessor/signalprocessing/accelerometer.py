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

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.window import window
from memphisdataprocessor.signalprocessing.vector import magnitude, normalize


def window_std_dev(datapoints: List[DataPoint],
                   window_start: datetime) -> DataPoint:
    """

    :param datapoints:
    :param window_start:
    :return:
    """
    data = np.array([dp.sample for dp in datapoints])
    return DataPoint.from_tuple(window_start, np.std(data))


def accelerometer_features(accel: DataStream,
                           window_length: float = 10.0,
                           activity_threshold: float = 0.21,
                           # From Figure 3: http://www.cs.memphis.edu/~santosh/Papers/Timing-JIT-UbiComp-2014.pdf
                           percentile_low: int = 1,
                           percentile_high: int = 99):

    """

    :param percentile_high:
    :param percentile_low:
    :param accel:
    :param window_length:
    :param activity_threshold:
    :return:
    """
    accelerometer_magnitude = magnitude(normalize(accel))

    accelerometer_win_mag_deviations_data = []
    for key, data in window(accelerometer_magnitude.datapoints, window_length).items():
        accelerometer_win_mag_deviations_data.append(window_std_dev(data, key[0]))

    accelerometer_win_mag_deviations = DataStream.from_datastream([accel])
    accelerometer_win_mag_deviations.datapoints = accelerometer_win_mag_deviations_data

    am_values = np.array([dp.sample for dp in accelerometer_magnitude.datapoints])
    low_limit = np.percentile(am_values, percentile_low)
    high_limit = np.percentile(am_values, percentile_high)
    time_range = high_limit - low_limit

    accel_activity_data = []
    for dp in accelerometer_win_mag_deviations_data:
        comparison = dp.sample > (low_limit + activity_threshold * time_range)
        accel_activity_data.append(DataPoint.from_tuple(dp.start_time, comparison))

    accel_activity = DataStream.from_datastream([accel])
    accel_activity.datapoints = accel_activity_data

    return accelerometer_magnitude, accelerometer_win_mag_deviations, accel_activity
