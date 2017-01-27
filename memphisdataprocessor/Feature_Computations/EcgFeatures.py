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
from typing import List

import numpy as np
import scipy.signal as signal
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.window import window_sliding
from cerebralcortex.kernel.datatypes.datapoint import DataPoint


def lomb(data: List[DataPoint],
         low_frequency: float,
         high_frequency: float):
    """
    Lomb–Scargle periodogram implementation
    :param data: List[DataPoint]
    :param high_frequency: float
    :param low_frequency: float
    """

    time_stamps = np.array([dp.get_timestamp_epoch() for dp in data])
    samples = np.array([dp.get_sample() for dp in data])
    f = np.linspace(low_frequency, high_frequency, len(data))
    result = signal.lombscargle(time_stamps, samples, f)
    return result, f


def heart_rate_power(power: np.ndarray,
                     frequency: np.ndarray,
                     high_rate: float,
                     low_rate: float):
    """
    Compute Heart Rate Power for specific frequency range
    :param power: np.ndarray
    :param frequency: np.ndarray
    :param high_rate: float
    :param low_rate: float
    :return: sum of power for the frequency range
    """
    result_power = 0
    for i in range(len(power)):
        if low_rate <= frequency[i] <= high_rate:
            result_power += power[i]
    return result_power


def low_high_ratio(data: List[DataPoint]):
    pass


def ecg_feature_computation(datastream: DataStream,
                            window_size: float,
                            window_offset: float):
    """
    ECG Feature Implementation
    :param datastream: DataStream
    :param window_size: float
    :param window_offset: float
    :return: ECG Feature DataStreams
    """

    if datastream is None:
        return None

    if len(datastream) == 0:
        return None

    # perform windowing of datastream

    window_data = window_sliding(datastream, window_size, window_offset)

    # initialize each ecg feature array

    rr_variance_data = []
    rr_mean_data = []
    rr_median_data = []
    rr_80percentile_data = []
    rr_20percentile_data = []
    rr_quartile_deviation_data = []
    rr_HF_data = []
    rr_LF_data = []
    rr_VLF_data = []
    rr_MF_data = []
    rr_LF_HF_data = []

    # ds_rr_variance = DataStream.from_DataStream(datastream)

    # iterate over each window and calculate features

    for key, value in window_data.items():
        reference_data = np.array([i.get_sample() for i in value])
        rr_variance_data.append(DataPoint(key[0],np.var(reference_data)))
        power, frequency = lomb(data=reference_data, low_frequency=0.01, high_frequency=0.4)
        rr_HF_data.append(heart_rate_power(power, frequency, 0.15, 0.4))
        # rr_MF_data.append(heart_rate_power(power, frequency, )
        rr_LF_data.append(heart_rate_power(power, frequency, 0.15, 0.4))
        rr_VLF_data.append(heart_rate_power(power, frequency, 0.15, 0.4))
        rr_LF_HF_data.append((low_high_ratio(reference_data,len(reference_data))))
        rr_mean_data.append(np.mean(reference_data))
        rr_median_data.append(np.median(reference_data))
        rr_quartile_deviation_data.append((0.5 * (np.percentile(reference_data, 75) - np.percentile(reference_data, 25))))
        rr_80percentile_data.append((np.percentile(reference_data, 80)))
        rr_20percentile_data.append((np.percentile(reference_data, 20)))
        # ds_rr_variance.set_data(rr_variance)
        # return ds_rr_variance

        # rr_variance = DataStream.from_datastream([datastream])
        # rr_variance.set_datapoints(rr_variance_data)
    return rr_variance_data\
        # , rr_mean, rr_median, rr_quartile_deviation, rr_80percentile, rr_20percentile