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
import datetime

from cerebralcortex.data_processor.signalprocessing.window import window_sliding
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.data_processor.signalprocessing.dataquality import Quality

def lomb(data: List[DataPoint],
         low_frequency: float,
         high_frequency: float):
    """
    Lomb–Scargle periodogram implementation
    :param data: List[DataPoint]
    :param high_frequency: float
    :param low_frequency: float
    :return lomb-scargle pgram and frequency values
    """

    time_stamps = np.array([dp.start_time.timestamp() for dp in data])
    samples = np.array([dp.sample for dp in data])
    frequency_range = np.linspace(low_frequency, high_frequency, len(data))
    result = signal.lombscargle(time_stamps, samples, frequency_range)
    return result, frequency_range


def heart_rate_power(power: np.ndarray,
                     frequency: np.ndarray,
                     low_rate: float,
                     high_rate: float):
    """
    Compute Heart Rate Power for specific frequency range
    :param power: np.ndarray
    :param frequency: np.ndarray
    :param high_rate: float
    :param low_rate: float
    :return: sum of power for the frequency range
    """
    result_power = float(0.0)
    for i, value in enumerate(power):
        if low_rate <= frequency[i] <= high_rate:
            result_power += value
    return result_power

def check_ecg_rr_window_quality(quality_datastream_data:List[DataPoint],
                                window_size: float,
                                starttime: datetime,
                                endtime: datetime,
                                acceptable_ratio: float = .66):
    """
    Calculates the quality of RR interval in between the export_data time and end time of a window.
    Renders a boolean decision if the window is usable for feature processing or not.

    """
    quality_datastream_data_reformed = quality_datastream_data
    acceptable_seconds = 0
    for dp in quality_datastream_data:

        if dp.start_time >= starttime and dp.end_time <= endtime and dp.sample == Quality.ACCEPTABLE:
            acceptable_seconds += (dp.end_time-dp.start_time).total_seconds()
        elif starttime <= dp.start_time <= endtime and dp.end_time > endtime and dp.sample == Quality.ACCEPTABLE:
            acceptable_seconds += (endtime-dp.start_time).total_seconds()

        if dp.end_time <= starttime:
            quality_datastream_data_reformed.remove(dp)
        if dp.start_time > endtime:
            break
    if acceptable_seconds/window_size >= acceptable_ratio:
        return True, quality_datastream_data_reformed
    return False, quality_datastream_data_reformed









def ecg_feature_computation(datastream: DataStream,
                            quality_datastream: DataStream,
                            window_size: float,
                            window_offset: float,
                            low_frequency: float = 0.01,
                            high_frequency: float = 0.7,
                            low_rate_vlf: float = 0.0009,
                            high_rate_vlf: float = 0.04,
                            low_rate_hf: float = 0.15,
                            high_rate_hf: float = 0.4,
                            low_rate_lf: float = 0.04,
                            high_rate_lf: float = 0.15):
    """
    ECG Feature Implementation. The frequency ranges for High, Low and Very low heart rate variability values are
    derived from the following paper:
    'Heart rate variability: standards of measurement, physiological interpretation and clinical use'
    :param high_rate_lf: float
    :param low_rate_lf: float
    :param high_rate_hf: float
    :param low_rate_hf: float
    :param high_rate_vlf: float
    :param low_rate_vlf: float
    :param high_frequency: float
    :param low_frequency: float
    :param datastream: DataStream
    :param window_size: float
    :param window_offset: float
    :return: ECG Feature DataStreams
    """

    if datastream is None:
        return None

    if len(datastream.data) == 0:
        return None

    # perform windowing of datastream

    window_data = window_sliding(datastream.data, window_size, window_offset)

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
    rr_LF_HF_data = []
    rr_heart_rate_data = []

    # iterate over each window and calculate features

    quality_datastream_data = quality_datastream.data

    for key, value in window_data.items():

        starttime, endtime = key

        window_quality, quality_datastream_data = check_ecg_rr_window_quality(quality_datastream_data=quality_datastream_data, window_size=window_size,
                                                     starttime=starttime, endtime=endtime)
        if window_quality:
            reference_data = np.array([i.sample for i in value])

            rr_variance_data.append(DataPoint.from_tuple(start_time=starttime,
                                                         end_time=endtime,
                                                         sample=np.var(reference_data)))

            power, frequency = lomb(data=value, low_frequency=low_frequency, high_frequency=high_frequency)

            rr_VLF_data.append(DataPoint.from_tuple(start_time=starttime,
                                                    end_time=endtime,
                                                    sample=heart_rate_power(power, frequency, low_rate_vlf, high_rate_vlf)))

            rr_HF_data.append(DataPoint.from_tuple(start_time=starttime,
                                                   end_time=endtime,
                                                   sample=heart_rate_power(power, frequency, low_rate_hf, high_rate_hf)))

            rr_LF_data.append(DataPoint.from_tuple(start_time=starttime,
                                                   end_time=endtime,
                                                   sample=heart_rate_power(power,
                                                                           frequency,
                                                                           low_rate_lf,
                                                                           high_rate_lf)))
            if heart_rate_power(power, frequency, low_rate_hf, high_rate_hf) != 0:
                lf_hf = float(heart_rate_power(power, frequency, low_rate_lf, high_rate_lf) / heart_rate_power(power,
                                                                                                               frequency,
                                                                                                               low_rate_hf,
                                                                                                               high_rate_hf))
                rr_LF_HF_data.append(DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=lf_hf))
            else:
                rr_LF_HF_data.append(DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=0))

            rr_mean_data.append(
                DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.mean(reference_data)))
            rr_median_data.append(
                DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.median(reference_data)))
            rr_quartile_deviation_data.append(DataPoint.from_tuple(start_time=starttime,
                                                                   end_time=endtime,
                                                                   sample=(0.5 * (
                                                                       np.percentile(reference_data, 75) - np.percentile(
                                                                           reference_data,
                                                                           25)))))
            rr_80percentile_data.append(
                DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.percentile(reference_data, 80)))
            rr_20percentile_data.append(
                DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.percentile(reference_data, 20)))
            rr_heart_rate_data.append(
                DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.median(60 / reference_data)))

    rr_variance = DataStream.from_datastream([datastream])
    rr_variance.data = rr_variance_data
    rr_vlf = DataStream.from_datastream([datastream])
    rr_vlf.data = rr_VLF_data
    rr_hf = DataStream.from_datastream([datastream])
    rr_hf.data = rr_HF_data
    rr_lf = DataStream.from_datastream([datastream])
    rr_lf.data = rr_LF_data
    rr_lf_hf = DataStream.from_datastream([datastream])
    rr_lf_hf.data = rr_LF_HF_data
    rr_mean = DataStream.from_datastream([datastream])
    rr_mean.data = rr_mean_data
    rr_median = DataStream.from_datastream([datastream])
    rr_median.data = rr_median_data
    rr_quartile = DataStream.from_datastream([datastream])
    rr_quartile.data = rr_quartile_deviation_data
    rr_80 = DataStream.from_datastream([datastream])
    rr_80.data = rr_80percentile_data
    rr_20 = DataStream.from_datastream([datastream])
    rr_20.data = rr_20percentile_data
    rr_heart_rate = DataStream.from_datastream([datastream])
    rr_heart_rate.data = rr_heart_rate_data

    return rr_variance, rr_vlf, rr_hf, rr_lf, rr_lf_hf, rr_mean, rr_median, rr_quartile, rr_80, rr_20, rr_heart_rate

