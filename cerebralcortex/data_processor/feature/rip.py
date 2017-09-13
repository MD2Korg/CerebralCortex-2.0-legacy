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
from typing import Tuple
from typing import List

import numpy as np
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.data_processor.signalprocessing.window import window_sliding



def rip_cycle_feature_computation(peaks_datastream: DataStream,
                                  valleys_datastream: DataStream) -> Tuple[DataStream]:
    """
    Respiration Feature Implementation. The respiration feature values are
    derived from the following paper:
    'puffMarker: a multi-sensor approach for pinpointing the timing of first lapse in smoking cessation'


    Removed due to lack of current use in the implementation
    roc_max = []  # 8. ROC_MAX = max(sample[j]-sample[j-1])
    roc_min = []  # 9. ROC_MIN = min(sample[j]-sample[j-1])


    :param peaks_datastream: DataStream
    :param valleys_datastream: DataStream
    :return: RIP Feature DataStreams
    """

    # TODO: This needs fixed to prevent crashing the execution pipeline
    if peaks_datastream is None or valleys_datastream is None:
        return None

    # TODO: This needs fixed to prevent crashing the execution pipeline
    if len(peaks_datastream.data) == 0 or len(valleys_datastream.data) == 0:
        return None

    inspiration_duration = []  # 1 Inhalation duration
    expiration_duration = []  # 2 Exhalation duration
    respiration_duration = []  # 3 Respiration duration
    inspiration_expiration_ratio = []  # 4 Inhalation and Exhalation ratio
    stretch = []  # 5 Stretch
    upper_stretch = []  # 6. Upper portion of the stretch calculation
    lower_stretch = []  # 7. Lower portion of the stretch calculation
    delta_previous_inspiration_duration = []  # 10. BD_INSP = INSP(i)-INSP(i-1)
    delta_previous_expiration_duration = []  # 11. BD_EXPR = EXPR(i)-EXPR(i-1)
    delta_previous_respiration_duration = []  # 12. BD_RESP = RESP(i)-RESP(i-1)
    delta_previous_stretch_duration = []  # 14. BD_Stretch= Stretch(i)-Stretch(i-1)
    delta_next_inspiration_duration = []  # 19. FD_INSP = INSP(i)-INSP(i+1)
    delta_next_expiration_duration = []  # 20. FD_EXPR = EXPR(i)-EXPR(i+1)
    delta_next_respiration_duration = []  # 21. FD_RESP = RESP(i)-RESP(i+1)
    delta_next_stretch_duration = []  # 23. FD_Stretch= Stretch(i)-Stretch(i+1)
    neighbor_ratio_expiration_duration = []  # 29. D5_EXPR(i) = EXPR(i) / avg(EXPR(i-2)...EXPR(i+2))
    neighbor_ratio_stretch_duration = []  # 32. D5_Stretch = Stretch(i) / avg(Stretch(i-2)...Stretch(i+2))

    valleys = valleys_datastream.data
    peaks = peaks_datastream.data[:-1]

    for i, peak in enumerate(peaks):
        valley_start_time = valleys[i].start_time

        delta = peak.start_time - valleys[i].start_time
        inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta.total_seconds()))

        delta = valleys[i + 1].start_time - peak.start_time
        expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta.total_seconds()))

        delta = valleys[i + 1].start_time - valley_start_time
        respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta.total_seconds()))

        ratio = (peak.start_time - valley_start_time) / (valleys[i + 1].start_time - peak.start_time)
        inspiration_expiration_ratio.append(DataPoint.from_tuple(start_time=valley_start_time, sample=ratio))

        value = peak.sample - valleys[i + 1].sample
        stretch.append(DataPoint.from_tuple(start_time=valley_start_time, sample=value))

        # upper_stretch.append(DataPoint.from_tuple(start_time=valley_start_time, sample=(peak.sample - valleys[i + 1][1]) / 2))  # TODO: Fix this by adding a tracking moving average and compute upper stretch from this to the peak and lower from this to the valley
        # lower_stretch.append(DataPoint.from_tuple(start_time=valley_start_time, sample=(-peak.sample + valleys[i + 1][1]) / 2))  # TODO: Fix this by adding a tracking moving average and compute upper stretch from this to the peak and lower from this to the valley

    for i in range(len(inspiration_duration)):
        valley_start_time = valleys[i].start_time
        if i == 0:  # Edge case
            delta_previous_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_previous_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_previous_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_previous_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
        else:
            delta = inspiration_duration[i].sample - inspiration_duration[i - 1].sample
            delta_previous_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = expiration_duration[i].sample - expiration_duration[i - 1].sample
            delta_previous_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = respiration_duration[i].sample - respiration_duration[i - 1].sample
            delta_previous_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = stretch[i].sample - stretch[i - 1].sample
            delta_previous_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

        if i == len(inspiration_duration) - 1:
            delta_next_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_next_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_next_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_next_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
        else:
            delta = inspiration_duration[i].sample - inspiration_duration[i + 1].sample
            delta_next_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = expiration_duration[i].sample - expiration_duration[i + 1].sample
            delta_next_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = respiration_duration[i].sample - respiration_duration[i + 1].sample
            delta_next_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = stretch[i].sample - stretch[i + 1].sample
            delta_next_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

        stretch_average = 0
        expiration_average = 0
        count = 0.0
        for j in [-2, -1, 1, 2]:
            if i + j < 0 or i + j >= len(inspiration_duration):
                continue
            stretch_average += stretch[i + j].sample
            expiration_average += expiration_duration[i + j].sample
            count += 1

        stretch_average /= count
        expiration_average /= count

        ratio = stretch[i].sample / stretch_average
        neighbor_ratio_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=ratio))

        ratio = expiration_duration[i].sample / expiration_average
        neighbor_ratio_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=ratio))

    # Begin assembling datastream for output
    inspiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    inspiration_duration_datastream.data = inspiration_duration

    expiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    expiration_duration_datastream.data = expiration_duration

    respiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    respiration_duration_datastream.data = respiration_duration

    inspiration_expiration_ratio_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    inspiration_expiration_ratio_datastream.data = inspiration_expiration_ratio

    stretch_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    stretch_datastream.data = stretch

    upper_stretch_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    upper_stretch_datastream.data = upper_stretch

    lower_stretch_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    lower_stretch_datastream.data = lower_stretch

    delta_previous_inspiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_previous_inspiration_duration_datastream.data = delta_previous_inspiration_duration

    delta_previous_expiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_previous_expiration_duration_datastream.data = delta_previous_expiration_duration

    delta_previous_respiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_previous_respiration_duration_datastream.data = delta_previous_respiration_duration

    delta_previous_stretch_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_previous_stretch_duration_datastream.data = delta_previous_stretch_duration

    delta_next_inspiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_next_inspiration_duration_datastream.data = delta_next_inspiration_duration

    delta_next_expiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_next_expiration_duration_datastream.data = delta_next_expiration_duration

    delta_next_respiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_next_respiration_duration_datastream.data = delta_next_respiration_duration

    delta_next_stretch_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_next_stretch_duration_datastream.data = delta_next_stretch_duration

    neighbor_ratio_expiration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    neighbor_ratio_expiration_datastream.data = neighbor_ratio_expiration_duration

    neighbor_ratio_stretch_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    neighbor_ratio_stretch_datastream.data = neighbor_ratio_stretch_duration

    return peaks_datastream, \
           valleys_datastream,\
           inspiration_duration_datastream, \
           expiration_duration_datastream, \
           respiration_duration_datastream, \
           inspiration_expiration_ratio_datastream, \
           stretch_datastream, \
           upper_stretch_datastream, \
           lower_stretch_datastream, \
           delta_previous_inspiration_duration_datastream, \
           delta_previous_expiration_duration_datastream, \
           delta_previous_respiration_duration_datastream, \
           delta_previous_stretch_duration_datastream, \
           delta_next_inspiration_duration_datastream, \
           delta_next_expiration_duration_datastream, \
           delta_next_respiration_duration_datastream, \
           delta_next_stretch_duration_datastream, \
           neighbor_ratio_expiration_datastream, \
           neighbor_ratio_stretch_datastream,


def calculateMinuteVentilation(valley: List[DataPoint],
                               peaks: List[DataPoint]):

    valley_time_stamps = np.array([dp.start_time.timestamp() for dp in valley])
    valley_samples = np.array([dp.sample for dp in valley])

    peak_time_stamps = np.array([dp.start_time.timestamp() for dp in peaks])
    peak_samples = np.array([dp.sample for dp in peaks])


    minuteVentilation = 0.0
    for i in range(0, len(valley_time_stamps)-1):
        minuteVentilation += (peak_time_stamps[i]-valley_time_stamps[i]) / 1000.0 * (peak_samples[i]-valley_samples[i]) / 2.0
    return minuteVentilation

def getBreathRate(datastream: DataStream,
                  window_size: float,
                  window_offset: float):

    """
    Computes breath rate

    :param datastream: DataStream
    :return: breathing rate per minute datastream
    """


    window_data = window_sliding(datastream.data, window_size, window_offset)

    breath_rates = []

    for key, value in window_data.items():
        starttime, endtime = key
        totalCycle = len(value)
        breath_rates.append(
            DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=totalCycle))

    datastream_breath_rates = DataStream.from_datastream([datastream])
    datastream_breath_rates.data = breath_rates
    return datastream_breath_rates

def getMinuteVentilation(peak_datastream: DataStream,
                  valley_datastream: DataStream,
                  window_size: float,
                  window_offset: float):

    """
    :param peak_datastream: DataStream
    :param valley_datastream: DataStream
    :return: respiration minute ventilation datastream
    """

    peak_window_data = window_sliding(peak_datastream.data, window_size, window_offset)
    valley_window_data = window_sliding(valley_datastream.data, window_size, window_offset)

    minuteVentilation = []

    for valley_key, valley_value in valley_window_data.items():
        starttime, endtime = valley_key
        for peak_key, peak_value in peak_window_data.items():
            if peak_key == valley_key:
                minuteVentilation.append(
                    DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=calculateMinuteVentilation(valley_value, peak_value)))

    datastream_minuteVentilation = DataStream.from_datastream([valley_datastream])
    datastream_minuteVentilation.data = minuteVentilation
    return datastream_minuteVentilation


def getBasicStatistics(datastream: DataStream,
                       window_size: float,
                       window_offset: float):

    """
    Computes mean, median, 80th percentile, and quartile deviation of datastream

    :param datastream: DataStream
    :return: mean, median, 80th percentile, and quartile deviation DataStreams of datastream
    """

    # perform windowing of datastream
    window_data = window_sliding(datastream.data, window_size, window_offset)

    datastream_mean_data = []
    datastream_median_data = []
    datastream_80percentile_data = []
    datastream_quartile_deviation_data = []

    for key, value in window_data.items():
        starttime, endtime = key
        reference_data = np.array([i.sample for i in value])

        datastream_mean_data.append(
            DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.mean(reference_data)))
        datastream_median_data.append(
            DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.median(reference_data)))
        datastream_quartile_deviation_data.append(DataPoint.from_tuple(start_time=starttime,
                                                                       end_time=endtime,
                                                                       sample=(0.5 * (
                                                                           np.percentile(reference_data, 75) - np.percentile(
                                                                               reference_data,
                                                                               25)))))
        datastream_80percentile_data.append(
            DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.percentile(reference_data, 80)))

    datastream_mean = DataStream.from_datastream([datastream])
    datastream_mean.data = datastream_mean_data
    datastream_median = DataStream.from_datastream([datastream])
    datastream_median.data = datastream_median_data
    datastream_quartile = DataStream.from_datastream([datastream])
    datastream_quartile.data = datastream_quartile_deviation_data
    datastream_80 = DataStream.from_datastream([datastream])
    datastream_80.data = datastream_80percentile_data

    return datastream_mean, datastream_median, datastream_quartile, datastream_80

def window_rip(peak_datastream: DataStream,
                valley_datastream: DataStream,
                inspiration_duration: DataStream,
                expiration_duration: DataStream,
                respiration_duration: DataStream,
                inspiration_expiration_ratio: DataStream,
                stretch: DataStream,
                window_size: float,
                window_offset: float) -> Tuple[DataStream]:

    """
    Respiration windowed Feature Implementation.
    :param inspiration_duration: DataStream
    :param expiration_duration: DataStream
    :param respiration_duration: DataStream
    :param inspiration_expiration_ratio: DataStream
    :param stretch: DataStream
    :param window_size: float
    :param window_offset: float
    :return: RIP windowed Feature DataStreams
    """

    if len(inspiration_duration.data) == 0:
        return None

    datastream_breath_rates = getBreathRate(inspiration_duration, window_size, window_offset)

    datastream_minuteVentilation = getMinuteVentilation(peak_datastream, valley_datastream, window_size, window_offset)

    inspiration_duration_mean, inspiration_duration_median, inspiration_duration_quartile_deviation, inspiration_duration_80percentile = getBasicStatistics(inspiration_duration, window_size, window_offset)

    expiration_duration_mean, expiration_duration_median, expiration_duration_quartile_deviation, expiration_duration_80percentile = getBasicStatistics(expiration_duration, window_size, window_offset)

    respiration_duration_mean, respiration_duration_median, respiration_duration_quartile_deviation, respiration_duration_80percentile = getBasicStatistics(respiration_duration, window_size, window_offset)

    inspiration_expiration_ratio_mean, inspiration_expiration_ratio_median, inspiration_expiration_ratio_quartile_deviation, inspiration_expiration_ratio_80percentile = getBasicStatistics(inspiration_expiration_ratio, window_size, window_offset)

    stretch_mean, stretch_median, stretch_quartile_deviation, stretch_80percentile = getBasicStatistics(stretch, window_size, window_offset)

    return datastream_breath_rates, datastream_minuteVentilation, \
           inspiration_duration_mean, inspiration_duration_median, inspiration_duration_quartile_deviation, inspiration_duration_80percentile, \
           expiration_duration_mean, expiration_duration_median, expiration_duration_quartile_deviation, expiration_duration_80percentile, \
           respiration_duration_mean, respiration_duration_median, respiration_duration_quartile_deviation, respiration_duration_80percentile, \
           inspiration_expiration_ratio_mean, inspiration_expiration_ratio_median, inspiration_expiration_ratio_quartile_deviation, inspiration_expiration_ratio_80percentile, \
           stretch_mean, stretch_median, stretch_quartile_deviation, stretch_80percentile

