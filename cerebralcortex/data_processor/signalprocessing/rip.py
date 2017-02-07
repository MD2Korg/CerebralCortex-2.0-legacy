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

from cerebralcortex.data_processor.signalprocessing.vector import smooth, moving_average_curve
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def compute_peak_valley(rip: DataStream,
                        fs: float = 21.33,
                        smoothing_factor: int = 5,
                        time_window: int = 8,
                        expiration_amplitude_threshold_perc: float = 0.10,
                        threshold_expiration_duration: float = 0.312,
                        inspiration_amplitude_threshold_perc: float = 0.10,
                        max_amplitude_change_peak_correction: float = 30,
                        min_neg_slope_count_peak_correction: int = 4,
                        minimum_peak_to_valley_time_diff=0.31) -> [DataStream, DataStream]:
    """
    Compute peak and valley from rip data and filter peak and valley.

    :param minimum_peak_to_valley_time_diff:
    :param inspiration_amplitude_threshold_perc:
    :param smoothing_factor:
    :return peak_datastream, valley_datastream:
    :param rip:
    :param fs:
    :param time_window:
    :param expiration_amplitude_threshold_perc:
    :param threshold_expiration_duration:
    :param max_amplitude_change_peak_correction:
    :param min_neg_slope_count_peak_correction:
    """

    data_smooth = smooth(data=rip.datapoints, span=smoothing_factor)
    window_length = int(round(time_window * fs))
    data_mac = moving_average_curve(data_smooth, window_length=window_length)

    up_intercepts, down_intercepts = up_down_intercepts(data=data_smooth, mac=data_mac)
    up_intercepts, down_intercepts = filter_intercept_outlier(up_intercepts=up_intercepts,
                                                              down_intercepts=down_intercepts)

    peaks, valleys = generate_peak_valley(up_intercepts=up_intercepts,
                                          down_intercepts=down_intercepts,
                                          data=data_smooth)

    valleys = correct_valley_position(peaks=peaks, valleys=valleys, up_intercepts=up_intercepts, data=data_smooth)

    peaks = correct_peak_position(peaks=peaks,
                                  valleys=valleys,
                                  up_intercepts=up_intercepts,
                                  data=data_smooth,
                                  max_amplitude_change_peak_correction=max_amplitude_change_peak_correction,
                                  min_neg_slope_count_peak_correction=min_neg_slope_count_peak_correction)

    # remove too close valley peak pair.
    peaks, valleys = remove_close_valley_peak_pair(peaks=peaks,
                                                   valleys=valleys,
                                                   minimum_peak_to_valley_time_diff=minimum_peak_to_valley_time_diff)

    # Remove small  Expiration duration < 0.31
    peaks, valleys = filter_expiration_duration_outlier(peaks=peaks,
                                                        valleys=valleys,
                                                        threshold_expiration_duration=threshold_expiration_duration)

    # filter out peak valley pair of inspiration of small amplitude.
    peaks, valleys = filter_small_amp_inspiration_peak_valley(peaks=peaks,
                                                              valleys=valleys,
                                                              inspiration_amplitude_threshold=inspiration_amplitude_threshold_perc)

    # filter out peak valley pair of expiration of small amplitude.
    peaks, valleys = filter_small_amp_expiration_peak_valley(peaks=peaks,
                                                             valleys=valleys,
                                                             expiration_amplitude_threshold=expiration_amplitude_threshold_perc)

    peak_datastream = DataStream.from_datastream([rip])
    peak_datastream.datapoints = peaks
    valley_datastream = DataStream.from_datastream([rip])
    valley_datastream.datapoints = valleys

    return peak_datastream, valley_datastream


def filter_small_amp_expiration_peak_valley(peaks: List[DataPoint],
                                            valleys: List[DataPoint],
                                            expiration_amplitude_threshold: float) -> [List[DataPoint],
                                                                                       List[DataPoint]]:
    """
    Filter out peak valley pair if their expiration amplitude is less than or equal to 10% of
    average expiration amplitude.

    :return: peaks_updated, valleys_updated:
    :param: peaks:
    :param: valleys:
    :param: expiration_amplitude_threshold_perc:
    """

    expiration_amplitudes = []
    peaks_updated = []
    valleys_updated = [valleys[0]]

    for i, peak in enumerate(peaks):
        if i < len(peaks) - 1:
            expiration_amplitudes.append(abs(valleys[i + 1].sample - peak.sample))

    mean_expiration_amplitude = np.mean(expiration_amplitudes)

    for i, expiration_amplitude in enumerate(expiration_amplitudes):
        if expiration_amplitude > expiration_amplitude_threshold * mean_expiration_amplitude:
            peaks_updated.append(peaks[i])
            valleys_updated.append(valleys[i + 1])

    peaks_updated.append(peaks[-1])

    return peaks_updated, valleys_updated


def filter_small_amp_inspiration_peak_valley(peaks: List[DataPoint],
                                             valleys: List[DataPoint],
                                             inspiration_amplitude_threshold: float) -> [List[DataPoint],
                                                                                         List[DataPoint]]:
    """
    Filter out peak valley pair if their inspiration amplitude is less than or to equal 10% of
    average inspiration amplitude.

    :return peaks_updated, valleys_updated:
    :param peaks:
    :param valleys:
    :param inspiration_amplitude_threshold:
    """

    peaks_updated = []
    valleys_updated = []

    inspiration_amplitudes = [(peaks[i].sample - valleys[i].sample) for i, valley in enumerate(valleys)]
    mean_inspiration_amplitude = np.mean(inspiration_amplitudes)

    for i, inspiration_amplitude in enumerate(inspiration_amplitudes):
        if inspiration_amplitude > inspiration_amplitude_threshold * mean_inspiration_amplitude:
            valleys_updated.append(valleys[i])
            peaks_updated.append(peaks[i])

    return peaks_updated, valleys_updated


def filter_expiration_duration_outlier(peaks: List[DataPoint],
                                       valleys: List[DataPoint],
                                       threshold_expiration_duration: float) -> [List[DataPoint], List[DataPoint]]:
    """
    Filter out peak valley pair for which expiration duration is too small.

    :return peaks_updated, valleys_updated:
    :param peaks:
    :param valleys:
    :param threshold_expiration_duration:
    """

    peaks_updated = []
    valleys_updated = [valleys[0]]

    for i, item in enumerate(peaks):
        if i < len(peaks) - 1:
            expiration_duration = valleys[i + 1].start_time - peaks[i].start_time
            if expiration_duration.total_seconds() > threshold_expiration_duration:
                peaks_updated.append(peaks[i])
                valleys_updated.append(valleys[i + 1])

    peaks_updated.append(peaks[-1])

    return peaks_updated, valleys_updated


def remove_close_valley_peak_pair(peaks: List[DataPoint],
                                  valleys: List[DataPoint],
                                  minimum_peak_to_valley_time_diff: float = 0.31) -> [List[DataPoint], List[DataPoint]]:
    """
    Filter out too close valley peak pair.

    :return peaks_updated, valleys_updated:
    :param peaks:
    :param valleys:
    :param minimum_peak_to_valley_time_diff:
    """

    peaks_updated = []
    valleys_updated = []

    for i, item in enumerate(peaks):
        time_diff_valley_peak = peaks[i].start_time - valleys[i].start_time
        if time_diff_valley_peak.total_seconds() > minimum_peak_to_valley_time_diff:
            peaks_updated.append(peaks[i])
            valleys_updated.append(valleys[i])

    return peaks_updated, valleys_updated


def correct_peak_position(peaks: List[DataPoint],
                          valleys: List[DataPoint],
                          up_intercepts: List[DataPoint],
                          data: List[DataPoint],
                          max_amplitude_change_peak_correction: float,
                          min_neg_slope_count_peak_correction: int) -> List[DataPoint]:
    """
    Correct peak position by checking if there is a notch in the inspiration branch at left position.
    If at least 60% inspiration is done at a notch point, assume that notch as an original peak.
    Our hypothesis is most of breathing in done for that cycle. We assume insignificant amount of
    breath is taken or some new cycle started after the notch.

    :return peaks:
    :param peaks:
    :param valleys:
    :param up_intercepts:
    :param data:
    :param max_amplitude_change_peak_correction:
    :param min_neg_slope_count_peak_correction:

    """

    for i, item in enumerate(peaks):
        up_intercepts_valley_to_peak = []
        for j in range(len(up_intercepts)):
            if valleys[i].start_time < up_intercepts[j].start_time < peaks[i].start_time:
                up_intercepts_valley_to_peak.append(up_intercepts[j])

        if len(up_intercepts_valley_to_peak) > 1:
            raise Exception("More than one consecutive up intercepts.")

        elif len(up_intercepts_valley_to_peak) == 1:
            up_intercept = up_intercepts_valley_to_peak[0]

            # points between current valley and UI.
            data_ui_to_peak = [j for j in data if up_intercept.start_time <= j.start_time <= peaks[i].start_time]

            sample_ui_to_peak = [j.sample for j in data_ui_to_peak]
            slope_at_samples = np.diff(sample_ui_to_peak)

            if not all(j >= 0 for j in slope_at_samples):
                indices_neg_slope = [j for j in range(len(slope_at_samples)) if slope_at_samples[j] < 0]
                peak_new = data_ui_to_peak[indices_neg_slope[0] - 1]
                valley_peak_dist_new = peak_new.sample - valleys[i].sample
                valley_peak_dist_prev = peaks[i].sample - valleys[i].sample
                if valley_peak_dist_new == 0:
                    raise Exception("New peak to valley distance is equal to zero. "
                                    "This will encounter divide by zero exception.")
                else:
                    amplitude_change = (valley_peak_dist_prev - valley_peak_dist_new) / valley_peak_dist_new * 100

                    if len(indices_neg_slope) >= min_neg_slope_count_peak_correction:
                        if amplitude_change <= max_amplitude_change_peak_correction:
                            peaks[i] = peak_new  # 60% inspiration is done at that point.

    return peaks


def correct_valley_position(peaks: List[DataPoint],
                            valleys: List[DataPoint],
                            up_intercepts: List[DataPoint],
                            data: List[DataPoint]) -> List[DataPoint]:
    """
    Correct Valley position by locating actual valley using maximum slope algorithm which is
    located between current valley and following peak.

    Algorithm - push valley towards right:
    Search for points lies in between current valley and following Up intercept.
    Calculate slopes at those points.
    Ensure that valley resides at the begining of inhalation cycle where inhalation slope is maximum.

    :return valley_updated:
    :param peaks:
    :param valleys:
    :param up_intercepts:
    :param data:
    """
    valley_updated = valleys.copy()

    for i in range(len(valleys)):
        up_intercepts_valley_to_peak = []
        for j in range(len(up_intercepts)):
            if valleys[i].start_time < up_intercepts[j].start_time < peaks[i].start_time:
                up_intercepts_valley_to_peak.append(up_intercepts[j])

        if len(up_intercepts_valley_to_peak) > 0:
            if len(up_intercepts_valley_to_peak) > 1:
                raise Exception('More than 2 consecutive up intercept is detected.')
            else:
                up_intercept = up_intercepts_valley_to_peak[0]

                data_valley_to_ui = []
                for j in data:
                    if valleys[i].start_time <= j.start_time <= up_intercept.start_time:
                        data_valley_to_ui.append(j)
                sample_valley_to_ui = [j.sample for j in data_valley_to_ui]

                slope_at_samples = np.diff(sample_valley_to_ui)

                consecutive_positive_slopes = [-1] * len(slope_at_samples)
                for j in range(len(slope_at_samples)):
                    slopes_subset = slope_at_samples[j:]
                    if all(slope > 0 for slope in slopes_subset):
                        consecutive_positive_slopes[j] = len(slopes_subset)

                if any(no_con_slope > 0 for no_con_slope in consecutive_positive_slopes):
                    indices_max_pos_slope = []
                    for k in range(len(consecutive_positive_slopes)):
                        if consecutive_positive_slopes[k] == max(consecutive_positive_slopes):
                            indices_max_pos_slope.append(k)
                    valley_updated[i] = data_valley_to_ui[indices_max_pos_slope[-1]]

    return valley_updated


def generate_peak_valley(up_intercepts: List[DataPoint],
                         down_intercepts: List[DataPoint],
                         data: List[DataPoint]) -> [List[DataPoint], List[DataPoint]]:
    """
    Compute peak valley from up intercepts and down intercepts indices.

    :return peaks, valleys:
    :param up_intercepts:
    :param down_intercepts:
    :param data:
    """
    peaks = []
    valleys = []

    last_iterated_index = 0
    for i in range(len(down_intercepts) - 1):
        peak = None
        valley = None

        for j in range(last_iterated_index, len(data)):
            if down_intercepts[i].start_time <= data[j].start_time <= up_intercepts[i].start_time:
                if valley is None or data[j].sample < valley.sample:
                    valley = data[j]
            elif up_intercepts[i].start_time <= data[j].start_time <= down_intercepts[i + 1].start_time:
                if peak is None or data[j].sample > peak.sample:
                    peak = data[j]
            elif data[j].start_time > down_intercepts[i + 1].start_time:
                last_iterated_index = j
                break

        valleys.append(valley)
        peaks.append(peak)

    return peaks, valleys


def filter_intercept_outlier(up_intercepts: List[DataPoint],
                             down_intercepts: List[DataPoint]) -> [List[DataPoint], List[DataPoint]]:
    """
    Remove two or more consecutive up or down intercepts.

    :return up_intercepts_updated, down_intercepts_updated:
    :param up_intercepts:
    :param down_intercepts:
    """

    up_intercepts_updated = []
    down_intercepts_updated = []

    for index in range(len(down_intercepts) - 1):
        up_intercepts_between_down_intercepts = []
        for ui in up_intercepts:
            if down_intercepts[index].start_time <= ui.start_time <= down_intercepts[index + 1].start_time:
                up_intercepts_between_down_intercepts.append(ui)

        if len(up_intercepts_between_down_intercepts) > 0:
            up_intercepts_updated.append(up_intercepts_between_down_intercepts[-1])

    for index in range(len(up_intercepts_updated) - 1):
        down_intercepts_between_up_intercepts = []
        for di in down_intercepts:
            if up_intercepts_updated[index].start_time <= di.start_time <= up_intercepts_updated[index + 1].start_time:
                down_intercepts_between_up_intercepts.append(di)

        if len(down_intercepts_between_up_intercepts) > 0:
            down_intercepts_updated.append(down_intercepts_between_up_intercepts[-1])

    up_intercepts_updated = []
    for ui in up_intercepts_updated:
        if ui.start_time >= down_intercepts_updated[0].start_time:
            up_intercepts_updated.append(ui)

    min_length = min(len(up_intercepts_updated), len(down_intercepts_updated))

    up_intercepts_updated = up_intercepts_updated[:min_length]
    down_intercepts_updated = down_intercepts_updated[:min_length]

    return up_intercepts_updated, down_intercepts_updated


def up_down_intercepts(data: List[DataPoint],
                       mac: List[DataPoint]) -> [List[DataPoint], List[DataPoint]]:
    """
    Returns Up and Down Intercepts.
    Moving Average Centerline curve intersects breath cycle twice. Once in the inhalation branch
    (Up intercept) and in the exhalation branch (Down intercept).

    :return up_intercepts, down_intercepts:
    :param data:
    :param mac:
    """

    up_intercepts = []
    down_intercepts = []

    subsets = [data[i - 1] for i in range(len(data)) if (mac[0].start_time <= data[i].start_time <= mac[-1].start_time)]

    for i in range(len(subsets) - 1):
        if subsets[i].sample <= mac[i].sample <= subsets[i + 1].sample:
            up_intercepts.append(mac[i])
        elif subsets[i].sample >= mac[i].sample >= subsets[i + 1].sample:
            down_intercepts.append(mac[i])

    return up_intercepts, down_intercepts
