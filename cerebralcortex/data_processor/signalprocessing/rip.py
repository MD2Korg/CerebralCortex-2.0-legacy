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

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
import numpy as np

def compute_peak_valley(rip: DataStream,
                    fs: float = 21.33,
                    smoothing_factor: int = 5,
                    time_window: int = 8,
                    expiration_amplitude_threshold_perc: float = 0.10,
                    threshold_expiration_duration: float = 0.312,
                    inspiration_amplitude_threshold_perc: float = 0.10,
                    max_amplitude_change_peak_correction: float = 30,
                    min_neg_slope_count_peak_correction: int = 4) -> [DataStream, DataStream]:

    """
    Compute peak and valley from rip data and filter peak and valley.

    :return peak_datastream, valley_datastream:
    :param rip:
    :param fs:
    :param time_window:
    :param expiration_amplitude_threshold_perc:
    :param threshold_expiration_duration:
    :param max_amplitude_change_peak_correction:
    :param min_neg_slope_count_peak_correction:
    """
    data = rip.datapoints
    sample = np.array([i.sample for i in data])
    timestamp = np.array([i.start_time for i in data])

    sample = smooth(sample, smoothing_factor) # Moving average filter with default smoothing factor = 5.
    window_length = round(time_window * fs)
    mac = get_moving_average_curve(sample, window_length=window_length)

    up_intercept, up_intercept_indices, down_intercept, down_intercept_indices = get_intercepts(sample=sample, mac=mac, window_length=window_length)
    up_intercept_indices, down_intercept_indices = filter_intercept_outlier(down_intercept_indices=down_intercept_indices, up_intercept_indices=up_intercept_indices, window_length=window_length)

    peaks, peak_indices, valleys, valley_indices = generate_peak_valley(up_intercept_indices=up_intercept_indices, down_intercept_indices=down_intercept_indices, sample=sample)

    valley_indices = correct_valley_position(peak_indices=peak_indices, valley_indices=valley_indices, up_intercept_indices=up_intercept_indices, sample=sample)
    peak_indices = correct_peak_position(peak_indices=peak_indices, valley_indices=valley_indices, up_intercept_indices=up_intercept_indices, sample=sample, max_amplitude_change_peak_correction=max_amplitude_change_peak_correction, min_neg_slope_count_peak_correction=min_neg_slope_count_peak_correction)

    # remove too close valley peak pair.
    peak_indices, valley_indices = remove_close_valley_peak_pair(peak_indices=peak_indices, valley_indices=valley_indices)

    # Remove small  Expiration duration < 0.31
    peak_indices, valley_indices = filter_expiration_duration_outlier(peak_indices=peak_indices, valley_indices=valley_indices, timestamp=timestamp, threshold_expiration_duration=threshold_expiration_duration)

    # filter out peak valley pair of inspiration of small amplitude.
    peak_indices, valley_indices = filter_small_amplitude_inspiration_peak_valley_pair(peak_indices=peak_indices, valley_indices=valley_indices, sample=sample, inspiration_amplitude_threshold_perc=inspiration_amplitude_threshold_perc)

    # filter out peak valley pair of expiration of small amplitude.
    peak_indices, valley_indices = filter_small_amplitude_expiration_peak_valley_pair(peak_indices=peak_indices, valley_indices=valley_indices, sample=sample, expiration_amplitude_threshold_perc=expiration_amplitude_threshold_perc)

    peak_datapoints, valley_datapoints = [], []

    for i in peak_indices:
        peak_datapoints.append(DataPoint.from_tuple(sample[i], timestamp[i]))

    for i in valley_indices:
        valley_datapoints.append(DataPoint.from_tuple(sample[i], timestamp[i]))

    peak_datastream = DataStream.from_datastream([rip])
    peak_datastream.datapoints = peak_datapoints
    valley_datastream = DataStream.from_datastream([rip])
    valley_datastream.datapoints = valley_datapoints

    # TODO: graph plot
    # import matplotlib.pyplot as plt
    # plt.plot(sample, 'b')
    # plt.plot(range(window_length + 1, len(sample) - (window_length + 1)), mac, 'r')
    # plt.plot(up_intercept_indices, [sample[i] for i in up_intercept_indices], 'r^', linewidth=5)
    # plt.plot(down_intercept_indices, [sample[i] for i in down_intercept_indices], 'rv', linewidth=5)
    # plt.plot(peak_indices, [sample[i] for i in peak_indices], 'o')
    # plt.plot(valley_indices, [sample[i] for i in valley_indices], 's')
    # plt.show()

    return peak_datastream, valley_datastream

def filter_small_amplitude_expiration_peak_valley_pair(peak_indices: np.ndarray,
                                                       valley_indices: np.ndarray,
                                                       sample: np.ndarray,
                                                       expiration_amplitude_threshold_perc: float = 0.10) -> [np.ndarray, np.ndarray]:
    """
    Filter out peak valley pair if their expiration amplitude is less than or equal to 10% of average expiration amplitude.

    :return peak_indices_updated, valley_indices_updated:
    :param peak_indices:
    :param valley_indices:
    :param sample:
    :param expiration_amplitude_threshold_perc:
    """
    peak_indices_updated, valley_indices_updated = [], [valley_indices[0]]

    expiration_amplitudes = []
    for i in range(len(peak_indices) - 1):
        expiration_amplitudes.append(abs(sample[valley_indices[i + 1]] - sample[peak_indices[i]]))

    mean_expiration_amplitude = np.mean(expiration_amplitudes)

    for i in range(len(expiration_amplitudes)):
        if expiration_amplitudes[i] > expiration_amplitude_threshold_perc * mean_expiration_amplitude: #TODO: remove hard coded .10 (10%) of mean expiration amplitude.
            valley_indices_updated.append(valley_indices[i + 1])
            peak_indices_updated.append(peak_indices[i])


    return peak_indices_updated, valley_indices_updated

def filter_small_amplitude_inspiration_peak_valley_pair(peak_indices: np.ndarray,
                                                        valley_indices: np.ndarray,
                                                        sample: np.ndarray,
                                                        inspiration_amplitude_threshold_perc:float=0.10) -> [np.ndarray, np.ndarray]:
    """
    Filter out peak valley pair if their inspiration amplitude is less than or to equal 10% of average inspiration amplitude.

    :return peak_indices_updated, valley_indices_updated:
    :param peak_indices:
    :param valley_indices:
    :param sample:
    :param inspiration_amplitude_threshold_perc:
    """
    peak_indices_updated, valley_indices_updated = [], []

    inspiration_amplitudes = [(sample[peak_indices[i]] - sample[valley_indices[i]]) for i in range(len(valley_indices))]
    mean_inspiration_amplitude = np.mean(inspiration_amplitudes)

    for i in range(len(inspiration_amplitudes)):
        if inspiration_amplitudes[i] > inspiration_amplitude_threshold_perc * mean_inspiration_amplitude:
            valley_indices_updated.append(valley_indices[i])
            peak_indices_updated.append(peak_indices[i])


    return peak_indices_updated, valley_indices_updated

def filter_expiration_duration_outlier(peak_indices: np.ndarray,
                                       valley_indices: np.ndarray,
                                       timestamp: np.ndarray,
                                       threshold_expiration_duration: float = 0.312) -> [np.ndarray, np.ndarray]:
    """
    Filter out peak valley pair for which expiration duration is too small.

    :return peak_indices_updated, valley_indices_updated:
    :param peak_indices:
    :param valley_indices:
    :param timestamp:
    :param threshold_expiration_duration:
    """
    expiration_durations = []
    for i in range(len(peak_indices) - 1):
        expiration_duration = (timestamp[valley_indices[i + 1]] - timestamp[peak_indices[i]])
        expiration_durations.append(expiration_duration)

    peak_indices_updated, valley_indices_updated = [], [valley_indices[0]]

    for i in range(len(expiration_durations)):
        if expiration_durations[i].total_seconds() > threshold_expiration_duration: # peak valley pair is assumed valid if expiration duration > 0.312 seconds.
            peak_indices_updated.append(peak_indices[i])
            valley_indices_updated.append(valley_indices[i + 1])

    peak_indices_updated.append(peak_indices[-1])

    return peak_indices_updated, valley_indices_updated


def remove_close_valley_peak_pair(peak_indices: np.ndarray,
                                  valley_indices: np.ndarray) -> [np.ndarray, np.ndarray]:
    """
    Filter out too close valley peak pair.

    :return peak_indices_updated, valley_indices_updated:
    :param peak_indices
    :param valley_indices
    """
    peak_to_valley_index_list = [(peak_indices[i] - valley_indices[i]) for i in range(len(peak_indices))]
    valid_peak_to_valley_index_list = [ i for i in range(len(peak_to_valley_index_list)) if peak_to_valley_index_list[i] >= 6] # minimum peak to valley distance assume valid if > 0.3 sec. floor(64/3*0.31 sec) = 6 points.

    peak_indices_updated = [peak_indices[i] for i in range(len(valid_peak_to_valley_index_list))]
    valley_indices_updated = [valley_indices[i] for i in range(len(valid_peak_to_valley_index_list))]

    return peak_indices_updated, valley_indices_updated

# push peak towards left
def correct_peak_position(peak_indices: np.ndarray,
                          valley_indices: np.ndarray,
                          up_intercept_indices: np.ndarray,
                          sample: np.ndarray,
                          max_amplitude_change_peak_correction: float = 30,
                          min_neg_slope_count_peak_correction: int = 4) -> np.ndarray:
    """
    Correct peak position by checking if there is a notch in the inspiration branch at left position.
    If at least 60% inspiration is done at a notch point, assume that notch as an original peak.
    Our hypothesis is most of breathing in done for that cycle. We assume insignificant amount of breath is taken or some new cycle started after the notch.

    :return peak_indices:
    :param peak_indices:
    :param valley_indices:
    :param up_intercept_indices:
    :param sample:
    :param max_amplitude_change_peak_correction:
    :param min_neg_slope_count_peak_correction:

    """

    for i in range(len(peak_indices)):
        up_indices_valley_to_peak = [j for j in range(len(up_intercept_indices)) if (valley_indices[i] < up_intercept_indices[j]) and (up_intercept_indices[j] < peak_indices[i])]
        if len(up_indices_valley_to_peak) > 0:
            temp_peak = sample[up_intercept_indices[up_indices_valley_to_peak[0]]:peak_indices[i]] #points between current valley and UI.
            slope_peak = np.diff(temp_peak)

            if all(j >= 0 for j in slope_peak) == False:
                indices_neg_slope = [j for j in range(len(slope_peak)) if slope_peak[j] < 0]
                peaks_new = up_intercept_indices[up_indices_valley_to_peak[0]] + indices_neg_slope[0] - 1
                valley_peak_dist_new = sample[peaks_new] - sample[valley_indices[i]]
                valley_peak_dist_prev = sample[peak_indices[i]] - sample[valley_indices[i]]
                amplitude_change = (valley_peak_dist_prev - valley_peak_dist_new) / valley_peak_dist_new * 100 #TODO: throw divide by zero encounter. Handle this situation.

                if len(indices_neg_slope) >= min_neg_slope_count_peak_correction and amplitude_change <= max_amplitude_change_peak_correction:
                    peak_indices[i] = up_intercept_indices[up_indices_valley_to_peak[0]] + indices_neg_slope[0] - 1 # 60% inspiration is done at that point.

    return peak_indices

# push valley towards right
def correct_valley_position(peak_indices: np.ndarray,
                            valley_indices: np.ndarray,
                            up_intercept_indices: np.ndarray,
                            sample: np.ndarray) -> np.ndarray:
    """
    Correct Valley position by locating actual valley using maximum slope algorithm which is located between current valley and following peak.

    Algorithm - push valley towards right:
    Search for points lies in between current valley and following Up intercept.
    Calculate slopes at those points.
    Ensure that valley resides at the begining of inhalation cycle where inhalation slope is maximum.

    :return valley_indices_updated:
    :param peak_indices:
    :param valley_indices:
    :param up_intercept_indices:
    :param sample:
    """
    valley_indices_updated = valley_indices.copy()

    for i in range(len(valley_indices)):
        up_indices_valley_to_peak = [j for j in range(len(up_intercept_indices)) if (valley_indices[i] < up_intercept_indices[j]) and (up_intercept_indices[j] < peak_indices[i])]

        if len(up_indices_valley_to_peak) > 0:
            if len(up_indices_valley_to_peak) > 1:
                print('WARNING::: More than 2 consectuve up intercept is detected.')
                continue
            else:
                up_index_valley_to_peak = up_indices_valley_to_peak[0]
                sample_valley_to_UI = sample[valley_indices[i]: up_intercept_indices[up_index_valley_to_peak] + 1]  # all points between current valley and UI
                slope_at_samples = np.diff(sample_valley_to_UI)

                consecutive_positive_slopes = [-1] * len(slope_at_samples)
                for j in range(len(slope_at_samples)):
                    slopes_subset = slope_at_samples[j:]
                    if all(slope > 0 for slope in slopes_subset):
                        consecutive_positive_slopes[j] = len(slopes_subset)

                if any(no_con_slope > 0 for no_con_slope in consecutive_positive_slopes):
                    indices_max_pos_slope = [k for k in range(len(consecutive_positive_slopes)) if
                                          consecutive_positive_slopes[k] == max(consecutive_positive_slopes)] # indices where no of consecutive positive slope is the maximum.
                    valley_indices_updated[i] = valley_indices[i] + indices_max_pos_slope[-1] - 1 # Update the valley indices.

    return valley_indices_updated

def generate_peak_valley(up_intercept_indices: np.ndarray,
                         down_intercept_indices: np.ndarray,
                         sample: np.ndarray) -> [np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute peak valley from up intercepts and down intercepts indices.

    :return peaks, peak_indices, valleys, valley_indices:
    :param up_intercept_indices:
    :param down_intercept_indices:
    :param sample:
    """
    peaks, valleys, peak_indices, valley_indices = [], [], [], []
    for i in range(len(down_intercept_indices) - 1):  # As UI>DI (We trim the UI data such that UI[0] < DI[0]), Down Intercept comes first.
        values = sample[up_intercept_indices[i]: down_intercept_indices[i + 1]]
        peak = max(values)
        peaks.append(peak)
        peakIndex = up_intercept_indices[i] + list(values).index(peak)
        peak_indices.append(peakIndex)

        values = sample[down_intercept_indices[i]: up_intercept_indices[i]]
        valley = min(values)
        valleys.append(valley)
        valleyIndex = down_intercept_indices[i] + list(values).index(valley)
        valley_indices.append(valleyIndex)

    return peaks, peak_indices, valleys, valley_indices


def filter_intercept_outlier(up_intercept_indices: np.ndarray,
                             down_intercept_indices: np.ndarray,
                             window_length: int):
    """
    Remove two or more consecutive up or down intercepts.

    :return uii, dii:
    :param up_intercept_indices:
    :param down_intercept_indices:
    :param window_length:
    """

    dictInterceptIndices = {}
    upSymbol = 'u'
    downSymbol = 'd'

    for i in up_intercept_indices:
        dictInterceptIndices[i] = upSymbol

    for i in down_intercept_indices:
        dictInterceptIndices[i] = downSymbol

    keys = list(dictInterceptIndices.keys()).copy()
    keys.sort()

    uii, dii = [], []

    i = 0
    while i < len(keys):
        while i + 1 < len(keys) and dictInterceptIndices[keys[i]] == dictInterceptIndices[keys[i + 1]]:
            i += 1

        if dictInterceptIndices[keys[i]] == upSymbol:
            uii.append(keys[i])
        elif dictInterceptIndices[keys[i]] == downSymbol:
            dii.append(keys[i])

        i += 1

    i = 0
    while uii[i] < dii[0]: # Targetted to start from down intercept. That's why trim up intercepts out where up intercept is less than first down intercept.
        i += 1

    uii = uii[i:]

    minLength = min(len(uii), len(dii))
    uii, dii = uii[:minLength], dii[:minLength]

    return uii, dii


def get_intercepts(sample: np.ndarray,
                   mac: np.ndarray,
                   window_length: int) -> [np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Returns Up and Down Intercepts.
    Moving Average Centerline curve intersects breath cycle twice. Once in the inhalation branch (Up intercept) and in the exhalation branch (Down intercept).

    :return up_intercept, up_intercept_indices, down_intercept, down_intercept_indices:
    :param sample:
    :param mac:
    :param window_length:
    """
    up_intercept, up_intercept_indices, down_intercept, down_intercept_indices = [], [], [], []

    for i in range(1, len(mac)):
        if (sample[i + window_length - 1] <= mac[i - 1] and sample[i + window_length] >= mac[i]):
            up_intercept.append(mac[i])
            up_intercept_indices.append(i + window_length - 1)
        elif (sample[i + window_length - 1] >= mac[i - 1] and sample[i + window_length] <= mac[i]):
            down_intercept.append(mac[i])
            down_intercept_indices.append(i + window_length - 1)

    return up_intercept, up_intercept_indices, down_intercept, down_intercept_indices

def get_moving_average_curve(sample: np.ndarray,
                             window_length: int) -> np.ndarray:

    """
    Moving average curve from filtered (using moving average) samples.

    :return: mac
    :param sample:
    :param window_length:
    """
    mac = []
    for i in range(window_length+1, len(sample) - (window_length + 1)): # average over [-window_length, window_length]  = 2 * window_length.
        averageValue = sample[i - window_length:i + window_length].mean()
        mac.append(averageValue)

    return mac

def smooth(data: np.ndarray,
           span: int = 5) -> np.ndarray:
    """
    Smooths data using moving average filter over a span.
    The first few elements of data_smooth are given by
    data_smooth(1) = data(1)
    data_smooth(2) = (data(1) + data(2) + data(3))/3
    data_smooth(3) = (data(1) + data(2) + data(3) + data(4) + data(5))/5
    data_smooth(4) = (data(2) + data(3) + data(4) + data(5) + data(6))/5

    for more details follow the below links:
    https://www.mathworks.com/help/curvefit/smooth.html
    http://stackoverflow.com/a/40443565

    :return: data_smooth
    :param data:
    :param span:
    """
    data_middle = np.convolve(data, np.ones(span, dtype=int), 'valid') / span
    divisor = np.arange(1, span - 1, 2)
    data_start = np.cumsum(data[:span - 1])[::2] / divisor
    data_end = (np.cumsum(data[:-span:-1])[::2] / divisor)[::-1]
    data_smooth = np.concatenate((data_start, data_middle, data_end))

    return data_smooth


