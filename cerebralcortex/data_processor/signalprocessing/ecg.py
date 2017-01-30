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
from scipy import signal

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.window import window


# TODO: CODE_REVIEW: Justify in the method documentation string the justification of the default values
def classify_ecg_window(data: List[DataPoint],
                        range_threshold: int = 200,
                        slope_threshold: int = 50,
                        maximum_value: int = 4000) -> bool:
    """

    :param data: window of raw ecg tuples
    :param range_threshold: range of the window
    :param slope_threshold: median slope of the window
    :param maximum_value: maximum value
    :return: True/False decision based on the three parameters
    """

    values = np.array([i.sample for i in data])
    if max(values) - min(values) < range_threshold:
        return False
    if max(values) > maximum_value:
        return False
    if np.median(np.abs(np.diff(values))) > slope_threshold:
        return False
    return True


# TODO: CODE_REVIEW: Justify in the method documentation string the justification of the default values
def filter_bad_ecg(ecg: DataStream,
                   fs: float,
                   no_of_secs: int = 2) -> DataStream:
    """
    This function splits the ecg array into non overlapping windows of specified seconds
    and assigns a binary decision on them returns a filtered ecg array which contains
    only the windows to be kept

    :param ecg: raw ecg array
    :param fs: sampling frequency
    :param no_of_secs : no of seconds each window would be long
    :return:  filtered ecg array
    """

    window_length = int(no_of_secs * fs)
    window_data = window(ecg.datapoints, window_size=window_length)

    ecg_filtered = DataStream.from_datastream([ecg])
    ecg_filtered_array = []

    # TODO: CODE_REVIEW: Should these thresholds be brought out to the outmost layer?
    for data in window_data.items():
        if classify_ecg_window(data, range_threshold=200, slope_threshold=50, maximum_value=4000):
            ecg_filtered_array.extend(data)

    ecg_filtered.set_datapoints(ecg_filtered_array)

    return ecg_filtered


def compute_rr_intervals(ecg: DataStream,
                         fs: float) -> DataStream:
    """
    filter ecg ...
    :param ecg:
    :param fs:
    :return:
    """
    ecg_filtered = filter_bad_ecg(ecg, fs)

    # compute the r-peak array
    ecg_rpeak = detect_rpeak(ecg_filtered, fs)

    return ecg_rpeak


# TODO: CODE_REVIEW: Justify in the method documentation string the justification of the default values
def rr_interval_update(rpeak_temp1: List[DataPoint],
                       rr_ave: float,
                       min_size: int = 8) -> float:
    """
    :param min_size:
    :param rpeak_temp1: R peak locations
    :param rr_ave: previous rr-interval average
    :return: the new rr-interval average of the previously detected 8 R peak locations
    """
    peak_interval = np.diff([0] + rpeak_temp1)
    return rr_ave if len(peak_interval) < min_size else np.sum(peak_interval[-min_size:]) / min_size


def compute_moving_window_int(sample: np.ndarray,
                              fs: float,
                              blackman_window_length: float) -> np.ndarray:
    """
    :param sample: ecg sample array
    :param fs: sampling frequency
    :param blackman_window_length: length of the blackman window on which to compute the moving window integration

    :return: the Moving window integration of the sample array
    """
    # TODO: CODE_REVIEW: Should these constants be moved to a Constants file or kept here?
    # TODO: CODE_REVIEW: blackman_window_length parameter is not utilized
    delta = .02
    # filter edges
    filter_edges = [0, 4.5 * 2 / fs, 5 * 2 / fs, 20 * 2 / fs, 20.5 * 2 / fs, 1]
    # gains at filter band edges
    gains = [0, 0, 1, 1, 0, 0]
    # weights
    weights = [500 / delta, 1 / delta, 500 / delta]
    # length of the FIR filter
    filter_length = 257  # TODO: CODE_REVIEW: Parameterize? 2^8+1?
    # FIR filter coefficients for bandpass filtering
    filter_coeff = signal.firls(filter_length, filter_edges, gains, weights)

    # bandpass filtered signal
    bandpass_signal = signal.convolve(sample, filter_coeff, 'same')
    bandpass_signal /= np.percentile(bandpass_signal, 90)

    # derivative array
    derivative_array = (np.array([-1.0, -2.0, 0, 2.0, 1.0])) * (1 / 8)
    # derivative signal (differentiation of the bandpass)
    derivative_signal = signal.convolve(bandpass_signal, derivative_array, 'same')
    derivative_signal /= np.percentile(derivative_signal, 90)

    # squared derivative signal
    derivative_squared_signal = derivative_signal ** 2
    derivative_squared_signal /= np.percentile(derivative_squared_signal, 90)

    # blackman window
    blackman_window_length = np.ceil(fs / 5)
    blackman_window = np.blackman(blackman_window_length)

    # moving window Integration of squared derivative signal
    mov_win_int_signal = signal.convolve(derivative_squared_signal, blackman_window, 'same')
    mov_win_int_signal /= np.percentile(mov_win_int_signal, 90)

    return mov_win_int_signal


def check_peak(data: List[DataPoint]) -> bool:
    """
    This is a function to check the condition of a simple peak of signal y in index i
    :param data:
    :return:
    """
    # TODO: Is this new implementation more general than this hardcoded logic?
    # (y[i - 2] < y[i - 1] < y[i]) and (y[i] >= y[i + 1] > y[i + 2])

    # TODO: CODE_REVIEW: How to handle exceptions in this code?
    if len(data) < 3:
        raise Exception("data too small")

    midpoint = int(len(data) / 2)
    test_value = data[0]

    for i in data[1:midpoint + 1]:
        if test_value < i:
            test_value = i
        else:
            return False

    for i in data[midpoint + 1:]:
        if test_value > i:
            test_value = i
        else:
            return False

    return True


# TODO: CODE_REVIEW: Justify in the method documentation string the justification of the default values
# TODO: CODE_REVIEW: Make hard-coded constants default method parameter
def compute_r_peaks(threshold_1: float,
                    rr_ave: float,
                    mov_win_int_signal: np.ndarray,
                    valuepks: List[DataPoint],
                    # TODO: CODE_REVIEW: valuepks and pkt should be combined to List[DataPoint] and code adjusted
                    pkt: list) -> list:
    """
    This function does the adaptive thresholding of the signal to get the R-peak locations


    :param threshold_1: Thr1 is the threshold above which the R peak
    :param rr_ave: running RR-interval average
    :param mov_win_int_signal: signal sample array
    :param valuepks: simple peaks of the process done before
    :param pkt: location of the simple peaks in signal array
    :param rpeak_temp1: The location of the R peaks in the signal sample array once found this is returned
    :param pkt_loc: location of the R peaks in the simple peak array detected in the process before

    """
    # TODO: CODE_REVIEW: Do threshold 1 and 2 have better names/meaning?
    threshold_2 = 0.5 * threshold_1  # any signal value between thr2 and thr1 is a noise peak
    sig_lev = 4 * threshold_1  # current signal level -any signal above thrice the signal level is discarded as a spurious value
    noise_lev = 0.1 * sig_lev  # current noise level of the signal

    i = 0  # TODO: CODE_REVIEW: Use a better variable name
    rpeak_temp1 = []
    pkt_loc = []
    while i < len(pkt):
        # if for 166 percent of the present RR interval no peak is detected as R peak then thr2 is taken as the
        # R peak threshold and the maximum of the range is taken as a R peak and RR interval is updated accordingly
        # TODO: CODE_REVIEW: Lots of hard-coded values here, they need refactored
        if len(rpeak_temp1) >= 8 and pkt[i] - pkt[pkt_loc[-1]] > 1.66 * rr_ave and i - pkt_loc[-1] > 1:

            # values and indexes of previous peaks discarded as not an R peak whose magnitude is above thr2
            searchback_array_inrange_values = [valuepks[k] for k in range(pkt_loc[-1] + 1, i) if
                                               3 * sig_lev > valuepks[k] > threshold_2]
            searchback_array_inrange_index = [k - pkt_loc[-1] for k in range(pkt_loc[-1] + 1, i) if
                                              3 * sig_lev > valuepks[k] > threshold_2]

            if len(searchback_array_inrange_values) > 0:
                # maximum inside the range calculated beforehand is taken as R peak
                searchback_max_index = np.argmax(searchback_array_inrange_values)

                rpeak_temp1.append(pkt[pkt_loc[-1] + 1 + searchback_array_inrange_index[searchback_max_index]])
                pkt_loc.append(pkt_loc[-1] + 1 + searchback_array_inrange_index[searchback_max_index])

                sig_lev = ewma(mov_win_int_signal[pkt[i]], sig_lev, 0.125)  # update the current signal level

                threshold_1 = noise_lev + 0.25 * (sig_lev - noise_lev)
                threshold_2 = 0.5 * threshold_1

                rr_ave = rr_interval_update(rpeak_temp1, rr_ave)
                i = pkt_loc[-1] + 1
            else:
                threshold_1 = noise_lev + 0.25 * (sig_lev - noise_lev)
                threshold_2 = 0.5 * threshold_1
                i += 1
        else:
            # R peak checking
            if threshold_1 <= mov_win_int_signal[pkt[i]] < 3 * sig_lev:
                rpeak_temp1.append(pkt[i])
                pkt_loc.append(i)

                sig_lev = ewma(mov_win_int_signal[pkt[i]], sig_lev, 0.125)  # update the signal level

            # noise peak checking
            elif threshold_1 > mov_win_int_signal[pkt[i]] > threshold_2:
                noise_lev = ewma(mov_win_int_signal[pkt[i]], noise_lev, 0.125)  # update the noise level

            threshold_1 = noise_lev + 0.25 * (sig_lev - noise_lev)
            threshold_2 = 0.5 * threshold_1
            i += 1

    return rpeak_temp1


def ewma(value: float, new_value: float, alpha: float) -> float:
    """

    :param value:
    :param new_value:
    :param alpha:
    :return:
    """
    return alpha * new_value + (1 - alpha) * value


# TODO: CODE_REVIEW: Justify in the method documentation string the justification of the default values
# TODO: CODE_REVIEW: Make hard-coded constants default method parameter
def remove_close_peaks(rpeak_temp1: List[DataPoint],
                       sample: List[DataPoint],
                       fs: float) -> List[DataPoint]:
    """
    This function removes one of two peaks from two consecutive R peaks
    if difference among them is less than the minimum possible

    :param rpeak_temp1: R peak array containing the index of the R peaks
    :param sample: sample array
    :param fs: sampling frequency
    :return: R peak array with no close R peaks
    """

    # TODO: CODE_REVIEW: What does normal initialization mean?  Why 1 and 2
    temp_ind = [1, 2]  # just a normal initialization so that it goes to while loop
    while len(temp_ind) != 0:
        temp = np.diff(rpeak_temp1)

        # index of the closest peak to discard
        temp_ind = []
        for i, value in enumerate(temp):
            if value < .5 * fs:  # TODO: CODE_REVIEW: Hard coded constant
                if sample[rpeak_temp1[i]] > sample[rpeak_temp1[i + 1]]:
                    temp_ind.append(i)
                else:
                    temp_ind.append(i + 1)

        # removal procedure
        count = 0
        for i in temp_ind:
            rpeak_temp1.remove(rpeak_temp1[i - count])
            count += 1
    return rpeak_temp1


# TODO: CODE_REVIEW: Justify in the method documentation string the justification of the default values
# TODO: CODE_REVIEW: Make hard-coded constants default method parameter
def confirm_peaks(rpeak_temp1: List[DataPoint],
                  sample: List[DataPoint],
                  fs: float) -> List[DataPoint]:
    """

    This function does the final check on the R peaks detected and
    finds the maximum in a range of fs/10 of the detected peak location and assigns it to be the peak

    :param rpeak_temp1: R peak array containing the index of the R peaks
    :param sample: sample array
    :param fs: sampling frequency
    :return: final R peak array

    """
    # TODO: CODE_REVIEW: Why is there  fs / 10 coded here? Should this be a parameter?
    for i in range(1, len(rpeak_temp1) - 1):
        start_index = int(rpeak_temp1[i] - np.ceil(fs / 10))
        end_index = int(rpeak_temp1[i] + np.ceil(fs / 10) + 1)

        index = np.argmax(sample[start_index:end_index])

        rpeak_temp1[i] = rpeak_temp1[i] - np.ceil(fs / 10) + index

    return np.array(rpeak_temp1).astype(
        np.int64)  # TODO: CODE_REVIEW: This is the first time I am seeing np.int64  Why is this type specified? Result should be list of DataPoints


# TODO: CODE_REVIEW: Justify in the method documentation string the justification of the default values
# TODO: CODE_REVIEW: Make hard-coded constants default method parameter
def detect_rpeak(ecg: DataStream,
                 fs: float = 64,
                 threshold: float = 0.5) -> DataStream:
    """
    This program implements the Pan Tomkins algorithm on ECG signal to detect the R peaks

    Since the ecg array can have discontinuity in the timestamp arrays the rr-interval calculated
    in the algorithm is calculated in terms of the index in the sample array

    The algorithm consists of some major steps

    1. computation of the moving window integration of the signal in terms of blackman window of a prescribed length
    2. compute all the peaks of the moving window integration signal
    3. adaptive thresholding with dynamic signal and noise thresholds applied to filter out the R peak locations
    4. confirm the R peaks through differentiation from the nearby peaks and remove the false peaks

    :param ecg: ecg array of tuples (timestamp,value)
    :param fs: sampling frequency
    :param threshold: initial threshold to detect the R peak in a signal normalized by the 90th percentile. .5 is default.
    :return: R peak array of tuples (timestamp, Rpeak interval)
    """

    data = ecg.datapoints

    sample = np.array([i.sample for i in data])
    timestamp = np.array([i.start_time for i in data])

    # computes the moving window integration of the signal
    blackman_win_len = np.ceil(fs / 5)  # TODO: CODE_REVIEW: Hard coded value of 5
    y = compute_moving_window_int(sample, fs, blackman_win_len)

    # TODO: CODE_REVIEW: Hard coded +/- 2.  Is this a function of the blackman_win_len?
    peak_location = [i for i in range(2, len(y) - 1) if check_peak(y[i - 2:i + 2])]
    peak_location_values = y[peak_location]

    # initial RR interval average
    running_rr_avg = sum(np.diff(peak_location)) / (len(peak_location) - 1)
    rpeak_temp1 = compute_r_peaks(threshold, running_rr_avg, y, peak_location_values, peak_location)
    rpeak_temp1 = remove_close_peaks(rpeak_temp1, sample, fs)
    index = confirm_peaks(rpeak_temp1, sample, fs)

    rpeak_timestamp = timestamp[index]
    rpeak_value = np.diff(rpeak_timestamp)
    rpeak_timestamp = rpeak_timestamp[1:]

    result_data = []
    for k in range(len(rpeak_value)):
        result_data.append(DataPoint.from_tuple(rpeak_timestamp[k], rpeak_value[k]))

    # Create resulting datastream to be returned
    result = DataStream.from_datastream([ecg])
    result.set_datapoints(result_data)

    return result
