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
import numpy as np
from scipy import signal


def rr_interval_update(rpeak_temp1: list,
                       rr_ave: float) -> float:
    """
    :param rpeak_temp1: R peak locations
    :param rr_ave: previous rr-interval average
    :return: the new rr-interval average of the previously detected 8 R peak locations
    """
    peak_interval = np.diff([0] + rpeak_temp1)
    return rr_ave if len(peak_interval) <= 7 else np.sum(peak_interval[-8:]) / 8


def compute_moving_window_int(sample: list,
                              fs: float,
                              blackmanWinlen: float) -> list:
    """
    :param sample: ecg sample array
    :param fs: sampling frequency
    :param blackmanWinlen: length of the blackman window on which to compute the moving window integration

    :return: the Moving window integration of the sample array
    """
    Del = .02
    # filter edges
    filter_edges = [0, 4.5 * 2 / fs, 5 * 2 / fs, 20 * 2 / fs, 20.5 * 2 / fs, 1]
    # gains at fliter band edges
    gains = [0, 0, 1, 1, 0, 0]
    # weights
    weights = [500 / Del, 1 / Del, 500 / Del]
    # length of the FIR filter
    filter_length = 257
    # FIR filter coefficients for bandpass filtering
    filter_coeff = signal.firls(filter_length, filter_edges, gains, weights)

    # bandpass filtered signal
    bandpass_signal = signal.convolve(sample, filter_coeff, 'same')
    bandpass_signal = bandpass_signal / np.percentile(bandpass_signal, 90)

    # derivative array
    derivative_array = (np.array([-1.0, -2.0, 0, 2.0, 1.0])) * (1 / 8)
    # derivative signal (differentiation of the bandpass)
    derivative_signal = signal.convolve(bandpass_signal, derivative_array, 'same')
    derivative_signal = derivative_signal / np.percentile(derivative_signal, 90)

    # squared derivative signal
    derivative_squared_signal = derivative_signal ** 2
    derivative_squared_signal = derivative_squared_signal / np.percentile(derivative_squared_signal, 90)

    # blackman window
    blackmanWinlen = np.ceil(fs / 5)
    blackman_window = np.blackman(blackmanWinlen)
    # moving window Integration of squared derivative signal
    mov_win_int_signal = signal.convolve(derivative_squared_signal, blackman_window, 'same')
    mov_win_int_signal = mov_win_int_signal / np.percentile(mov_win_int_signal, 90)

    return mov_win_int_signal


def check_peak(y, i):
    """
    This is a function to check the condition of a simple peak of signal y in index i
    :param y: sample array
    :param i: index
    :return: True/False= peak/not_peak
    """
    return y[i - 2] < y[i - 1] and y[i - 1] < y[i] and y[i] >= y[i + 1] and y[i + 1] > y[i + 2]


def compute_r_peaks(thr1: float,
                    rr_ave: float,
                    mov_win_int_signal: list,
                    valuepks: list,
                    pkt: list,
                    ) -> list:
    """
    This function does the adaptive thresholding of the signal to get the R-peak locations


    :param thr1: Thr1 is the threshold above which the R peak
    :param rr_ave: running RR-interval average
    :param mov_win_int_signal: signal sample array
    :param valuepks: simple peaks of the process done before
    :param pkt: location of the simple peaks in signal array
    :param rpeak_temp1: The location of the R peaks in the signal sample array once found this is returned
    :param pkt_loc: location of the R peaks in the simple peak array detected in the process before

    """

    thr2 = 0.5 * thr1  # any signal value between thr2 and thr1 is a noise peak
    sig_lev = 4 * thr1  # current signal level -any signal above thrice the signal level is discarded as a spurious value
    noise_lev = 0.1 * sig_lev  # current noise level of the signal
    i = 0
    rpeak_temp1 = []
    pkt_loc = []
    while i < len(pkt):
        # if for 166 percent of the present RR interval no peak is detected as R peak then thr2 is taken as the
        # R peak threshold and the maximum of the range is taken as a R peak and RR interval is updated accordingly
        if len(rpeak_temp1) >= 8 and pkt[i] - pkt[pkt_loc[-1]] > 1.66 * rr_ave and i - pkt_loc[-1] > 1:

            # values and indexes of previous peaks discarded as not an R peak whose magnitude is above thr2
            searchback_array_inrange_values = [valuepks[k] for k in range(pkt_loc[-1] + 1, i) if
                                               valuepks[k] < 3 * sig_lev and valuepks[k] > thr2]
            searchback_array_inrange_index = [k - pkt_loc[-1] for k in range(pkt_loc[-1] + 1, i) if
                                              valuepks[k] < 3 * sig_lev and valuepks[k] > thr2]

            if len(searchback_array_inrange_values) > 0:
                # maximum inside the range calculated beforehand is taken as R peak
                searchback_max_index = np.argmax(searchback_array_inrange_values)
                rpeak_temp1.append(pkt[pkt_loc[-1] + 1 + searchback_array_inrange_index[searchback_max_index]])
                pkt_loc.append(pkt_loc[-1] + 1 + searchback_array_inrange_index[searchback_max_index])
                sig_lev = 0.125 * mov_win_int_signal[pkt[i]] + 0.875 * sig_lev  # update the current signal level
                thr1 = noise_lev + 0.25 * (sig_lev - noise_lev)
                thr2 = 0.5 * thr1
                rr_ave = rr_interval_update(rpeak_temp1, rr_ave)
                i = pkt_loc[-1] + 1
            else:
                thr1 = noise_lev + 0.25 * (sig_lev - noise_lev)
                thr2 = 0.5 * thr1
                i = i + 1
        else:
            # R peak checking
            if mov_win_int_signal[pkt[i]] >= thr1 and mov_win_int_signal[pkt[i]] < 3 * sig_lev:
                rpeak_temp1.append(pkt[i])
                pkt_loc.append(i)
                sig_lev = 0.125 * mov_win_int_signal[pkt[i]] + 0.875 * sig_lev  # update the signal level
            # noise peak checking
            elif mov_win_int_signal[pkt[i]] < thr1 and mov_win_int_signal[pkt[i]] > thr2:
                noise_lev = 0.125 * mov_win_int_signal[pkt[i]] + 0.875 * noise_lev  # update the noise level
            thr1 = noise_lev + 0.25 * (sig_lev - noise_lev)
            thr2 = 0.5 * thr1
            i = i + 1
    return rpeak_temp1


def remove_close_peaks(rpeak_temp1: list,
                       sample: list,
                       fs: float) -> int:
    """
    This function removes one of two peaks from two consecutive R peaks
    if difference among them is less than the minimum possible

    :param rpeak_temp1: R peak array containing the index of the R peaks
    :param sample: sample array
    :param fs: sampling frequency
    :return: R peak array with no close R peaks

    """

    def check_min(sample, rpeak_temp1, i):
        """
        returns which peak has the maximum value (i)th or (i+1)th
        """
        return i if sample[rpeak_temp1[i]] > sample[rpeak_temp1[i + 1]] else i + 1

    temp_ind = [1, 2]  # just a normal initialization so that it goes to while loop
    while len(temp_ind) != 0:
        temp = np.diff(rpeak_temp1)

        # index of the close peak to discard
        temp_ind = [check_min(sample, rpeak_temp1, i) for i in range(len(temp)) if temp[i] < .5 * fs]

        # removal procedure
        count = 0
        for i in temp_ind:
            rpeak_temp1.remove(rpeak_temp1[i - count])
            count = count + 1
    return rpeak_temp1


def confirm_peaks(rpeak_temp1: list, sample: list, fs: float) -> list:
    """

    This function does the final check on the R peaks detected and
    finds the maximum in a range of fs/10 of the detected peak location and assigns it to be the peak

    :param rpeak_temp1: R peak array containing the index of the R peaks
    :param sample: sample array
    :param fs: sampling frequency
    :return: final R peak array

    """

    for i in range(1, len(rpeak_temp1) - 1):
        index = np.argmax(sample[int(rpeak_temp1[i] - np.ceil(fs / 10)):int(rpeak_temp1[i] + np.ceil(fs / 10) + 1)])
        rpeak_temp1[i] = rpeak_temp1[i] - np.ceil(fs / 10) + index

    return np.array(rpeak_temp1).astype(np.int64)


def detect_rpeak(ecg: list, fs: int = 64, threshold: float = 0.5) -> list:
    """
    This program implements the Pan Tomkins algorithm on ECG signal to detect the R peaks

    Since the ecg array can have discontinuity in the timestamp arrays the rr-interval calculated
    in the algorithm is calculated in terms of the index in the sample array

    The algorithm consists of some major steps

    1. computation of the moving window integration of the signal in terms of balckman window of a prescribed length
    2. compute all the peaks of the moving window integration signal
    3. adaptive thresholding with dynamic signal and noise thresholds applied to filter out the R peak locations
    4. confirm the R peaks through differntiation from the nearby peaks and remove the false peaks

    :param ecg: ecg array of tuples (timestamp,value)
    :param fs: sampling frequency
    :param threshold: initial threshold to detect the R peak in a signal normalized by the 90th percentile. .5 is default.
    :return: R peak array of tuples (timestamp, Rpeak interval)
    """

    sample = np.array([i[1] for i in ecg])
    timestamp = np.array([i[0] for i in ecg])

    # computes the moving window integration of the signal
    blackmanWinlen = np.ceil(fs / 5)
    y = compute_moving_window_int(sample, fs, blackmanWinlen)

    peak_location = [i for i in range(2, len(y) - 1) if check_peak(y, i) == True]
    peak_location_values = y[peak_location]

    # initial RR interval average
    running_rr_avg = sum(np.diff(peak_location)) / (len(peak_location) - 1)
    thr1 = threshold
    rpeak_temp1 = compute_r_peaks(thr1, running_rr_avg, y, peak_location_values, peak_location)
    rpeak_temp1 = remove_close_peaks(rpeak_temp1[0:], sample, fs)
    index = confirm_peaks(rpeak_temp1, sample, fs)

    rpeak_timestamp = timestamp[index]
    rpeak_value = np.diff(rpeak_timestamp)
    rpeak_timestamp = rpeak_timestamp[1:]

    return [(rpeak_timestamp[k], rpeak_value[k]) for k in range(len(rpeak_value))]
