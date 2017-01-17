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
import numpy as np
from scipy import signal

def rr_interval_update(rpeak_temp1, rr_ave):
    """
    :param rpeak_temp1: R peak locations
    :param rr_ave: previous rr-interval average
    :return: the new rr-interval average of the previously detected 8 R peak locations
    """
    R=[0]+rpeak_temp1
    peak_interval = np.diff(R)
    if len(peak_interval) <= 7:
        return rr_ave
    else:
        x=np.sum(peak_interval[-8:]) / 8
        return x


def compute_moving_window_int(sample,fs,blackmanWinlen):
    """

    :param sample: ecg sample array
    :param fs: sampling frequency
    :param blackmanWinlen: length of the blackman window on which to compute the moving window integration

    :return: the Moving window integration of the sample array
    """
    f = 2 / fs
    Del=.02
    F = [0, 4.5 * f, 5 * f, 20 * f, 20.5 * f, 1] #filter edges
    A = [0, 0, 1, 1, 0, 0] #gains at fliter band edges
    w = [500 / Del, 1 / Del, 500 / Del] #weights
    fl = 257 #length of the FIR filter
    b = signal.firls(fl, F, A, w) #FIR filter coefficients for bandpass filtering
    y2 = signal.convolve(sample, b, 'same') #bandpass filtered signal
    y2 = y2 / np.percentile(y2, 90)

    h_D = (np.array([-1.0, -2.0, 0, 2.0, 1.0])) * (1 / 8) #derivative array
    y3 = signal.convolve(y2, h_D, 'same') #derivative signal (differentiation of the bandpass)
    y3 = y3 / np.percentile(y3, 90)

    y4 = y3 ** 2 #squared derivative
    y4 = y4 / np.percentile(y4, 90)

    blackmanWinlen = np.ceil(fs / 5)
    h_I = np.blackman(blackmanWinlen) # blackman window
    mov_win_int_signal = signal.convolve(y4, h_I, 'same') #moving window Integration of squared derivative signal
    mov_win_int_signal = mov_win_int_signal / np.percentile(mov_win_int_signal, 90)
    return mov_win_int_signal

def check_peak(y,i):
    """
    This is a function to check the condition of a simple peak of signal y in index i
    """
    return y[i - 2] < y[i - 1] and y[i - 1] < y[i] and y[i] >= y[i + 1] and y[i + 1] > y[i + 2]

def compute_r_peaks(rpeak_temp1,pkt_loc,sig_lev,noise_lev,thr1,thr2,rr_ave,mov_win_int_signal,valuepks,pkt):
    """
    This function does the adaptive thresholding of the signal to get the R-peak locations
    :param rpeak_temp1: The location of the R peaks in the signal sample array once found this is returned
    :param pkt_loc: location of the R peaks in the simple peak array detected in the process before
    :param sig_lev: signal threshold level which is updated accordingly
    :param noise_lev: noise threshold level which is updated accordingly
    :param thr1: Thr1 is the threshold above which the R peak
    :param thr2: any signal value between thr2 and thr1 is a noise peak
    :param rr_ave: running RR-interval average
    :param mov_win_int_signal: signal sample array
    :param valuepks: simple peaks of the process done before
    :param pkt: location of the simple peaks in signal array

    """
    i=0
    while i < len (pkt):
        if len(rpeak_temp1) >= 8 and pkt[i] - pkt[pkt_loc[-1]] > 1.66 * rr_ave and i - pkt_loc[-1] > 1:
            #
            # if for 166 percent of the present RR interval no peak is detected as R peak then thr2 is taken as the
            # R peak threshold and the maximum of the range is taken as a R peak and RR interval is updated accordingly
            #
            searchback_array_inrange_values=[valuepks[k] for k in range(pkt_loc[-1]+1,i) if valuepks[k] < 3*sig_lev and valuepks[k]> thr2]
            searchback_array_inrange_index=[k-pkt_loc[-1] for k in range(pkt_loc[-1]+1,i) if valuepks[k] < 3*sig_lev and valuepks[k]> thr2]
            if len(searchback_array_inrange_values) > 0:
                searchback_max_index = np.argmax(searchback_array_inrange_values)
                rpeak_temp1.append(pkt[pkt_loc[-1]+1+searchback_array_inrange_index[searchback_max_index]])
                pkt_loc.append(pkt_loc[-1]+1+searchback_array_inrange_index[searchback_max_index])
                sig_lev = 0.125 * mov_win_int_signal[pkt[i]] + 0.875 * sig_lev
                thr1 = noise_lev + 0.25 * (sig_lev - noise_lev)
                thr2 = 0.5 * thr1
                rr_ave = rr_interval_update(rpeak_temp1, rr_ave)
                i = pkt_loc[-1] + 1
            else:
                thr1 = noise_lev + 0.25 * (sig_lev - noise_lev)
                thr2 = 0.5 * thr1
                i = i + 1
        else:
            if mov_win_int_signal[pkt[i]] >= thr1 and mov_win_int_signal[pkt[i]] < 3 * sig_lev:
                rpeak_temp1.append(pkt[i])
                pkt_loc.append(i)
                sig_lev = 0.125 * mov_win_int_signal[pkt[i]] + 0.875 * sig_lev
            elif mov_win_int_signal[pkt[i]] < thr1 and mov_win_int_signal[pkt[i]] > thr2:
                noise_lev = 0.125 * mov_win_int_signal[pkt[i]] + 0.875 * noise_lev
            thr1 = noise_lev + 0.25 * (sig_lev - noise_lev)
            thr2 = 0.5 * thr1
            i = i + 1
    return rpeak_temp1

def remove_close_peaks(rpeak_temp1,sample,fs):
    """
    This function removes one of two peaks from the detected R peaks
    if difference among them is less than the minimum possible for two consecutive R peaks
    """
    def check_min(sample,rpeak_temp1,i):
        return i if sample[rpeak_temp1[i]] > sample[rpeak_temp1[i+1]] else i+1
    temp_ind=[1,2]
    while len(temp_ind) != 0:
        temp = np.diff(rpeak_temp1)
        temp_ind=[check_min(sample,rpeak_temp1,i) for i in range(len(temp)) if temp[i]<.5*fs]
        count=0
        for i in temp_ind:
            rpeak_temp1.remove(rpeak_temp1[i-count])
            count=count+1
    return rpeak_temp1

def confirm_peaks(rpeak_temp1,sample,fs):
    """
    This function does the final check on the R peaks detected and
    finds the maximum in a range of fs/10 of the detected peak location and assigns it to be the peak
    """
    count=1
    for i in range(1,len(rpeak_temp1)-1):
        index=np.argmax(sample[int(rpeak_temp1[i]-np.ceil(fs/10)):int(rpeak_temp1[i]+np.ceil(fs/10)+1)])
        rpeak_temp1[i]=rpeak_temp1[i]-np.ceil(fs/10)+index
        count=count+1
    return np.array(rpeak_temp1).astype(np.int64)


def detect_rpeak(ecg,fs:int=64,threshold:float=0.5):
    """
    This program implements the Pan Tomkins algorithm on ECG signal to detect the R peaks

    Since the ecg array can have discontinuity in the timestamp arrays the rr-interval calculated
    in the algorithm is calculated in terms of the index in the sample array

    The algorithm consists of some major steps

    1. computation of the moving window integration of the signal in terms of balckman window of a prescribed length
    2. compute all the peaks of the moving window integration signal
    3. adaptive thresholding with dynamic signal and noise thresholds applied to filter out the R peak locations
    4. confirm the R peaks through differntiation from the nearby peaks and remove the false peaks
    """
    sample=np.array([i[1] for i in ecg])
    timestamp=np.array([i[0] for i in ecg])

    blackmanWinlen = np.ceil(fs / 5)
    y=compute_moving_window_int(sample,fs,blackmanWinlen) #computes the moving window integration of the signal

    peak_location=[i for i in range(2, len(y) - 1) if check_peak(y,i)==True]
    peak_location_values = y[peak_location]
    running_rr_avg = sum(np.diff(peak_location)) / (len(peak_location) - 1) #initial RR interval average

    thr1 = threshold
    thr2 = 0.5 * thr1
    sig_lev = 4 * thr1
    noise_lev = 0.1 * sig_lev
    rpeak_temp1 = compute_r_peaks([],[],sig_lev,noise_lev,thr1,thr2,running_rr_avg,y,peak_location_values,peak_location)
    rpeak_temp1 = remove_close_peaks(rpeak_temp1[0:],sample,fs)
    index = confirm_peaks(rpeak_temp1,sample,fs)

    rpeak_timestamp=timestamp[index]
    rpeak_value=np.diff(rpeak_timestamp)
    rpeak_timestamp = rpeak_timestamp[1:]
    return [(rpeak_timestamp[k],rpeak_value[k]) for k in range(len(rpeak_value))]

