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
from cerebralcortex.kernel.datatypes.datastream import DataStream
from memphisdataprocessor.preprocessor.rpeak_detect import detect_rpeak
import numpy as np


def classify_ecg_window(window: list,range_threshold:int = 200, slope_threshold:int = 50, maximum_value:int=4000):
    """
    :param window: window of raw ecg tuples
    :param range_threshold: range of the window
    :param slope_threshold: median slope of the window
    :param maximum_value: maximum value
    :return: True/False decision based on the three parameters

    """
    values = np.array([i[1] for i in window])
    if max(values) - min(values) < range_threshold:
        return False
    if max(values) > maximum_value:
        return False
    if np.median(np.abs(np.diff(values))) > slope_threshold:
        return False
    return True

def filter_bad_ecg(ecg_array: list, fs: float, no_of_secs:int = 2)-> list:
    """
    This function splits the ecg array into non overlapping windows of specified seconds
    and assigns a binary decision on them returns a filtered ecg array which contains
    only the windows to be kept

    :param ecg_array: raw ecg array
    :param fs: sampling frequency
    :param no_of_secs : no of seconds each window would be long
    :return:  filtered ecg array
    """

    window_length = int(no_of_secs * fs)

    ecg_filtered_array = []
    for i in range(0,len(ecg_array)-window_length-1,window_length):
        #get the index of the window elements
        index = [k for k in range(i,i+window_length)]

        if classify_ecg_window(ecg_array[index]):
            #append the elements to the returned array
            for x in ecg_array[index]:
                ecg_filtered_array.append((x[0],x[1]))

    return ecg_filtered_array


def compute_rr_datastream(ecg:DataStream,fs:float)-> list:
    # get the ecg array from datastream
    ecg_array = np.array([(x.get_timestamp_epoch()/1000, x.get_sample()) for x in ecg.get_datapoints()])

    # filter the ecg array
    ecg_filtered_array = filter_bad_ecg(ecg_array,fs)

    #compute the r-peak array
    ecg_rpeak_array = detect_rpeak(ecg_filtered_array,fs)

    # return DataStream(ecg.get_user(),data=ecg_rpeak_array)
    return ecg_rpeak_array