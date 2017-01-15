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
from cerebralcortex.kernel.datatypes.datastream import DataStream
from memphisdataprocessor.preprocessor.rpeak_detect import detect_rpeak
import numpy as np
import uuid

def classify_ecg_window(window):
    '''
    param
    window --window of raw ecg tuples

    returns a decision on the window based on three conditions
    '''
    values = np.array([i[1] for i in window])
    if max(values) - min(values) < 200:
        return False
    if max(values) > 4000:
        return False
    if np.median(np.abs(np.diff(values))) > 50:
        return False
    return True

def filter_bad_ecg(ecg_array,fs):
    '''
    param
    ecg_array-- is a list of raw ecg tuples
    fs -- is the sampling frequency

    This function splits the ecg array into non overlapping windows of 2 seconds and assigns a binary decision on them

    returns a filtered ecg array which contains only the windows to be kept
    '''
    window_length = int(2 * fs)

    ecg_filtered_array = []
    for i in range(0,len(ecg_array)-window_length-1,window_length):
        index = [k for k in range(i,i+window_length)]
        if classify_ecg_window(ecg_array[index])==True:
            for x in ecg_array[index]:
                ecg_filtered_array.append((x[0],x[1]))

    return ecg_filtered_array


def compute_rr_datastream(ecg:DataStream,fs:float)->DataStream:
    ecg_array = np.array([(x.get_timestamp_epoch()/1000, x.get_sample()) for x in ecg.get_datapoints()])
    ecg_filtered_array = filter_bad_ecg(ecg_array,fs)
    ecg_rpeak_array = detect_rpeak(ecg_filtered_array,fs)
    return DataStream(uuid.uuid4(),data=ecg_rpeak_array)
