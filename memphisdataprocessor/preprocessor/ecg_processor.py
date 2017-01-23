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

from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.window import window
from memphisdataprocessor.preprocessor.rpeak_detect import detect_rpeak


# TODO: CODE_REVIEW: Justify in the method documentation string the justification of the default values
def classify_ecg_window(data: list,
                        range_threshold: int = 200,
                        slope_threshold: int = 50,
                        maximum_value: int = 4000):
    """

    :param data: window of raw ecg tuples
    :param range_threshold: range of the window
    :param slope_threshold: median slope of the window
    :param maximum_value: maximum value
    :return: True/False decision based on the three parameters
    """

    values = np.array([i.get_sample() for i in data])
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
    window_data = window(ecg.get_datapoints(), window_size=window_length)

    ecg_filtered = DataStream.from_datastream([ecg])
    ecg_filtered_array = []

    # TODO: CODE_REVIEW: Should these thresholds be brought out to the outmost layer?
    for key, data in window_data:
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
