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
import matplotlib.pyplot as plt

def compute_peak_valley(rip: DataStream,
                        fs: float) -> [DataStream, DataStream]:
    peak_datastream, valley_datastream = get_peak_valley(rip=rip, fs=fs)
    return peak_datastream, valley_datastream

def get_peak_valley(rip: DataStream,
                    fs: float = 21.33,
                    smoothing_factor: int = 5,
                    time_window: int = 8) -> [DataStream, DataStream]:

    data = rip.get_datapoints()
    sample = np.array([i.get_sample() for i in data])
    timestamp = np.array([i.get_timestamp() for i in data])

    sample = smooth(sample, smoothing_factor)
    window_length = round(time_window * fs)
    mac = get_moving_average_curve(sample, window_length=window_length)

    plt.plot(sample, 'b')
    plt.plot(range(window_length + 1, len(sample) - (window_length + 1)), mac, 'r')

    up_intercept, up_intercept_indices, down_intercept, down_intercept_indices = get_intercepts(sample=sample, mac=mac, window_length=window_length)

    plt.plot(up_intercept_indices, [sample[i] for i in up_intercept_indices], 'k^', linewidth=12)
    plt.plot(down_intercept_indices, [sample[i] for i in down_intercept_indices], 'kv', linewidth=12)

    up_intercept_indices, down_intercept_indices = filter_intercept_outlier(down_intercept_indices=down_intercept_indices, up_intercept_indices=up_intercept_indices, window_length=window_length)

    plt.plot(up_intercept_indices, [sample[i] for i in up_intercept_indices], 'r^', linewidth=5)
    plt.plot(down_intercept_indices, [sample[i] for i in down_intercept_indices], 'rv', linewidth=5)

    peaks, peak_indices, valleys, valley_indices = generate_peak_valley(up_intercept_indices=up_intercept_indices, down_intercept_indices=down_intercept_indices, sample=sample)

    plt.plot(peak_indices, [sample[i] for i in peak_indices], 'o')
    plt.plot(valley_indices, [sample[i] for i in valley_indices], 's')


    # valley_indices = correct_valley_position(peak_indices=peak_indices, valley_indices=valley_indices, up_intercept_indices=up_intercept_indices, sample=sample)  # push valley towards right
    # plt.plot(valley_indices, [sample[i] for i in valley_indices], 'ks')

    plt.show()

    peak_datapoints, valley_datapoints = [], []

    for i in peak_indices:
        peak_datapoints.append(DataPoint.from_tuple(sample[i], timestamp[i]))

    for i in valley_indices:
        valley_datapoints.append(DataPoint.from_tuple(sample[i], timestamp[i]))

    peak_datastream = DataStream.from_datastream([rip])
    peak_datastream.set_datapoints(peak_datapoints)
    valley_datastream = DataStream.from_datastream([rip])
    valley_datastream.set_datapoints(valley_datapoints)

    return peak_datastream, valley_datastream

def get_moving_average_curve(sample: np.ndarray,
                             window_length: int) -> np.ndarray:
    mac = []
    for i in range(window_length+1, len(sample) - (window_length + 1)):
        averageValue = sample[i - window_length:i + window_length].mean()
        mac.append(averageValue)

    return mac


def get_intercepts(sample: np.ndarray,
                   mac: np.ndarray,
                   window_length: int) -> [np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    up_intercept, up_intercept_indices, down_intercept, down_intercept_indices = [], [], [], []

    for i in range(1, len(mac)):
        if (sample[i + window_length - 1] <= mac[i - 1] and sample[i + window_length] >= mac[i]):
            up_intercept.append(mac[i])
            up_intercept_indices.append(i + window_length - 1)
        elif (sample[i + window_length - 1] >= mac[i - 1] and sample[i + window_length] <= mac[i]):
            down_intercept.append(mac[i])
            down_intercept_indices.append(i + window_length - 1)

    return up_intercept, up_intercept_indices, down_intercept, down_intercept_indices

def filter_intercept_outlier(up_intercept_indices: np.ndarray,
                             down_intercept_indices: np.ndarray,
                             window_length: int):
    # removal of 2 or more consecutive up intercepts or down intercepts

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
    while uii[i] < dii[0]:
        i += 1

    uii = uii[i:]

    minLength = min(len(uii), len(dii))
    uii, dii = uii[:minLength], dii[:minLength]

    return uii, dii

def generate_peak_valley(up_intercept_indices: np.ndarray,
                         down_intercept_indices: np.ndarray,
                         sample: np.ndarray) -> [np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    peaks, valleys, peak_indices, valley_indices = [], [], [], []
    for i in range(len(down_intercept_indices) - 1):  # As UI>DI, Down Intercept comes first
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


def correct_valley_position(peak_indices: np.ndarray,
                            valley_indices: np.ndarray,
                            up_intercept_indices: np.ndarray,
                            sample: np.ndarray) -> np.ndarray:
    valley_indicesUpdated = valley_indices.copy()

    for i in range(len(valley_indices)):
        upIndicesValleyToPeak = [j for j in range(len(up_intercept_indices)) if (valley_indices[i] < up_intercept_indices[j]) and (up_intercept_indices[j] < peak_indices[i])]

        if len(upIndicesValleyToPeak) > 0:
            if len(upIndicesValleyToPeak) > 1:
                print('WARNING::: More than 2 consectuve up intercept is detected.')
                continue
            else:
                upIndexValleyToPeak = upIndicesValleyToPeak[0]
                sampleValleyToUI = sample[valley_indices[i]: up_intercept_indices[
                    upIndexValleyToPeak]]  # all points between current valley and UI
                slopeAtSamples = np.diff(sampleValleyToUI)  # np.diff is equivalent to matlab diff

                noConsecutivePositiveSlopes = [-1] * len(slopeAtSamples)
                for j in range(len(slopeAtSamples)):
                    slopesSubset = slopeAtSamples[j:]
                    if all(slope > 0 for slope in slopesSubset):
                        noConsecutivePositiveSlopes = len(slopesSubset)

                if any(noConSlope > 0 for noConSlope in noConsecutivePositiveSlopes):
                    indicesMaxPosSlope = [k for k in range(len(noConsecutivePositiveSlopes)) if
                                          noConsecutivePositiveSlopes[k] == max(noConsecutivePositiveSlopes)]
                    valley_indicesUpdated = valley_indices[i] + indicesMaxPosSlope[-1] - 1

    return valley_indicesUpdated

def smooth(data: np.ndarray,
           span: int = 5) -> np.ndarray:
    data_middle = np.convolve(data, np.ones(span, dtype=int), 'valid') / span
    divisor = np.arange(1, span - 1, 2)
    data_start = np.cumsum(data[:span - 1])[::2] / divisor
    data_end = (np.cumsum(data[:-span:-1])[::2] / divisor)[::-1]
    data_smooth = np.concatenate((data_start, data_middle, data_end))

    return data_smooth


