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

from cerebralcortex.data_processor.signalprocessing.window import window_sliding
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def rsa_calculate(start_time,
                  end_time,
                  datastream: DataStream):
    """

    :param start_time:
    :param end_time:
    :param datastream:
    :return: rsa sample
    """

    result = DataPoint.from_tuple(start_time, -1.0)

    _max = DataPoint.from_tuple(0, 0.0)
    _min = DataPoint.from_tuple(0, 0.0)
    maxFound = False
    minFound = False
    for dp in datastream.data:
        if start_time < dp.start_time < end_time:
            if _max.start_time == 0 and _min.start_time == 0:
                _max = DataPoint.from_tuple(start_time=dp.start_time, sample=dp.sample)
                _min = DataPoint.from_tuple(start_time=dp.start_time, sample=dp.sample)
            elif dp.sample >= _max.sample:
                _max = DataPoint.from_tuple(start_time=dp.start_time, sample=dp.sample)
                maxFound = True
            elif dp.sample <= _min.sample:
                _min = DataPoint.from_tuple(start_time=dp.start_time, sample=dp.sample)
                minFound = True
    if maxFound and minFound:
        result = DataPoint.from_tuple(start_time=result.start_time, sample=(_max.sample - _min.sample))
    return result.sample


def compute_datastream_rsa(valleys_datastream: DataStream,
                           rr_datastream: DataStream):
    """

    :param valleys_datastream:
    :param rr_datastream:
    :return: rsa_datastream
    """

    rsa_ds = DataStream.from_datastream([valleys_datastream])
    rsa_datapoints = []

    for index, value in enumerate(valleys_datastream.data[:-2]):
        rsa_sample = rsa_calculate(valleys_datastream.data[index].start_time,
                                   valleys_datastream.data[index + 2].start_time,
                                   rr_datastream)
        if rsa_sample != -1.0:
            rsa_datapoints.append(DataPoint.from_tuple(start_time=valleys_datastream.data[index].start_time,
                                                       end_time=valleys_datastream.data[index + 2].start_time,
                                                       sample=rsa_sample))
    if not rsa_datapoints:
        print('Error computing RSA!')
    rsa_ds.data = rsa_datapoints
    return rsa_ds


def rsa_feature_computation(valleys_datastream: DataStream,
                            rr_datastream: DataStream,
                            window_size: float,
                            window_offset: float
                            ):
    """

    :param rr_datastream:
    :param valleys_datastream:
    :param window_size:
    :param window_offset:
    :return: rsa_features computed over the given window
    """
    if (valleys_datastream or rr_datastream) is None:
        return None

    if (len(valleys_datastream.data) or len(rr_datastream)) == 0:
        return None

    rsa_datastream = compute_datastream_rsa(valleys_datastream, rr_datastream)

    # perform windowing of rsa_datastream

    window_data = window_sliding(rsa_datastream.data, window_size, window_offset)

    # initialize each RSA feature

    RSA_Quartile_Deviation = []
    RSA_Mean = []
    RSA_Median = []
    RSA_80thPercentile = []

    # iterate over each window and calculate features

    for key, value in window_data.items():
        starttime, endtime = key
        rsa = np.array([i.sample for i in value])
        RSA_Quartile_Deviation.append(DataPoint.from_tuple(start_time=starttime,
                                                           end_time=endtime,
                                                           sample=(0.5 * (
                                                               np.percentile(rsa, 75) - np.percentile(
                                                                   rsa,
                                                                   25)))))
        RSA_80thPercentile.append(
            DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.percentile(rsa, 80)))
        RSA_Mean.append(DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.mean(rsa)))

        RSA_Median.append(DataPoint.from_tuple(start_time=starttime, end_time=endtime, sample=np.median(rsa)))
    rsa_mean = DataStream.from_datastream([valleys_datastream])
    rsa_mean.data = RSA_Mean
    rsa_median = DataStream.from_datastream([valleys_datastream])
    rsa_median.data = RSA_Median
    rsa_quartile = DataStream.from_datastream([valleys_datastream])
    rsa_quartile.data = RSA_Quartile_Deviation
    rsa_80 = DataStream.from_datastream([valleys_datastream])
    rsa_80.data = RSA_80thPercentile
    return rsa_mean, rsa_median, rsa_quartile, rsa_80
