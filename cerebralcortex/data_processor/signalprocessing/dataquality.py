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

from cerebralcortex.kernel.datatypes.annotationstream import AnnotationStream
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.data_processor.signalprocessing.ECGqualitycalculation import compute_dataquality_ecg
import matplotlib.pyplot as plt


def ECGDataQuality(datastream: DataStream,
                   windowsize: float = 2.0,
                   acceptable_outlier_percent: float = .34,
                   outlier_threshold_high: float = .9769,
                   outlier_threshold_low: float = .004884,
                   ecg_threshold_band_loose: float = .01148,
                   ecg_threshold_slope: float = .02443,
                   buffer_length: int = 3) -> DataStream:

    quality_stream = DataStream.from_datastream(input_streams=[datastream])
    print(len(datastream.data))
    window_data = window(datastream.data, window_size=windowsize)

    ecg_quality = []
    ecg_range = []
    for key, data in window_data.items():
        result = compute_dataquality_ecg(data,ecg_range,acceptable_outlier_percent,outlier_threshold_high,outlier_threshold_low,ecg_threshold_band_loose,ecg_threshold_slope,buffer_length)
        if len(ecg_quality)==0:
            ecg_quality.append(DataPoint.from_tuple(data[0].start_time, result, data[-1].start_time))
        else:
            if ecg_quality[-1].sample == result:
                new_point = DataPoint.from_tuple(ecg_quality[-1].start_time, result, data[-1].start_time)
                ecg_quality[-1] = new_point
            else:
                ecg_quality.append(DataPoint.from_tuple(data[0].start_time, result, data[-1].start_time))
        x=[i.start_time for i in data]
        y=[i.sample for i in data]
        # if result == 0:
        # plt.figure()
        # plt.plot(x, y)
        # plt.show()
    quality_stream.data = ecg_quality

    return quality_stream



def RIPDataQuality(datastream: DataStream,
                   windowsize: float = 5.0,
                   bufferLength=5,
                   acceptableOutlierPercent=50,
                   outlierThresholdHigh=4500,
                   outlierThresholdLow=20,
                   badSegmentThreshod=2,
                   ripBandOffThreshold=20,
                   ripBandLooseThreshold=150) -> DataStream:
    """

    :param datastream:
    :param windowsize:
    :param bufferLength:
    :param acceptableOutlierPercent:
    :param outlierThresholdHigh:
    :param outlierThresholdLow:
    :param badSegmentThreshod:
    :param ripBandOffThreshold:
    :param ripBandLooseThreshold:
    :return:
    """
    # windows = window(datastream.datapoints, window_size=windowsize)

    # TODO: Do something with windows here

    result = DataStream.from_datastream(input_streams=[datastream])

    # # Do something here for data quality
    # ripQuality = []
    # for i in range(1, 10):
    #     ripQuality.append(Span(result.getID(),
    #                            starttime=datetime.now(),
    #                            endtime=datetime.now(),
    #                            label=DataQuality.GOOD))
    #
    # result.set_spans(ripQuality)

    return result
