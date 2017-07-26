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
from typing import Tuple

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def rip_feature_computation(peaks_datastream: DataStream,
                            valleys_datastream: DataStream) -> Tuple[DataStream]:
    """
    Respiration Feature Implementation. The respiration feature values are
    derived from the following paper:
    'puffMarker: a multi-sensor approach for pinpointing the timing of first lapse in smoking cessation'


    Removed due to lack of current use in the implementation
    roc_max = []  # 8. ROC_MAX = max(sample[j]-sample[j-1])
    roc_min = []  # 9. ROC_MIN = min(sample[j]-sample[j-1])


    :param peaks_datastream: DataStream
    :param valleys_datastream: DataStream
    :return: RIP Feature DataStreams
    """

    # TODO: This needs fixed to prevent crashing the execution pipeline
    if peaks_datastream is None or valleys_datastream is None:
        return None

    # TODO: This needs fixed to prevent crashing the execution pipeline
    if len(peaks_datastream.data) == 0 or len(valleys_datastream.data) == 0:
        return None

    inspiration_duration = []  # 1 Inhalation duration
    expiration_duration = []  # 2 Exhalation duration
    respiration_duration = []  # 3 Respiration duration
    inspiration_expiration_ratio = []  # 4 Inhalation and Exhalation ratio
    stretch = []  # 5 Stretch
    upper_stretch = []  # 6. Upper portion of the stretch calculation
    lower_stretch = []  # 7. Lower portion of the stretch calculation
    delta_previous_inspiration_duration = []  # 10. BD_INSP = INSP(i)-INSP(i-1)
    delta_previous_expiration_duration = []  # 11. BD_EXPR = EXPR(i)-EXPR(i-1)
    delta_previous_respiration_duration = []  # 12. BD_RESP = RESP(i)-RESP(i-1)
    delta_previous_stretch_duration = []  # 14. BD_Stretch= Stretch(i)-Stretch(i-1)
    delta_next_inspiration_duration = []  # 19. FD_INSP = INSP(i)-INSP(i+1)
    delta_next_expiration_duration = []  # 20. FD_EXPR = EXPR(i)-EXPR(i+1)
    delta_next_respiration_duration = []  # 21. FD_RESP = RESP(i)-RESP(i+1)
    delta_next_stretch_duration = []  # 23. FD_Stretch= Stretch(i)-Stretch(i+1)
    neighbor_ratio_expiration_duration = []  # 29. D5_EXPR(i) = EXPR(i) / avg(EXPR(i-2)...EXPR(i+2))
    neighbor_ratio_stretch_duration = []  # 32. D5_Stretch = Stretch(i) / avg(Stretch(i-2)...Stretch(i+2))

    valleys = valleys_datastream.data
    peaks = peaks_datastream.data[:-1]

    for i, peak in enumerate(peaks):
        valley_start_time = valleys[i].start_time

        delta = peak.start_time - valleys[i].start_time
        inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta.total_seconds()))

        delta = valleys[i + 1].start_time - peak.start_time
        expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta.total_seconds()))

        delta = valleys[i + 1].start_time - valley_start_time
        respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta.total_seconds()))

        ratio = (peak.start_time - valley_start_time) / (valleys[i + 1].start_time - peak.start_time)
        inspiration_expiration_ratio.append(DataPoint.from_tuple(start_time=valley_start_time, sample=ratio))

        value = peak.sample - valleys[i + 1].sample
        stretch.append(DataPoint.from_tuple(start_time=valley_start_time, sample=value))

        # upper_stretch.append(DataPoint.from_tuple(start_time=valley_start_time, sample=(peak.sample - valleys[i + 1][1]) / 2))  # TODO: Fix this by adding a tracking moving average and compute upper stretch from this to the peak and lower from this to the valley
        # lower_stretch.append(DataPoint.from_tuple(start_time=valley_start_time, sample=(-peak.sample + valleys[i + 1][1]) / 2))  # TODO: Fix this by adding a tracking moving average and compute upper stretch from this to the peak and lower from this to the valley

    for i in range(len(inspiration_duration)):
        valley_start_time = valleys[i].start_time
        if i == 0:  # Edge case
            delta_previous_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_previous_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_previous_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_previous_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
        else:
            delta = inspiration_duration[i].sample - inspiration_duration[i - 1].sample
            delta_previous_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = expiration_duration[i].sample - expiration_duration[i - 1].sample
            delta_previous_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = respiration_duration[i].sample - respiration_duration[i - 1].sample
            delta_previous_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = stretch[i].sample - stretch[i - 1].sample
            delta_previous_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

        if i == len(inspiration_duration) - 1:
            delta_next_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_next_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_next_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
            delta_next_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=0.0))
        else:
            delta = inspiration_duration[i].sample - inspiration_duration[i + 1].sample
            delta_next_inspiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = expiration_duration[i].sample - expiration_duration[i + 1].sample
            delta_next_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = respiration_duration[i].sample - respiration_duration[i + 1].sample
            delta_next_respiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

            delta = stretch[i].sample - stretch[i + 1].sample
            delta_next_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=delta))

        stretch_average = 0
        expiration_average = 0
        count = 0.0
        for j in [-2, -1, 1, 2]:
            if i + j < 0 or i + j >= len(inspiration_duration):
                continue
            stretch_average += stretch[i + j].sample
            expiration_average += expiration_duration[i + j].sample
            count += 1

        stretch_average /= count
        expiration_average /= count

        ratio = stretch[i].sample / stretch_average
        neighbor_ratio_stretch_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=ratio))

        ratio = expiration_duration[i].sample / expiration_average
        neighbor_ratio_expiration_duration.append(DataPoint.from_tuple(start_time=valley_start_time, sample=ratio))

    # Begin assembling datastream for output
    inspiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    inspiration_duration_datastream.data = inspiration_duration

    expiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    expiration_duration_datastream.data = expiration_duration

    respiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    respiration_duration_datastream.data = respiration_duration

    inspiration_expiration_ratio_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    inspiration_expiration_ratio_datastream.data = inspiration_expiration_ratio

    stretch_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    stretch_datastream.data = stretch

    upper_stretch_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    upper_stretch_datastream.data = upper_stretch

    lower_stretch_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    lower_stretch_datastream.data = lower_stretch

    delta_previous_inspiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_previous_inspiration_duration_datastream.data = delta_previous_inspiration_duration

    delta_previous_expiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_previous_expiration_duration_datastream.data = delta_previous_expiration_duration

    delta_previous_respiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_previous_respiration_duration_datastream.data = delta_previous_respiration_duration

    delta_previous_stretch_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_previous_stretch_duration_datastream.data = delta_previous_stretch_duration

    delta_next_inspiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_next_inspiration_duration_datastream.data = delta_next_inspiration_duration

    delta_next_expiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_next_expiration_duration_datastream.data = delta_next_expiration_duration

    delta_next_respiration_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_next_respiration_duration_datastream.data = delta_next_respiration_duration

    delta_next_stretch_duration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    delta_next_stretch_duration_datastream.data = delta_next_stretch_duration

    neighbor_ratio_expiration_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    neighbor_ratio_expiration_datastream.data = neighbor_ratio_expiration_duration

    neighbor_ratio_stretch_datastream = DataStream.from_datastream([peaks_datastream, valleys_datastream])
    neighbor_ratio_stretch_datastream.data = neighbor_ratio_stretch_duration

    return inspiration_duration_datastream, \
           expiration_duration_datastream, \
           respiration_duration_datastream, \
           inspiration_expiration_ratio_datastream, \
           stretch_datastream, \
           upper_stretch_datastream, \
           lower_stretch_datastream, \
           delta_previous_inspiration_duration_datastream, \
           delta_previous_expiration_duration_datastream, \
           delta_previous_respiration_duration_datastream, \
           delta_previous_stretch_duration_datastream, \
           delta_next_inspiration_duration_datastream, \
           delta_next_expiration_duration_datastream, \
           delta_next_respiration_duration_datastream, \
           delta_next_stretch_duration_datastream, \
           neighbor_ratio_expiration_datastream, \
           neighbor_ratio_stretch_datastream
