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
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.datatypes.datapoint import DataPoint

def classify_data_points(data:list,
                         ecg_or_rip:object,
                         threshold_slope: float,
                         outlier_threshold_high: float = .9769,
                         outlier_threshold_low: float = .004884,
                         ADC_range: int = 4095):
    no_of_outliers = 0
    max_value= data[0].sample
    min_value= data[0].sample
    for i,item in enumerate(data):
        previous_index = (len(data)-1) if i==0 else i-1
        next_index = 0 if (i==len(data)-1) else i+1
        stuck =(item.sample==data[previous_index].sample) and (item.sample==data[next_index].sample)
        flip = abs(item.sample-data[previous_index].sample)> outlier_threshold_high*ADC_range  or abs(item.sample-data[next_index].sample)> outlier_threshold_high*ADC_range
        if ecg_or_rip=="ecg":
            disc = abs(item.sample-data[previous_index].sample)> threshold_slope*ADC_range  and abs(item.sample-data[next_index].sample)> threshold_slope*ADC_range
        else:
            disc = abs(item.sample-data[previous_index].sample)> threshold_slope*ADC_range  or abs(item.sample-data[next_index].sample)> threshold_slope*ADC_range

        if disc:
            no_of_outliers+=1
        elif stuck:
            no_of_outliers+=1
        elif flip:
            no_of_outliers+=1
        elif item.sample >= outlier_threshold_high*ADC_range:
            no_of_outliers+=1
        elif item.sample <= outlier_threshold_low*ADC_range:
            no_of_outliers+=1
        else:
            if item.sample > max_value:
                max_value = item.sample
            if item.sample < min_value:
                min_value= item.sample
    return no_of_outliers,max_value,min_value




def classify_segment(data:list,
                     no_of_outliers:int,
                     acceptable_outlier_percent: float = .34)->int:
    if no_of_outliers > acceptable_outlier_percent * len(data):
        return 1
    else:
        return 0



def classify_buffer(range_values:list,
                    threshold_band_loose:float,
                    buffer_length:int = 3,
                    ADC_range: int = 4095)-> int:
    amplitude_small = 0
    if len(range_values) < buffer_length:
        return amplitude_small
    else:
        for item in range_values:
            if item.sample < threshold_band_loose*ADC_range:
                amplitude_small+=1
        return amplitude_small





def compute_dataquality(data:list,
                        ecg_range:list,
                        ecg_or_rip: object ,
                        threshold_band_loose: float,
                        threshold_slope: float,
                        acceptable_outlier_percent: float = .34,
                        outlier_threshold_high: float = .9769,
                        outlier_threshold_low: float = .004884,
                        buffer_length: int = 3,
                        ADC_range: int = 4095)->int:
    no_of_outliers,max_value,min_value = classify_data_points(data,ecg_or_rip,threshold_slope,outlier_threshold_high,outlier_threshold_low)

    segment_class = classify_segment(data,no_of_outliers,acceptable_outlier_percent)

    ecg_range.append(DataPoint.from_tuple(data[0].start_time, max_value-min_value))

    range_values = ecg_range[(-1)*buffer_length:]
    amplitude_small = classify_buffer(range_values, threshold_band_loose, buffer_length)

    if segment_class == 1:
        return 1
    elif 2 * amplitude_small > buffer_length:
        return 1
    elif (max_value-min_value) <= threshold_band_loose *ADC_range:
        return 1
    return 0




def ECGDataQuality(datastream: DataStream,
                   windowsize: float = 2.0,
                   acceptable_outlier_percent: float = .34,
                   outlier_threshold_high: float = .9769,
                   outlier_threshold_low: float = .004884,
                   ecg_threshold_band_loose: float = .01148,
                   ecg_threshold_slope: float = .02443,
                   buffer_length: int = 3) -> DataStream:

    ecg_quality_stream = DataStream.from_datastream(input_streams=[datastream])
    window_data = window(datastream.data, window_size=windowsize)

    ecg_quality = []
    ecg_range = []
    for key, data in window_data.items():
        result = compute_dataquality(data,ecg_range,"ecg",ecg_threshold_band_loose,ecg_threshold_slope,acceptable_outlier_percent,outlier_threshold_high,outlier_threshold_low,buffer_length)
        if len(ecg_quality)==0:
            ecg_quality.append(DataPoint.from_tuple(data[0].start_time, result, data[-1].start_time))
        else:
            if ecg_quality[-1].sample == result:
                new_point = DataPoint.from_tuple(ecg_quality[-1].start_time, result, data[-1].start_time)
                ecg_quality[-1] = new_point
            else:
                ecg_quality.append(DataPoint.from_tuple(data[0].start_time, result, data[-1].start_time))

    ecg_quality_stream.data = ecg_quality

    return ecg_quality_stream


def RIPDataQuality(datastream: DataStream,
                   windowsize: float = 4.0,
                   acceptable_outlier_percent: float = .34,
                   outlier_threshold_high: float = .9769,
                   outlier_threshold_low: float = .004884,
                   rip_threshold_band_loose: float = .0428,
                   rip_threshold_slope: float = .0733,
                   buffer_length: int = 3) -> DataStream:

    rip_quality_stream = DataStream.from_datastream(input_streams=[datastream])
    window_data = window(datastream.data, window_size=windowsize)

    rip_quality = []
    rip_range = []
    for key, data in window_data.items():
        result = compute_dataquality(data,rip_range,"rip",rip_threshold_band_loose,rip_threshold_slope,acceptable_outlier_percent,outlier_threshold_high,outlier_threshold_low,buffer_length)
        if len(rip_quality)==0:
            rip_quality.append(DataPoint.from_tuple(data[0].start_time, result, data[-1].start_time))
        else:
            if rip_quality[-1].sample == result:
                new_point = DataPoint.from_tuple(rip_quality[-1].start_time, result, data[-1].start_time)
                rip_quality[-1] = new_point
            else:
                rip_quality.append(DataPoint.from_tuple(data[0].start_time, result, data[-1].start_time))

    rip_quality_stream.data = rip_quality

    return rip_quality_stream



