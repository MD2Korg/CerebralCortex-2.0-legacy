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

from cerebralcortex.data_processor.feature.ecg import ecg_feature_computation
from cerebralcortex.data_processor.signalprocessing import rip
from cerebralcortex.data_processor.signalprocessing.accelerometer import accelerometer_features
from cerebralcortex.data_processor.signalprocessing.alignment import timestamp_correct, \
    timestamp_correct_and_sequence_align
from cerebralcortex.data_processor.signalprocessing.dataquality import ECGDataQuality, RIPDataQuality
from cerebralcortex.data_processor.signalprocessing.ecg import compute_rr_intervals
from cerebralcortex.kernel.datatypes.datastream import DataStream


def cStress(raw_ecg: DataStream,
            raw_rip: DataStream,
            raw_accel_x: DataStream,
            raw_accel_y: DataStream,
            raw_accel_z: DataStream) -> DataStream:
    """

    :return:
    :param raw_ecg:
    :param raw_rip:
    :param raw_accel_x:
    :param raw_accel_y:
    :param raw_accel_z:
    """

    # TODO: TWH Temporary
    ecg_sampling_frequency = 64.0
    rip_sampling_frequency = 64.0 / 3.0
    accel_sampling_frequency = 64.0 / 6.0

    # Timestamp correct datastreams
    ecg_corrected = timestamp_correct(datastream=raw_ecg, sampling_frequency=ecg_sampling_frequency)
    rip_corrected = timestamp_correct(datastream=raw_rip, sampling_frequency=rip_sampling_frequency)
    accel = timestamp_correct_and_sequence_align(datastream_array=[raw_accel_x, raw_accel_y, raw_accel_z],
                                                 sampling_frequency=accel_sampling_frequency)

    # ECG and RIP signal morphology data quality
    ecg_data_quality = ECGDataQuality(ecg_corrected,
                                      windowsize=5.0,  # What does windowsize mean here?
                                      bufferLength=3,
                                      acceptableOutlierPercent=50,
                                      outlierThresholdHigh=4500,
                                      outlierThresholdLow=20,
                                      badSegmentThreshod=2,
                                      ecgBandLooseThreshold=47)
    # ecg_corrected.add_span_stream(ecg_data_quality)

    rip_data_quality = RIPDataQuality(rip_corrected,
                                      windowsize=5.0,  # What does windowsize mean here?
                                      bufferLength=5,
                                      acceptableOutlierPercent=50,
                                      outlierThresholdHigh=4500,
                                      outlierThresholdLow=20,
                                      badSegmentThreshod=2,
                                      ripBandOffThreshold=20,
                                      ripBandLooseThreshold=150)
    # rip_corrected.add_span_stream(rip_data_quality)

    # Accelerometer Feature Computation
    accelerometer_magnitude, accelerometer_win_mag_deviations, accel_activity = accelerometer_features(accel)

    # rip features
    rip_peak_datastream, rip_valley_datastream = rip.compute_peak_valley(rip=rip_corrected)

    # r-peak datastream computation
    ecg_rr_datastream = compute_rr_intervals(ecg_corrected , ecg_sampling_frequency)

    ecg_features = ecg_feature_computation(ecg_rr_datastream, window_size=60, window_offset=60)
    print(len(ecg_features))
    print(len(rip_peak_datastream), len(ecg_rr_datastream))


    # TODO: TWH Fix when feature vector result is available
    return raw_ecg
