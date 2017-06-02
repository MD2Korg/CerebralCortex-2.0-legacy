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
from pyspark import RDD

from cerebralcortex.data_processor.feature.ecg import ecg_feature_computation
from cerebralcortex.data_processor.feature.rip import rip_feature_computation
from cerebralcortex.data_processor.signalprocessing import rip
from cerebralcortex.data_processor.signalprocessing.accelerometer import accelerometer_features
from cerebralcortex.data_processor.signalprocessing.alignment import timestamp_correct, autosense_sequence_align
from cerebralcortex.data_processor.signalprocessing.ecg import compute_rr_intervals
from cerebralcortex.data_processor.signalprocessing.dataquality import ECGDataQuality
from cerebralcortex.data_processor.signalprocessing.dataquality import RIPDataQuality
from cerebralcortex.data_processor.signalprocessing.dataquality import compute_outlier_ecg

def fix_two_joins(nested_data):
    key = nested_data[0]
    base_value = nested_data[1][0]
    new_value = (nested_data[1][1],)
    return key, base_value + new_value

def join_feature_vector(*argsrdd):
    """

    :param argsrdd: Input feature dataStreams
    :return: Joined feature vector
    """

    frdd=argsrdd[0]
    if len(argsrdd) > 1:
        for erdd in range(len(argsrdd)-1):
            frdd = frdd.join(argsrdd[erdd+1])
        return frdd
    else:
        return frdd

def cStress(rdd: RDD) -> RDD:

    # TODO: TWH Temporary
    ecg_sampling_frequency = 64.0
    rip_sampling_frequency = 64.0
    accel_sampling_frequency = 64.0 / 6.0



    # Timestamp correct datastreams
    ecg_corrected = rdd.map(lambda ds: (
    ds['participant'], timestamp_correct(datastream=ds['ecg'], sampling_frequency=ecg_sampling_frequency)))
    rip_corrected = rdd.map(lambda ds: (
    ds['participant'], timestamp_correct(datastream=ds['rip'], sampling_frequency=rip_sampling_frequency)))

    accelx_corrected = rdd.map(lambda ds: (
    ds['participant'], timestamp_correct(datastream=ds['accelx'], sampling_frequency=accel_sampling_frequency)))
    accely_corrected = rdd.map(lambda ds: (
    ds['participant'], timestamp_correct(datastream=ds['accely'], sampling_frequency=accel_sampling_frequency)))
    accelz_corrected = rdd.map(lambda ds: (
    ds['participant'], timestamp_correct(datastream=ds['accelz'], sampling_frequency=accel_sampling_frequency)))

    ecg_quality = ecg_corrected.map(lambda ds: (ds[0], ECGDataQuality(ds[1])))
    ecg_quality.take(1)

    rip_quality = rip_corrected.map(lambda ds: (ds[0], RIPDataQuality(ds[1])))
    rip_quality.take(1)

    accel_group = accelx_corrected.join(accely_corrected).join(accelz_corrected).map(fix_two_joins)
    accel = accel_group.map(lambda ds: (ds[0], autosense_sequence_align(datastreams=[ds[1][0], ds[1][1], ds[1][2]],
                                                                        sampling_frequency=accel_sampling_frequency)))

    # Accelerometer Feature Computation
    accel_features = accel.map(lambda ds: (ds[0], accelerometer_features(ds[1], window_length=10.0)))

    rip_corrected_and_quality = rip_corrected.join(rip_quality)


    # rip features
    peak_valley = rip_corrected_and_quality.map(lambda ds: (ds[0], rip.compute_peak_valley(rip=ds[1][0],rip_quality=ds[1][1])))
    peak_valley.take(1)
    rip_features = peak_valley.map(lambda ds: (ds[0], rip_feature_computation(ds[1][0], ds[1][1])))

    ecg_corrected_and_quality = ecg_corrected.join(ecg_quality)

    # r-peak datastream computation
    ecg_rr_rdd = ecg_corrected_and_quality.map(lambda ds:
                                               (ds[0], compute_rr_intervals(ecg=ds[1][0], ecg_quality=ds[1][1], fs=ecg_sampling_frequency)))


    ecg_rr_quality = ecg_rr_rdd.map(lambda ds: (ds[0],compute_outlier_ecg(ds[1])))

    # ecg_features = ecg_rr_rdd.map(lambda ds: (ds[0], ecg_feature_computation(ds[1], window_size=60, window_offset=60)))


    #computer cStress feature vector
    f_vector = join_feature_vector(ecg_features, rip_features, accel_features)
    return f_vector