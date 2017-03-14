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


def fix_two_joins(nested_data):
    key = nested_data[0]
    base_value = nested_data[1][0]
    new_value = (nested_data[1][1],)
    return key, base_value + new_value


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

    accel_group = accelx_corrected.join(accely_corrected).join(accelz_corrected).map(fix_two_joins)
    accel = accel_group.map(lambda ds: (ds[0], autosense_sequence_align(datastreams=[ds[1][0], ds[1][1], ds[1][2]],
                                                                        sampling_frequency=accel_sampling_frequency)))

    # Accelerometer Feature Computation
    accel_features = accel.map(lambda ds: (ds[0], accelerometer_features(ds[1], window_length=10.0)))

    # rip features
    peak_valley = rip_corrected.map(lambda ds: (ds[0], rip.compute_peak_valley(rip=ds[1])))
    rip_features = peak_valley.map(lambda ds: (ds[0], rip_feature_computation(ds[1][0], ds[1][1])))

    # r-peak datastream computation
    ecg_rr_rdd = ecg_corrected.map(lambda ds: (ds[0], compute_rr_intervals(ds[1], ecg_sampling_frequency)))
    ecg_features = ecg_rr_rdd.map(lambda ds: (ds[0], ecg_feature_computation(ds[1], window_size=60, window_offset=60)))

    # return rip_features.join(ecg_features).join(accel_features).map(fix_two_joins)
    return ecg_features
