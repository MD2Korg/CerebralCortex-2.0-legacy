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
from cerebralcortex.data_processor.feature.feature_vector import generate_cStress_feature_vector
from cerebralcortex.data_processor.feature.rip import rip_cycle_feature_computation
from cerebralcortex.data_processor.feature.rip import window_rip
from cerebralcortex.data_processor.signalprocessing import rip
from cerebralcortex.data_processor.signalprocessing.accelerometer import accelerometer_features
from cerebralcortex.data_processor.signalprocessing.alignment import timestamp_correct, autosense_sequence_align
from cerebralcortex.data_processor.signalprocessing.dataquality import compute_outlier_ecg
from cerebralcortex.data_processor.signalprocessing.dataquality import ecg_data_quality
from cerebralcortex.data_processor.signalprocessing.dataquality import rip_data_quality
from cerebralcortex.data_processor.signalprocessing.ecg import compute_rr_intervals
from cerebralcortex.model_development.model_development import analyze_events_with_features


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

    frdd = argsrdd[0]
    if len(argsrdd) > 1:
        for erdd in range(len(argsrdd) - 1):
            frdd = frdd.join(argsrdd[erdd + 1])
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

    ecg_quality = ecg_corrected.map(lambda ds: (ds[0], ecg_data_quality(ds[1])))
    rip_quality = rip_corrected.map(lambda ds: (ds[0], rip_data_quality(ds[1])))

    accel_group = accelx_corrected.join(accely_corrected).join(accelz_corrected).map(fix_two_joins)
    accel = accel_group.map(lambda ds: (ds[0], autosense_sequence_align(datastreams=[ds[1][0], ds[1][1], ds[1][2]],
                                                                        sampling_frequency=accel_sampling_frequency)))

    # Accelerometer Feature Computation
    accel_features = accel.map(lambda ds: (ds[0], accelerometer_features(ds[1], window_length=10.0)))

    windowed_accel_features = accel_features.map(lambda ds: (ds[0], window_accel(ds[1], window_size=60)))


    rip_corrected_and_quality = rip_corrected.join(rip_quality)

    # rip features
    peak_valley = rip_corrected_and_quality.map(
        lambda ds: (ds[0], rip.compute_peak_valley(rip=ds[1][0], rip_quality=ds[1][1])))

    rip_cycle_features = peak_valley.map(lambda ds: (ds[0], rip_cycle_feature_computation(ds[1][0])))

    windowed_rip_features = rip_cycle_features.map(lambda ds: (ds[0], window_rip(peak_datastream=ds[1][0],
                                                                                 valley_datastream=ds[1][1],
                                                                                 inspiration_duration=ds[1][2],
                                                                                 expiration_duration=ds[1][3],
                                                                                 respiration_duration=ds[1][4],
                                                                                 inspiration_expiration_ratio=ds[1][5],
                                                                                 stretch=ds[1][6],
                                                                                 window_size=60,
                                                                                 window_offset=60)))

    ecg_corrected_and_quality = ecg_corrected.join(ecg_quality)

    # r-peak datastream computation
    ecg_rr_rdd = ecg_corrected_and_quality.map(lambda ds:
                                               (ds[0], compute_rr_intervals(ecg=ds[1][0], ecg_quality=ds[1][1],
                                                                            fs=ecg_sampling_frequency)))

    ecg_rr_quality = ecg_rr_rdd.map(lambda ds: (ds[0], compute_outlier_ecg(ds[1])))
    ecg_rr_and_quality = ecg_rr_rdd.join(ecg_rr_quality)

    windowed_ecg_features = ecg_rr_and_quality.map(
         lambda ds: (ds[0], ecg_feature_computation(datastream=ds[1][0], quality_datastream=ds[1][1],
                                                    window_size=60, window_offset=60)))

    peak_valley_rr_int = peak_valley.join(ecg_rr_rdd)  # TODO: Add RR_Quality here?

    # rsa_cycle_features = peak_valley_rr_int.map(
    #     lambda ds: (ds[0], compute_rsa_cycle_feature(valleys=ds[1][1], rr_int=ds[1][2])))
    # windowed_rsa_features = rsa_cycle_features.map(lambda ds: (ds[0], window_rsa(ds[0][0], window_size=60)))
    #
    # combined_features = windowed_accel_features.join(windowed_ecg_features).join(windowed_rip_features).join(
    #     windowed_rsa_features)
    # Fix joins here

    feature_vector_ecg_rip = windowed_ecg_features.map(lambda ds: (ds[0], generate_cStress_feature_vector(ds[1])))

    stress_ground_truth = rdd.map(lambda ds:(ds['participant'],ds['stress_marks']))

    feature_vector_with_ground_truth = feature_vector_ecg_rip.join(stress_ground_truth)

    train_data_with_ground_truth_and_subjects = feature_vector_with_ground_truth.map(lambda ds: analyze_events_with_features(participant=ds[0],stress_mark_stream=ds[1][1],feature_stream=ds[1][0]))


    return train_data_with_ground_truth_and_subjects  # Data stream with data points (ST, ET, [...37 values...])
