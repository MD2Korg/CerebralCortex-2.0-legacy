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

import os

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.configuration import Configuration

# create and load CerebralCortex object and configs
configuration_file = os.path.join(os.path.dirname(__file__), '../../../cerebralcortex.yml')
CC = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", load_spark=False)

# output folder path
output_folder = "/home/ali/Desktop/DUMP/data/"
file_name = "data_diagnostic.csv"
# load data diagnostic configs
config = Configuration(filepath="data_diagnostic_config.yml").config


def all_participants_data(study_name):
    # get all participants' name-ids
    write_to_file_header()
    participants = CC.get_all_participants(study_name)
    for participant in participants:
        missing_streams(participant["identifier"])


def missing_streams(owner_id):
    # following streams shall be present for all participants
    required_streams = []
    required_streams.append(config["stream_names"]["phone_battery"])
    required_streams.append(config["stream_names"]["phone_accel"])
    required_streams.append(config["stream_names"]["phone_gyro"])
    required_streams.append(config["stream_names"]["motionsense_hrv_accel_right"])
    required_streams.append(config["stream_names"]["motionsense_hrv_gyro_right"])
    required_streams.append(config["stream_names"]["motionsense_hrv_led_right"])
    required_streams.append(config["stream_names"]["motionsense_hrv_accel_left"])
    required_streams.append(config["stream_names"]["motionsense_hrv_gyro_right"])
    required_streams.append(config["stream_names"]["motionsense_hrv_led_left"])

    available_streams = CC.get_participant_streams(owner_id)
    if (len(available_streams)) > 0:
        for required_stream in required_streams:
            if required_stream in available_streams:
                total_days = available_streams[required_stream]["end_time"] - available_streams[required_stream][
                    "start_time"]
                write_to_file(owner_id, required_stream, available_streams[required_stream]["start_time"],
                              available_streams[required_stream]["end_time"], total_days)
            else:
                total_days = get_start_end_time(available_streams, "end_time") - get_start_end_time(available_streams,
                                                                                                    "start_time")
                write_to_file(owner_id, required_stream, "MISSING", "MISSING", total_days)


def get_start_end_time(available_streams, start_end):
    _time = []
    for key, available_stream in available_streams.items():
        _time.append(available_stream[start_end])
    return min(_time)


def write_to_file(owner_id, stream_name, start_time, end_time, total_days):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    with open(output_folder + file_name, 'a+') as fp:
        fp.write(str(owner_id) + ", " + str(stream_name) + ", " + str(start_time) + ", " + str(end_time) + ", " + str(
            total_days).replace("day,", "-").replace("days,", "-") + "\n")


def write_to_file_header():
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    with open(output_folder + file_name, 'a+') as fp:
        fp.write("PARTICIPANT ID, STREAM NAME, START TIME, END TIME, EXPECTED TIME (Day(s)-Time)\n")


def gen_test_data(id, start_time, end_time, sample):
    for i in range(1, 10):
        write_to_file("0000cc98-2e38-3c34-800b-7048d6cdf09f", )


if __name__ == '__main__':
    # run for all the participants in a study
    all_participants_data("mperf")
