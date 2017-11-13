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
from cerebralcortex.data_processor.data_diagnostic.attachment_marker.motionsense import \
    attachment_marker as ms_attachment_marker
from cerebralcortex.data_processor.data_diagnostic.battery_data_marker import battery_marker
from cerebralcortex.data_processor.data_diagnostic.packet_loss_marker import packet_loss_marker
from cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker.motionsense import \
    wireless_disconnection as ms_wd

CC = {}
configuration_file = os.path.join(os.path.dirname(__file__), '../../../cerebralcortex.yml')
CC["driver"] = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", load_spark=True)
CC["worker"] = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", load_spark=False)

# load data diagnostic configs
config = Configuration(filepath="data_diagnostic_config.yml").config


def one_participant_data(participant_ids=None):
    # get all streams for a participant
    if len(participant_ids)>0:
        streams = CC["driver"].get_participant_streams(participant_ids)
        diagnose_queue(participant_ids, streams)
    else:
        print("No participant is selected")


def all_participants_data(study_name):
    # get all participants' name-ids
    CC_worker = CC["worker"]
    participants = CC["driver"].get_all_participants(study_name)
    if len(participants)>0:
        participants_rdd = CC["driver"].sc.parallelize(participants)
        results = participants_rdd.map(
            lambda participant: diagnose_queue(participant["identifier"], CC_worker, config))
        results.count()
    else:
        print("No participant is selected")


def diagnose_queue(participant_id, CC, config):
    pass
    streams = CC.get_participant_streams(participant_id)

    # phone battery
    if config["stream_names"]["phone_battery"] in streams:
        battery_marker(streams[config["stream_names"]["phone_battery"]]["identifier"],
                       streams[config["stream_names"]["phone_battery"]]["name"], participant_id,
                       config["stream_names"]["phone_battery_marker"], CC, config)
    # autosense battery
    if config["stream_names"]["autosense_battery"] in streams:
        battery_marker(streams[config["stream_names"]["autosense_battery"]]["identifier"],
                       streams[config["stream_names"]["autosense_battery"]]["name"], participant_id,
                       config["stream_names"]["autosense_battery_marker"], CC, config)

    # TODO: Motionsense battery values are not available.
    # TODO: Uncomment following code when the motionsense battery values are available
    # if config["stream_names"]["motionsense_hrv_battery_right"] in streams:
    #     battery_marker(streams[config["stream_names"]["motionsense_hrv_battery_right"]]["identifier"], streams[config["stream_names"]["motionsense_hrv_battery_right"]]["name"], participant_id,  config["stream_names"]["motionsense_hrv_battery_right_marker"], CC, config)
    # if config["stream_names"]["motionsense_hrv_battery_left"] in streams:
    #     battery_marker(streams[config["stream_names"]["motionsense_hrv_battery_left"]]["identifier"], streams[config["stream_names"]["motionsense_hrv_battery_left"]]["name"], participant_id,  config["stream_names"]["motionsense_hrv_battery_left_marker"], CC, config)

    ### Sensor unavailable - wireless disconnection
    if config["stream_names"]["motionsense_hrv_accel_right"] in streams:
        phone_accel_stream_id = streams[config["stream_names"]["phone_accel"]]["identifier"]
        ms_wd(streams[config["stream_names"]["motionsense_hrv_accel_right"]]["identifier"],
              streams[config["stream_names"]["motionsense_hrv_accel_right"]]["name"], participant_id,
              config["stream_names"]["motionsense_hrv_right_wireless_marker"], phone_accel_stream_id, CC, config)

    ### Attachment marker
    if config["stream_names"]["motionsense_hrv_led_quality_right"] in streams:
        ms_attachment_marker(streams[config["stream_names"]["motionsense_hrv_led_quality_right"]]["identifier"],
                             streams[config["stream_names"]["motionsense_hrv_led_quality_right"]]["name"],
                             participant_id, config["stream_names"]["motionsense_hrv_right_attachment_marker"], CC,
                             config)
    if config["stream_names"]["motionsense_hrv_led_quality_left"] in streams:
        ms_attachment_marker(streams[config["stream_names"]["motionsense_hrv_led_quality_left"]]["identifier"],
                             streams[config["stream_names"]["motionsense_hrv_led_quality_left"]]["name"],
                             participant_id, config["stream_names"]["motionsense_hrv_left_attachment_marker"], CC,
                             config)

    ### Packet-loss marker
    if config["stream_names"]["motionsense_hrv_accel_right"] in streams:
        packet_loss_marker(streams[config["stream_names"]["motionsense_hrv_accel_right"]]["identifier"],
                           streams[config["stream_names"]["motionsense_hrv_accel_right"]]["name"], participant_id,
                           config["stream_names"]["motionsense_hrv_accel_right_packetloss_marker"], CC, config)
    if config["stream_names"]["motionsense_hrv_accel_left"] in streams:
        packet_loss_marker(streams[config["stream_names"]["motionsense_hrv_accel_left"]]["identifier"],
                           streams[config["stream_names"]["motionsense_hrv_accel_left"]]["name"], participant_id,
                           config["stream_names"]["motionsense_hrv_accel_left_packetloss_marker"], CC, config)
    if config["stream_names"]["motionsense_hrv_gyro_right"] in streams:
        packet_loss_marker(streams[config["stream_names"]["motionsense_hrv_gyro_right"]]["identifier"],
                           streams[config["stream_names"]["motionsense_hrv_gyro_right"]]["name"], participant_id,
                           config["stream_names"]["motionsense_hrv_gyro_right_packetloss_marker"], CC, config)
    if config["stream_names"]["motionsense_hrv_gyro_left"] in streams:
        packet_loss_marker(streams[config["stream_names"]["motionsense_hrv_gyro_left"]]["identifier"],
                           streams[config["stream_names"]["motionsense_hrv_gyro_left"]]["name"], participant_id,
                           config["stream_names"]["motionsense_hrv_gyro_left_packetloss_marker"], CC, config)


if __name__ == '__main__':
    # run with one participant
    # DiagnoseData().one_participant_data(["cd7c2cd6-d0a3-4680-9ba2-0c59d0d0c684"])

    # run for all the participants in a study
    all_participants_data("mperf")
