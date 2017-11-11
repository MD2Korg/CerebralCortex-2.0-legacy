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

from cerebralcortex.configuration import Configuration
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.battery_data_marker import battery_marker
from cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker.motionsense import wireless_disconnection as ms_wd
from cerebralcortex.data_processor.data_diagnostic.attachment_marker.motionsense import attachment_marker as ms_attachment_marker
from cerebralcortex.data_processor.data_diagnostic.packet_loss_marker import packet_loss_marker

class DiagnoseData:

    def __init__(self):
        self.CC = {}
        configuration_file = os.path.join(os.path.dirname(__file__), '../../../cerebralcortex.yml')
        self.CC["driver"] = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", time_zone="US/Central", load_spark=True)
        self.CC["worker"] = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", time_zone="US/Central", load_spark=False)

        # load data diagnostic configs
        self.config = Configuration(filepath="data_diagnostic_config.yml").config

    def one_participant_data(self, participant_id=None):
        # get all streams for a participant
        streams = self.CC["driver"].get_participant_streams(participant_id)
        self.diagnose_queue(participant_id, streams)

    def all_participants_data(self, study_name):
        # get all participants' name-ids
        participants = self.CC["driver"].get_all_participants(study_name)

        for participant in participants:
            # get all streams for a participant
            streams = self.CC["driver"].get_participant_streams(participant["identifier"])
            self.diagnose_queue(participant["identifier"], streams)

    def diagnose_queue(self, participant_id, streams):

        # phone battery
        if self.config["stream_names"]["phone_battery"] in streams:
            battery_marker(streams[self.config["stream_names"]["phone_battery"]]["identifier"], streams[self.config["stream_names"]["phone_battery"]]["name"], participant_id, self.config["stream_names"]["phone_battery_marker"], self.CC, self.config)
        # autosense battery
        if self.config["stream_names"]["autosense_battery"] in streams:
            battery_marker(streams[self.config["stream_names"]["autosense_battery"]]["identifier"], streams[self.config["stream_names"]["autosense_battery"]]["name"],  participant_id, self.config["stream_names"]["autosense_battery_marker"], self.CC, self.config)

        # TODO: Motionsense battery values are not available.
        # TODO: Uncomment following code when the motionsense battery values are available
        # if self.config["stream_names"]["motionsense_hrv_battery_right"] in streams:
        #     battery_marker(streams[self.config["stream_names"]["motionsense_hrv_battery_right"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_battery_right"]]["name"], participant_id,  self.config["stream_names"]["motionsense_hrv_battery_right_marker"], self.CC, self.config)
        # if self.config["stream_names"]["motionsense_hrv_battery_left"] in streams:
        #     battery_marker(streams[self.config["stream_names"]["motionsense_hrv_battery_left"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_battery_left"]]["name"], participant_id,  self.config["stream_names"]["motionsense_hrv_battery_left_marker"], self.CC, self.config)

        ### Sensor unavailable - wireless disconnection
        if self.config["stream_names"]["motionsense_hrv_accel_right"] in streams:
            phone_accel_stream_id = streams[self.config["stream_names"]["phone_accel"]]["identifier"]
            ms_wd(streams[self.config["stream_names"]["motionsense_hrv_accel_right"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_accel_right"]]["name"], participant_id, self.config["stream_names"]["motionsense_hrv_right_wireless_marker"], phone_accel_stream_id, self.CC, self.config)

        ### Attachment marker
        if self.config["stream_names"]["motionsense_hrv_led_quality_right"] in streams:
            ms_attachment_marker(streams[self.config["stream_names"]["motionsense_hrv_led_quality_right"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_led_quality_right"]]["name"], participant_id,  self.config["stream_names"]["motionsense_hrv_right_attachment_marker"], self.CC, self.config)
        if self.config["stream_names"]["motionsense_hrv_led_red_left"] in streams:
            ms_attachment_marker(streams[self.config["stream_names"]["motionsense_hrv_led_quality_left"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_led_quality_left"]]["name"], participant_id,  self.config["stream_names"]["motionsense_hrv_left_attachment_marker"], self.CC, self.config)

        ### Packet-loss marker
        if self.config["stream_names"]["motionsense_hrv_accel_right"] in streams:
            packet_loss_marker(streams[self.config["stream_names"]["motionsense_hrv_accel_right"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_accel_right"]]["name"], participant_id, self.config["stream_names"]["motionsense_hrv_accel_right_packetloss_marker"], self.CC, self.config)
        if self.config["stream_names"]["motionsense_hrv_accel_left"] in streams:
            packet_loss_marker(streams[self.config["stream_names"]["motionsense_hrv_accel_left"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_accel_left"]]["name"], participant_id, self.config["stream_names"]["motionsense_hrv_accel_left_packetloss_marker"], self.CC, self.config)
        if self.config["stream_names"]["motionsense_hrv_gyro_right"] in streams:
            packet_loss_marker(streams[self.config["stream_names"]["motionsense_hrv_gyro_right"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_gyro_right"]]["name"], participant_id, self.config["stream_names"]["motionsense_hrv_gyro_right_packetloss_marker"], self.CC, self.config)
        if self.config["stream_names"]["motionsense_hrv_gyro_left"] in streams:
            packet_loss_marker(streams[self.config["stream_names"]["motionsense_hrv_gyro_left"]]["identifier"], streams[self.config["stream_names"]["motionsense_hrv_gyro_left"]]["name"], participant_id, self.config["stream_names"]["motionsense_hrv_gyro_left_packetloss_marker"], self.CC, self.config)



if __name__ == '__main__':
    # run with one participant
    DiagnoseData().one_participant_data("cd7c2cd6-d0a3-4680-9ba2-0c59d0d0c684")

    # run for all the participants in a study
    #DiagnoseData().all_participants_data("mperf")

