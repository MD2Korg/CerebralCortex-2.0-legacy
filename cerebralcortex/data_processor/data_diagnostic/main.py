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
from cerebralcortex.data_processor.data_diagnostic.attachment_marker import attachment_marker
from cerebralcortex.data_processor.data_diagnostic.battery_data_marker import battery_marker
from cerebralcortex.data_processor.data_diagnostic.packet_loss_marker import packet_loss_marker
from cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker import *


class DiagnoseData:
    @classmethod
    def diagnose_data(cls):

        """
        This method is just an example of how to call diagnostic module
        """
        configuration_file = os.path.join(os.path.dirname(__file__), '../../../cerebralcortex.yml')
        CC = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", time_zone="US/Central")

        configuration = Configuration(filepath="data_diagnostic_config.yml").config
        stream_name = configuration["sensor_types"]["autosense_rip"]

        cls.diagnose("de5b4a7d-ba1b-44c4-b55e-cd0ca7487734", CC, configuration, stream_name)

    @staticmethod
    def diagnose(owner_id, CC_obj, config, stream_name, start_time=None, end_time=None):
        """

        :param owner_id:
        :param CC_obj:
        :param stream_name: only three types of sensors (i.e., rip, ecg, motionsense)
        :param start_time:
        :param end_time:
        """
        main_stream_id = CC_obj.get_stream_ids_of_owner(owner_id, stream_name)

        # phone battery
        phone_battery_stream_id = CC_obj.get_stream_ids_of_owner(owner_id,
                                                                 config["sensor_types"][
                                                                     "phone_battery"])
        battery_marker(phone_battery_stream_id, CC_obj, config, start_time=start_time, end_time=end_time)

        if stream_name == config["sensor_types"]["autosense_rip"] or stream_name == config["sensor_types"][
            "autosense_ecg"]:
            autosense_battery_stream_id = CC_obj.get_stream_ids_of_owner(owner_id,
                                                                         config["sensor_types"][
                                                                             "autosense_battery"])
            battery_marker(autosense_battery_stream_id, CC_obj, config, start_time=start_time, end_time=end_time)
        elif stream_name == config["sensor_types"]["motionsense_accel"]:
            motionsense_battery_stream_id = CC_obj.get_stream_ids_of_owner(owner_id, config[
                "sensor_types"]["motionsense_battery"])
            battery_marker(motionsense_battery_stream_id, CC_obj, config, start_time=start_time, end_time=end_time)

        wireless_disconnection(main_stream_id, CC_obj, config, start_time=start_time, end_time=end_time)
        attachment_marker(main_stream_id, CC_obj, config, start_time=start_time, end_time=end_time)
        packet_loss_marker(main_stream_id, CC_obj, config, start_time=start_time, end_time=end_time)


stream = DiagnoseData.diagnose_data()
