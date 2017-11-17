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

import uuid

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


def filter_battery_off_windows(stream_id: uuid, stream_name: str, main_stream_windows: dict, owner_id: uuid,
                               config: dict, CC_obj: CerebralCortex) -> dict:
    """

    :param stream_id:
    :param stream_name:
    :param main_stream_windows:
    :param owner_id:
    :param config:
    :param CC_obj:
    :return:
    """

    start_time = ""
    end_time = ""
    # load phone battery data
    phone_battery_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS,
                                                str(stream_id + config["stream_name"]["phone_battery"] + owner_id))
    phone_battery_marker_stream = CC_obj.get_datastream(phone_battery_marker_stream_id, data_type=DataSet.ONLY_DATA,
                                                        start_time=start_time,
                                                        end_time=end_time)

    # load sensor battery data
    if stream_name == config["stream_names"]["autosense_ecg"] or stream_name == config["stream_names"]["autosense_rip"]:
        sensor_battery_marker_stream = CC_obj.get_datastream(phone_battery_marker_stream_id,
                                                             data_type=DataSet.ONLY_DATA, start_time=start_time,
                                                             end_time=end_time)
    elif stream_name == config["stream_names"]["motionsense_hrv_accel_right"]:
        sensor_battery_marker_stream = CC_obj.get_datastream(phone_battery_marker_stream_id,
                                                             data_type=DataSet.ONLY_DATA, start_time=start_time,
                                                             end_time=end_time)
    elif stream_name == config["stream_names"]["motionsense_hrv_accel_left"]:
        sensor_battery_marker_stream = CC_obj.get_datastream(phone_battery_marker_stream_id,
                                                             data_type=DataSet.ONLY_DATA, start_time=start_time,
                                                             end_time=end_time)
    battery_marker = 0
    results = None
    for key, data in main_stream_windows.items():
        for phone_key, phone_data in phone_battery_marker_stream.items():
            if phone_key.start_time <= key.start_time and phone_key.end_time >= key.end_time:
                battery_marker = 1
        for sensor_key, sensor_data in sensor_battery_marker_stream.items():
            if sensor_key.start_time <= key.start_time and sensor_key.end_time >= key.end_time:
                battery_marker = 1

        if battery_marker != 1:
            results[key] = data
    return results
