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
from collections import OrderedDict
from typing import List

import numpy as np

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import get_execution_context, get_annotations
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import get_stream_days
from cerebralcortex.data_processor.data_diagnostic.util import magnitude_datapoints
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


def sensor_availability(raw_stream_id: uuid, stream_name: str, owner_id: uuid, dd_stream_name,
                        phone_physical_activity, CC: CerebralCortex, config: dict):
    """
    Mark missing data as wireless disconnection if a participate walks away from phone or sensor
    :param raw_stream_id:
    :param stream_name:
    :param owner_id:
    :param dd_stream_name:
    :param phone_physical_activity:
    :param CC:
    :param config:
    """

    # using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
    wireless_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(raw_stream_id + dd_stream_name + owner_id))

    stream_days = get_stream_days(raw_stream_id, wireless_marker_stream_id, CC)

    for day in stream_days:
        # load stream data to be diagnosed
        raw_stream = CC.get_datastream(raw_stream_id, day, data_type=DataSet.COMPLETE)
        if len(raw_stream.data) > 0:

            windowed_data = window(raw_stream.data, config['general']['window_size'], True)
            results = process_windows(windowed_data, day, CC, phone_physical_activity, config)
            merged_windows = merge_consective_windows(results)

            if len(merged_windows) > 0:
                input_streams = [{"owner_id": owner_id, "id": str(raw_stream_id), "name": stream_name}]
                output_stream = {"id": wireless_marker_stream_id, "name": dd_stream_name,
                                 "algo_type": config["algo_type"]["sensor_unavailable_marker"]}
                metadata = get_metadata(dd_stream_name, input_streams, config)
                store(merged_windows, input_streams, output_stream, metadata, CC, config)


def process_windows(windowed_data, day, CC, phone_physical_activity, config):
    results = OrderedDict()

    motionsense_threshold = config['sensor_unavailable_marker']['motionsense']
    phone_threshold = config['sensor_unavailable_marker']['phone']

    if windowed_data:
        for key, data in windowed_data.items():
            dps = []
            for k in data:
                dps.append(k)

            if dps:
                prev_data = dps
                results[key] = config['labels']['motionsense_available']
            else:

                # compute magnitude of the window
                magnitude_vals = magnitude_datapoints(prev_data)
                # get phone accel stream data
                if phone_physical_activity:
                    phone_physical_activity_data = CC.get_stream_samples(phone_physical_activity, day=day,
                                                                         start_time=key[0], end_time=key[1])
                    # compute phone accel data magnitude
                    if np.var(magnitude_vals) > motionsense_threshold or get_total_walking_episodes(
                            phone_physical_activity_data) > 20:  # if 20 seconds contain as physical activity
                        results[key] = config['labels']['motionsense_unavailable']
                else:
                    if np.var(magnitude_vals) > motionsense_threshold:
                        results[key] = config['labels']['motionsense_unavailable']
        return results


def get_total_walking_episodes(phone_physical_activity: List) -> int:
    """

    :param phone_physical_activity:
    :return:
    """
    total = 0
    for walking in phone_physical_activity:
        try:
            walking = int(walking)
        except Exception as e:
            print(e)
        if walking and (walking == 3 or walking == 4):  # 3: walking, 4:running
            total += 1
    return total


def get_metadata(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if dd_stream_name == config["stream_names"]["autosense_wireless_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "sensor_unavailable_ecg_threshold": config["sensor_unavailable_marker"]["ecg"],
                       "sensor_unavailable_rip_threshold": config["sensor_unavailable_marker"]["rip"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                           "DESCRIPTION": "AutoSense unavailable label: " + str(
                               config["labels"]["autosense_unavailable"])}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_right_wireless_marker"] or dd_stream_name == \
            config["stream_names"]["motionsense_hrv_left_wireless_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "sensor_unavailable_motionsense_threshold": config["sensor_unavailable_marker"]["motionsense"],
                       "sensor_unavailable_phone_threshold": config["sensor_unavailable_marker"]["phone"]
                       }
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                           "DESCRIPTION": "Motionsense unavailable label: " + str(
                               config["labels"]["motionsense_unavailable"])}
    else:
        raise ValueError("Incorrect sensor type")

    algo_description = config["description"]["sensor_unavailable_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}
