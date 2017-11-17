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

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import get_execution_context, get_annotations
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import get_stream_days
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


def packet_loss_marker(raw_stream_id: uuid, stream_name: str, owner_id: uuid, dd_stream_name, CC: CerebralCortex,
                       config: dict):
    """
    Label a window as packet-loss if received packets are less than the expected packets.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore.
    :param raw_stream_id:
    :param CC_obj:
    :param config:
    """

    # using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
    packetloss_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(raw_stream_id + dd_stream_name + owner_id))

    stream_days = get_stream_days(raw_stream_id, packetloss_marker_stream_id, CC)

    if stream_name == config["stream_names"]["autosense_ecg"]:
        sampling_rate = config["sampling_rate"]["ecg"]
        threshold_val = config["packet_loss_marker"]["ecg_acceptable_packet_loss"]
        label = config["labels"]["ecg_packet_loss"]
    elif stream_name == config["stream_names"]["autosense_rip"]:
        sampling_rate = config["sampling_rate"]["rip"]
        threshold_val = config["packet_loss_marker"]["rip_acceptable_packet_loss"]
        label = config["labels"]["rip_packet_loss"]
    elif stream_name == config["stream_names"]["motionsense_hrv_accel_right"] or stream_name == config["stream_names"][
        "motionsense_hrv_accel_left"]:
        sampling_rate = config["sampling_rate"]["motionsense_accel"]
        threshold_val = config["packet_loss_marker"]["motionsense_accel_acceptable_packet_loss"]
        label = config["labels"]["motionsense_gyro_packet_loss"]
    elif stream_name == config["stream_names"]["motionsense_hrv_gyro_right"] or stream_name == config["stream_names"][
        "motionsense_hrv_gyro_left"]:
        sampling_rate = config["sampling_rate"]["motionsense_gyro"]
        threshold_val = config["packet_loss_marker"]["motionsense_gyro_acceptable_packet_loss"]
        label = config["labels"]["motionsense_gyro_packet_loss"]

    for day in stream_days:
        # load stream data to be diagnosed
        stream = CC.get_datastream(raw_stream_id, day, data_type=DataSet.COMPLETE)

        if len(stream.data) > 0:

            windowed_data = window(stream.data, config['general']['window_size'], True)

            results = process_windows(windowed_data, sampling_rate, threshold_val, label, config)
            merged_windows = merge_consective_windows(results)
            if len(merged_windows) > 0:
                input_streams = [{"owner_id": owner_id, "id": str(raw_stream_id), "name": stream_name}]
                output_stream = {"id": packetloss_marker_stream_id, "name": dd_stream_name,
                                 "algo_type": config["algo_type"]["packet_loss_marker"]}
                metadata = get_metadata(dd_stream_name, input_streams, config)
                store(merged_windows, input_streams, output_stream, metadata, CC, config)


def process_windows(windowed_data: OrderedDict, sampling_rate: float, threshold_val: float, label: str,
                    config: dict) -> OrderedDict:
    """
    :param windowed_data:
    :param sampling_rate:
    :param threshold_val:
    :param label:
    :param config:
    :return:
    """
    results = OrderedDict()
    expected_packets = sampling_rate * config["general"]["window_size"]

    if windowed_data:
        for key, data in windowed_data.items():
            available_packets = len(data)
            if (available_packets / expected_packets) < threshold_val and (available_packets / expected_packets) > 0.1:
                results[key] = label
        return results


def get_metadata(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if dd_stream_name == config["stream_names"]["autosense_ecg_packetloss_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "ecg_acceptable_packet_loss": config["packet_loss_marker"]["ecg_acceptable_packet_loss"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                           "DESCRIPTION": "Packet-Loss label: " + str(config["labels"]["ecg_packet_loss"])}
    elif dd_stream_name == config["stream_names"]["autosense_rip_packetloss_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_acceptable_packet_loss": config["packet_loss_marker"]["rip_acceptable_packet_loss"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                           "DESCRIPTION": "Packet-Loss label: " + str(config["labels"]["rip_packet_loss"])}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_accel_right_packetloss_marker"] or dd_stream_name == \
            config["stream_names"]["motionsense_hrv_accel_left_packetloss_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_acceptable_packet_loss": config["packet_loss_marker"][
                           "motionsense_accel_acceptable_packet_loss"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Packet-Loss label: " + str(
            config["labels"]["motionsense_accel_packet_loss"])}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_gyro_right_packetloss_marker"] or dd_stream_name == \
            config["stream_names"]["motionsense_hrv_gyro_left_packetloss_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_acceptable_packet_loss": config["packet_loss_marker"][
                           "motionsense_gyro_acceptable_packet_loss"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                           "DESCRIPTION": "Packet-Loss label: " + str(config["labels"]["motionsense_gyro_packet_loss"])}
    else:
        raise ValueError("Incorrect sensor type")

    algo_description = config["description"]["packet_loss_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.packet_loss_marker.py'

    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}
