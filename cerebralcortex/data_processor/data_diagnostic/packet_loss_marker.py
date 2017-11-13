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
from datetime import timedelta
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.vector import magnitude
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


def packet_loss_marker(stream_id: uuid, stream_name: str, owner_id: uuid, dd_stream_name, CC: CerebralCortex, config: dict):
    """
    Label a window as packet-loss if received packets are less than the expected packets.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore.
    :param stream_id:
    :param CC_obj:
    :param config:
    """

    #using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
    packetloss_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(stream_id+dd_stream_name+owner_id))

    stream_end_days = CC.get_stream_start_end_time(packetloss_marker_stream_id)["end_time"]

    if not stream_end_days:
        stream_end_days = []
        stream_days = CC.get_stream_start_end_time(stream_id)
        days = stream_days["end_time"]-stream_days["start_time"]
        for day in range(days.days+1):
            stream_end_days.append((stream_days["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    else:
        stream_end_days = [(stream_end_days+timedelta(days=1)).strftime('%Y%m%d')]

    if stream_name == config["stream_names"]["autosense_ecg"]:
        sampling_rate = config["sampling_rate"]["ecg"]
        threshold_val = config["packet_loss_marker"]["ecg_acceptable_packet_loss"]
        label = config["labels"]["ecg_packet_loss"]
    elif stream_name == config["stream_names"]["autosense_rip"]:
        sampling_rate = config["sampling_rate"]["rip"]
        threshold_val = config["packet_loss_marker"]["rip_acceptable_packet_loss"]
        label = config["labels"]["rip_packet_loss"]
    elif stream_name == config["stream_names"]["motionsense_hrv_accel_right"] or stream_name == config["stream_names"]["motionsense_hrv_accel_left"]:
        sampling_rate = config["sampling_rate"]["motionsense_accel"]
        threshold_val = config["packet_loss_marker"]["motionsense_accel_acceptable_packet_loss"]
        label = config["labels"]["motionsense_gyro_packet_loss"]
    elif stream_name == config["stream_names"]["motionsense_hrv_gyro_right"] or stream_name == config["stream_names"]["motionsense_hrv_gyro_left"]:
        sampling_rate = config["sampling_rate"]["motionsense_gyro"]
        threshold_val = config["packet_loss_marker"]["motionsense_gyro_acceptable_packet_loss"]
        label = config["labels"]["motionsense_gyro_packet_loss"]

    for day in stream_end_days:
        #load stream data to be diagnosed
        stream = CC.get_datastream(stream_id, day, data_type=DataSet.COMPLETE)

        if len(stream.data)>0:

            windowed_data = window(stream.data, config['general']['window_size'], True)

            results = process_windows(windowed_data, sampling_rate, threshold_val, label, CC, config)
            merged_windows = merge_consective_windows(results)

            input_streams = [{"owner_id":owner_id, "id": str(stream_id), "name": stream_name}]
            output_stream = {"id":packetloss_marker_stream_id, "name": dd_stream_name, "algo_type": config["algo_type"]["packet_loss_marker"]}
            result = store(merged_windows, input_streams, output_stream, CC, config)



def process_windows(windowed_data, sampling_rate, threshold_val, label, CC, config):

    results = OrderedDict()
    expected_packets = sampling_rate*config["general"]["window_size"]

    if windowed_data:
        for key, data in windowed_data.items():
            available_packets = len(data)
            if (available_packets / expected_packets) < threshold_val:
                results[key] = label
        return results
