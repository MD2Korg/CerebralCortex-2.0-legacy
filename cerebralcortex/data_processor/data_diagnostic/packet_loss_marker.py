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

import uuid
from collections import OrderedDict

from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows, motionsense_magnitude
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.CerebralCortex import CerebralCortex

def packet_loss_marker(stream_id: uuid, CC_obj: CerebralCortex, config: dict):
    """
    Label a window as packet-loss if received packets are less than the expected packets.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore.
    :param stream_id:
    :param CC_obj:
    :param config:
    """
    stream = CC_obj.get_datastream(stream_id, data_type="all")

    name = stream._name
    results = OrderedDict()

    if name == config["sensor_types"]["autosense_ecg"]:
        sampling_rate = config["sampling_rate"]["ecg"]
        threshold_val = config["packet_loss_marker"]["ecg_acceptable_packet_loss"]
        label = config["labels"]["ecg_packet_loss"]
        windowed_data = window(stream.data, config['general']['window_size'], False)
    elif name == config["sensor_types"]["autosense_rip"]:
        sampling_rate = config["sampling_rate"]["rip"]
        threshold_val = config["packet_loss_marker"]["rip_acceptable_packet_loss"]
        label = config["labels"]["rip_packet_loss"]
        windowed_data = window(stream.data, config['general']['window_size'], False)
    elif name == config["sensor_types"]["motionsense_accel"]:
        sampling_rate = config["sampling_rate"]["motionsense"]
        threshold_val = config["packet_loss_marker"]["motionsense_acceptable_packet_loss"]
        label = config["labels"]["motionsense_packet_loss"]
        motionsense_accel_magni = motionsense_magnitude(stream.data)
        windowed_data = window(motionsense_accel_magni, config['general']['window_size'], False)
    else:
        raise ValueError("Incorrect sensor type.")

    for key, data in windowed_data.items():

        available_packets = len(data)
        expected_packets = sampling_rate * config['general']['window_size']

        if (available_packets / expected_packets) < threshold_val:
            results[key] = label

    merged_windows = merge_consective_windows(results)
    input_streams = [{"id": str(stream_id), "name": name}]
    store(input_streams, merged_windows, CC_obj, config, config["algo_names"]["packet_loss_marker"])
