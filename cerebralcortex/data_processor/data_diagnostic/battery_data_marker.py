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
import numpy as np
import uuid
from collections import OrderedDict

from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.CerebralCortex import CerebralCortex


def battery_marker(stream_id: uuid, CC_obj: CerebralCortex, config: dict):
    """
    This algorithm uses battery percentages to decide whether phone was powered-off or battery was low.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore.
    :param stream_id:
    :param CC_obj:
    :param config:
    """
    results = OrderedDict()

    stream = CC_obj.get_datastream(stream_id, data_type="all")
    windowed_data = window(stream.data, config['general']['window_size'], True)

    name = stream._name

    for key, data in windowed_data.items():
        dp = []
        for k in data:
            dp.append(float(k.sample))

        if name == config["sensor_types"]["phone_battery"]:
            results[key] = phone_battery(dp, config)
        elif name == config["sensor_types"]["motionsense_battery"]:
            results[key] = motionsense_battery(dp, config)
        elif name == config["sensor_types"]["autosense_battery"]:
            results[key] = autosense_battery(dp, config)
        else:
            raise ValueError("Incorrect sensor type.")

    merged_windows = merge_consective_windows(results)
    input_streams = [{"id": str(stream_id), "name": name}]
    store(input_streams, merged_windows, CC_obj, config, config["algo_names"]["battery_marker"])


def phone_battery(dp: list, config: dict) -> str:
    """
    labels a window as sensor powerd-off or low battery and returns the label
    :param dp:
    :param config:
    :return:
    """
    dp_sample_avg = np.mean(dp)
    if dp_sample_avg <= config['battery_marker']['phone_powered_off']:
        return config['labels']['sensor_powered_off']
    else:
        if dp_sample_avg < config['battery_marker']['phone_battery_down']:
            return config['labels']['phone_battery_down']
    return None


def motionsense_battery(dp: list, config: dict) -> str:
    """
    labels a window as sensor powerd-off or low battery and returns the label
    :param dp:
    :param config:
    :return:
    """
    dp_sample_avg = np.mean(dp)

    if dp_sample_avg <= config['battery_marker']['motionsense_powered_off']:
        return config['labels']['motionsense_powered_off']
    else:
        if dp_sample_avg < config['battery_marker']['phone_powered_off']:
            return config['labels']['motionsense_battery_down']
    return None


def autosense_battery(dp: list, config:dict) -> str:
    """
    labels a window as sensor powerd-off or low battery and returns the label
    :param dp:
    :param config:
    :return:
    """
    dp_sample_avg = np.mean(dp)
    if dp_sample_avg <= config['battery_marker']['autosense_powered_off']:
        return config['labels']['autosesen_powered_off']
    else:
        # Values (Min=0 and Max=6) in battery voltage.
        voltageValue = (dp_sample_avg / 4096) * 3 * 2
        if voltageValue < config['battery_marker']['autosense_battery_down']:
            return config['labels']['autosense_battery_down']
    return None
