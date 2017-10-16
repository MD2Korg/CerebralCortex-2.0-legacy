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

import numpy as np

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


def battery_marker(stream_id: uuid, CC_obj: CerebralCortex, config: dict, start_time=None, end_time=None):
    """
    This algorithm uses battery percentages to decide whether phone was powered-off or battery was low.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore.
    :param stream_id:
    :param CC_obj:
    :param config:
    """
    results = OrderedDict()

    stream = CC_obj.get_datastream(stream_id, data_type=DataSet.COMPLETE, start_time=start_time, end_time=end_time)
    windowed_data = window(stream.data, config['general']['window_size'], True)

    stream_name = stream._name

    for key, data in windowed_data.items():
        dp = []
        for k in data:
            dp.append(float(k.sample))

        results[key] = battery(dp, config)
    merged_windows = merge_consective_windows(results)
    labelled_windows = mark_windows(merged_windows)
    input_streams = [{"id": str(stream_id), "name": stream_name}]
    store(input_streams, labelled_windows, CC_obj, config, config["algo_names"]["battery_marker"])


def battery(dp: list, sensor_type: str, config: dict) -> str:
    """
    label a window as sensor powerd-off or low battery
    :param dp:
    :param config:
    :return:
    """
    if not dp:
        dp_sample_avg = 0
    else:
        dp_sample_avg = np.median(dp)

    if sensor_type=="phone_battery":
        sensor_battery_down = config['battery_marker']['phone_battery_down']
        sensor_battery_off = config['battery_marker']['phone_powered_off']
    elif sensor_type=="autosense_battery":
        sensor_battery_down = config['battery_marker']['autosense_battery_down']
        sensor_battery_off = config['battery_marker']['autosense_powered_off']
        # Values (Min=0 and Max=6) in battery voltage.
        dp_sample_avg = (dp_sample_avg / 4096) * 3 * 2
    elif sensor_type=="motionsense_battery":
        sensor_battery_down = config['battery_marker']['motionsense_battery_down']
        sensor_battery_off = config['battery_marker']['motionsense_powered_off']
    else:
        raise ValueError("Unknow sensor-battery type")

    if dp_sample_avg < 1:
        return "no-data"
    elif dp_sample_avg < sensor_battery_down and dp_sample_avg > 1:
        return "low"
    elif dp_sample_avg > sensor_battery_off:
        return "charged"


def mark_windows(merged_windows: list, config: dict) -> str:
    """
    label a window as sensor powerd-off or low battery
    :param dp:
    :param config:
    :return:
    """
    prev_wind = None
    labelled_windows = []
    for merged_window in merged_windows:
        if merged_window.sample == "no-data":
            if prev_wind and prev_wind.sample == "charged":
                merged_window.sample = config['labels']['powered_off']
            elif prev_wind and prev_wind.sample == "low":
                merged_window.sample = config['labels']['battery_down']
            else:
                # TODO: if first window data has 'no-data' label then query DB to check label of the last window
                merged_window.sample = "TODO"
            labelled_windows.append(merged_window)
        else:
            prev_wind = merged_window
    return labelled_windows