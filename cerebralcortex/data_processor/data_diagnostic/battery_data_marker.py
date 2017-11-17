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
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


def battery_marker(raw_stream_id: uuid, stream_name: str, owner_id, dd_stream_name, CC: CerebralCortex, config: dict,
                   start_time=None, end_time=None):
    """
    This algorithm uses battery percentages to decide whether device was powered-off or battery was low.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore.
    :param raw_stream_id:
    :param CC:
    :param config:
    """

    try:
        # using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
        battery_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(raw_stream_id + dd_stream_name + owner_id))

        stream_days = get_stream_days(raw_stream_id, battery_marker_stream_id, CC)

        for day in stream_days:
            stream = CC.get_datastream(raw_stream_id, data_type=DataSet.COMPLETE, day=day)

            if len(stream.data) > 0:
                windowed_data = window(stream.data, config['general']['window_size'], True)
                results = process_windows(windowed_data, stream_name, config)

                merged_windows = merge_consective_windows(results)
                if len(merged_windows) > 0:
                    input_streams = [{"owner_id": owner_id, "id": str(raw_stream_id), "name": stream_name}]
                    output_stream = {"id": battery_marker_stream_id, "name": dd_stream_name,
                                     "algo_type": config["algo_type"]["battery_marker"]}
                    labelled_windows = mark_windows(battery_marker_stream_id, merged_windows, CC, config)
                    metadata = get_metadata(dd_stream_name, input_streams, config)
                    store(labelled_windows, input_streams, output_stream, metadata, CC, config)
    except Exception as e:
        print(e)


def process_windows(windowed_data: OrderedDict, stream_name: str, config: dict) -> OrderedDict:
    """
    :param windowed_data:
    :param stream_name:
    :param config:
    :return:
    """
    results = OrderedDict()
    try:
        for key, data in windowed_data.items():
            dp = []
            for k in data:
                try:
                    # TODO: it might not work with autosense/motionsense
                    sample = float(k.sample[0])
                    dp.append(sample)
                except Exception as e:
                    print(e)

            results[key] = battery(dp, stream_name, config)
    except Exception as e:
        print(e)
    return results


def battery(dp: List, stream_name: str, config: dict) -> str:
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

    try:
        if stream_name == config["stream_names"]["phone_battery"]:
            sensor_battery_down = config['battery_marker']['phone_battery_down']
            sensor_battery_off = config['battery_marker']['phone_powered_off']
        elif stream_name == config["stream_names"]["autosense_battery"]:
            sensor_battery_down = config['battery_marker']['autosense_battery_down']
            sensor_battery_off = config['battery_marker']['autosense_powered_off']
            # Values (Min=0 and Max=6) in battery voltage.
            dp_sample_avg = (dp_sample_avg / 4096) * 3 * 2
        elif stream_name == config["stream_names"]["motionsense_hrv_battery_right"] or stream_name == \
                config["stream_names"]["motionsense_hrv_battery_left"]:
            sensor_battery_down = config['battery_marker']['motionsense_battery_down']
            sensor_battery_off = config['battery_marker']['motionsense_powered_off']
        else:
            raise ValueError("Unknow sensor-battery type")
    except Exception as e:
        print(e)
    if dp_sample_avg < 1:
        return "no-data"
    elif dp_sample_avg < sensor_battery_down and dp_sample_avg > 1:
        return "low"
    elif dp_sample_avg > sensor_battery_off:
        return "charged"


def mark_windows(battery_marker_stream_id: uuid, merged_windows: List, CC, config: dict) -> str:
    """
    label a window as sensor powerd-off or low battery
    :param dp:
    :param config:
    :return:
    """
    prev_wind_sample = None
    labelled_windows = []
    try:
        for merged_window in merged_windows:
            if merged_window.sample == "no-data":
                if prev_wind_sample and prev_wind_sample == "charged":
                    merged_window.sample = config['labels']['powered_off']
                elif prev_wind_sample and prev_wind_sample == "low":
                    merged_window.sample = config['labels']['battery_down']
                else:
                    # get last stored battery marker stream if there is no previous window available for the day.
                    rows = CC.get_stream_samples(battery_marker_stream_id)
                    yesterday_sample = rows[len(rows) - 1]
                    merged_window.sample = yesterday_sample
                    prev_wind_sample = yesterday_sample

                labelled_windows.append(merged_window)
            else:
                prev_wind_sample = merged_window.sample
    except Exception as e:
        print(e)
    return labelled_windows


def get_metadata(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if dd_stream_name == config["stream_names"]["phone_battery_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "phone_powered_off_threshold": config["battery_marker"]["phone_powered_off"],
                       "phone_battery_down_threshold": config["battery_marker"]["phone_battery_down"]}
    elif dd_stream_name == config["stream_names"]["autosense_battery_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "autosense_powered_off_threshold": config["battery_marker"]["autosense_powered_off"],
                       "autosense_battery_down_threshold": config["battery_marker"]["autosense_battery_down"]}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_battery_right_marker"] or dd_stream_name == \
            config["stream_names"]["motionsense_hrv_battery_left_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "motionsense_powered_off_threshold": config["battery_marker"]["motionsense_powered_off"],
                       "motionsense_battery_down_threshold": config["battery_marker"]["motionsense_battery_down"]}
    else:
        raise ValueError("Incorrect sensor type")

    data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Labels - Powered off" + str(
        config["labels"]["powered_off"]) + ", Battery down" + str(config["labels"]["battery_down"])}
    algo_description = config["description"]["battery_data_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.battery_data_marker.py'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method, algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}
