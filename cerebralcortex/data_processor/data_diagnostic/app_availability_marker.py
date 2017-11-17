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


def mobile_app_availability_marker(raw_stream_id: uuid, stream_name: str, owner_id, dd_stream_name, CC: CerebralCortex,
                                   config: dict, start_time=None, end_time=None):
    """
    This algorithm uses phone battery percentages to decide whether mobile app was available or unavailable.
    Theoretically, phone battery data shall be collected 24/7.
    :param raw_stream_id:
    :param CC:
    :param config:
    """

    try:
        # using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
        app_availability_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(
            raw_stream_id + dd_stream_name + owner_id + "mobile app availability marker"))

        stream_days = get_stream_days(raw_stream_id, app_availability_marker_stream_id, CC)

        for day in stream_days:
            stream = CC.get_datastream(raw_stream_id, data_type=DataSet.COMPLETE, day=day)
            if len(stream.data) > 0:
                windowed_data = window(stream.data, config['general']['window_size'], True)
                results = process_windows(windowed_data, config)

                merged_windows = merge_consective_windows(results)
                if len(merged_windows) > 0:
                    input_streams = [{"owner_id": owner_id, "id": str(raw_stream_id), "name": stream_name}]
                    output_stream = {"id": app_availability_marker_stream_id, "name": dd_stream_name,
                                     "algo_type": config["algo_type"]["app_availability_marker"]}
                    metadata = get_metadata(dd_stream_name, input_streams, config)
                    store(merged_windows, input_streams, output_stream, metadata, CC, config)

    except Exception as e:
        print(e)


def process_windows(windowed_data: OrderedDict, config: dict) -> OrderedDict:
    """

    :param windowed_data:
    :param config:
    :return:
    """
    results = OrderedDict()
    for key, data in windowed_data.items():
        dp = []
        for k in data:
            try:
                sample = float(k.sample[0])
                dp.append(sample)
            except Exception as e:
                print(e)
        results[key] = app_availability(dp, config)
    return results


def app_availability(dp: List, config: dict) -> str:
    """

    :param dp:
    :param config:
    :return:
    """
    if not dp:
        dp_sample_avg = 0
    else:
        try:
            dp_sample_avg = np.median(dp)
        except:
            dp_sample_avg = 0

    if dp_sample_avg < 1:
        return config['labels']['app_unavailable']
    else:
        return config['labels']['app_available']


def get_metadata(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    input_param = {"window_size": config["general"]["window_size"],
                   "app_availability_marker_battery_threshold": "1"}
    data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "mobile phone availability: " + str(
        config["labels"]["app_unavailable"]) + ", " + str(config["labels"]["app_available"])}

    algo_description = config["description"]["app_availability_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.app_availability.py'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}
