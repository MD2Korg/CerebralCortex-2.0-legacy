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


def phone_screen_touch_marker(raw_stream_id: uuid, raw_stream_name: str, owner_id, dd_stream_name, CC: CerebralCortex,
                              config: dict, start_time=None, end_time=None):
    """
    This is not part of core data diagnostic suite.
    It only calculates how many screen touches are there.
    :param raw_stream_id:
    :param CC:
    :param config:
    """

    try:
        # using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
        screen_touch_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(
            raw_stream_id + dd_stream_name + owner_id + "mobile phone screen touch marker"))

        stream_days = get_stream_days(raw_stream_id, screen_touch_stream_id, CC)

        for day in stream_days:
            stream = CC.get_datastream(raw_stream_id, data_type=DataSet.COMPLETE, day=day, start_time=start_time,
                                       end_time=end_time)
            if len(stream.data) > 0:
                windowed_data = window(stream.data, config['general']['window_size'], True)
                results = process_windows(windowed_data)

                merged_windows = merge_consective_windows(results)
                if len(merged_windows) > 0:
                    input_streams = [{"owner_id": owner_id, "id": str(raw_stream_id), "name": raw_stream_name}]
                    output_stream = {"id": screen_touch_stream_id, "name": dd_stream_name,
                                     "algo_type": config["algo_type"]["app_availability_marker"]}
                    metadata = get_metadata(dd_stream_name, input_streams, config)
                    store(merged_windows, input_streams, output_stream, metadata, CC, config)

    except Exception as e:
        print(e)


def process_windows(windowed_data):
    results = OrderedDict()
    for key, data in windowed_data.items():
        dp = []
        for k in data:
            try:
                sample = float(k.sample)
                dp.append(sample)
            except:
                pass

        if len(dp) > 0:
            results[key] = "touch"
        else:
            results[key] = "no-touch"

    return results


def get_metadata(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    input_param = {"window_size": config["general"]["window_size"]}
    data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int",
                       "DESCRIPTION": "Participant's active and inactive periods on phone: labels: touch, no-touch"}

    algo_description = config["description"]["phone_screen_touch"]
    method = 'cerebralcortex.data_processor.data_diagnostic.util.phone_screen_touch.py'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}
