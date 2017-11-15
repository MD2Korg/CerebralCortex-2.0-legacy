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
from datetime import timedelta
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet

def mobile_app_availability_marker(stream_id: uuid, stream_name:str, owner_id, dd_stream_name, CC: CerebralCortex, config: dict, start_time=None, end_time=None):
    """
    This algorithm uses phone battery percentages to decide whether mobile app was available or unavailable.
    Theoretically, phone battery data shall be collected 24/7.
    :param stream_id:
    :param CC:
    :param config:
    """

    try:
        #using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
        app_availability_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(stream_id+dd_stream_name+owner_id+"mobile app availability marker"))

        stream_end_days = CC.get_stream_start_end_time(app_availability_marker_stream_id)["end_time"]

        if not stream_end_days:
            stream_end_days = []
            stream_days = CC.get_stream_start_end_time(stream_id)
            days = stream_days["end_time"]-stream_days["start_time"]
            for day in range(days.days+1):
                stream_end_days.append((stream_days["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
        else:
            stream_end_days = [(stream_end_days+timedelta(days=1)).strftime('%Y%m%d')]

        for day in stream_end_days:
            stream = CC.get_datastream(stream_id, data_type=DataSet.COMPLETE, day=day, start_time=start_time, end_time=end_time)
            if len(stream.data)>0:
                windowed_data = window(stream.data, config['general']['window_size'], True)
                results = process_windows(windowed_data, config)

                merged_windows = merge_consective_windows(results)
                if len(merged_windows)>0:
                    input_streams = [{"owner_id":owner_id, "id": str(stream_id), "name": stream_name}]
                    output_stream = {"id":app_availability_marker_stream_id, "name": dd_stream_name, "algo_type": config["algo_type"]["app_availability_marker"]}
                    store(merged_windows, input_streams, output_stream, CC, config)

    except Exception as e:
        print(e)


def process_windows(windowed_data, config):
    results = OrderedDict()
    for key, data in windowed_data.items():
        dp = []
        for k in data:
            try:
                sample = float(k.sample)
                dp.append(sample)
            except:
                pass
        results[key] = app_availability(dp, config)
    return results


def app_availability(dp: list, config: dict) -> str:
    """

    :param dp:
    :param config:
    :return:
    """
    if len(dp)<1:
        dp_sample_avg = 0
    else:
        try:
            dp_sample_avg = np.median(dp)
        except:
            dp_sample_avg=0

    if dp_sample_avg < 1:
        return config['labels']['app_unavailable']
    else:
        return config['labels']['app_available']
