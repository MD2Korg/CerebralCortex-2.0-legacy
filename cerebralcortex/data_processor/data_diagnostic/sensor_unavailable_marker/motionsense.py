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
from cerebralcortex.data_processor.signalprocessing.window import window
import numpy as np
from datetime import timedelta
from cerebralcortex.data_processor.data_diagnostic.util import magnitude_datapoints, magnitude_list
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet
from cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker import filter_battery_off_windows


def wireless_disconnection(stream_id: uuid, stream_name: str, owner_id: uuid, dd_stream_name, phone_accel_stream_id, CC: CerebralCortex, config: dict):
    """

    :param stream_id:
    :param CC:
    :param config:
    """

    #using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
    wireless_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(stream_id+dd_stream_name+owner_id))

    stream_end_days = CC.get_stream_start_end_time(wireless_marker_stream_id)["end_time"]

    if not stream_end_days:
        stream_end_days = []
        stream_days = CC.get_stream_start_end_time(stream_id)
        days = stream_days["end_time"]-stream_days["start_time"]
        for day in range(days.days+1):
            stream_end_days.append((stream_days["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    else:
        stream_end_days = [(stream_end_days+timedelta(days=1)).strftime('%Y%m%d')]

    for day in stream_end_days:
        #load stream data to be diagnosed
        stream = CC.get_datastream(stream_id, day, data_type=DataSet.COMPLETE)
        if len(stream.data)>0:

            windowed_data = window(stream.data, config['general']['window_size'], True)
            results = process_windows(windowed_data, day, CC, phone_accel_stream_id, config)
            merged_windows = merge_consective_windows(results)

            input_streams = [{"owner_id":owner_id, "id": str(stream_id), "name": stream_name}]
            output_stream = {"id":wireless_marker_stream_id, "name": dd_stream_name, "algo_type": config["algo_type"]["sensor_unavailable_marker"]}
            store(merged_windows, input_streams, output_stream, CC, config)


def process_windows(windowed_data, day, CC, phone_accel_stream_id, config):

    results = OrderedDict()

    motionsense_threshold = config['sensor_unavailable_marker']['motionsense']
    phone_threshold = config['sensor_unavailable_marker']['phone']
    label = config['labels']['motionsense_unavailable']

    if windowed_data:
        for key, data in windowed_data.items():
            dps = []
            for k in data:
                dps.append(k)

            if dps:
                prev_data = dps
            else:

                # compute magnitude of the window
                magnitude_vals = magnitude_datapoints(prev_data)
                #get phone accel stream data
                if phone_accel_stream_id:
                    phone_accel_stream = CC.get_stream_samples(phone_accel_stream_id, day=day, start_time=key[0], end_time=key[1])
                    #compute phone accel data magnitude
                    phone_magnitude_vas = magnitude_list(phone_accel_stream)
                    #check if motionsense accel or phone accel magnitude variance is below than the threshold
                    if np.var(magnitude_vals) > motionsense_threshold or np.var(phone_magnitude_vas)> phone_threshold:
                        results[key] = label
                else:
                    if np.var(magnitude_vals) > motionsense_threshold:
                        results[key] = label
        return results





