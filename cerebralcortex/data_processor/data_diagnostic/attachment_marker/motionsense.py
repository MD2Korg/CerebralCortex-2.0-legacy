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
import numpy as np
from collections import OrderedDict
from datetime import timedelta
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows, outlier_detection
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet

def attachment_marker(stream_id: uuid, stream_name: str, owner_id: uuid, dd_stream_name, CC: CerebralCortex, config: dict):
    """
    Label sensor data as sensor-on-body, sensor-off-body, or improper-attachment.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore

    """
    CC_driver = CC["driver"]
    CC_worker = CC["worker"]

    #using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
    attachment_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(stream_id+dd_stream_name+owner_id))

    stream_end_days = CC_driver.get_stream_start_end_time(attachment_marker_stream_id)["end_time"]

    if not stream_end_days:
        stream_end_days = []
        stream_days = CC_driver.get_stream_start_end_time(stream_id)
        days = stream_days["end_time"]-stream_days["start_time"]
        for day in range(days.days+1):
            stream_end_days.append((stream_days["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    else:
        stream_end_days = [(stream_end_days+timedelta(days=1)).strftime('%Y%m%d')]

    for day in stream_end_days:
        #load stream data to be diagnosed
        stream = CC_driver.get_datastream(stream_id, day, data_type=DataSet.COMPLETE)
        size = stream.data.map(lambda data: len(data))
        if size.take(1)[0]>0:
            windowed_data = stream.data.map(lambda data: window(data, config['general']['window_size'], True))
            results = windowed_data.map(lambda data: process_windows(data, config))
            merged_windows = results.map(lambda  data: merge_consective_windows(data))

            input_streams = [{"owner_id":owner_id, "id": str(stream_id), "name": stream_name}]
            output_stream = {"id":attachment_marker_stream_id, "name": dd_stream_name, "algo_type": config["algo_type"]["attachment_marker"]}
            result = merged_windows.map(lambda data: store(data, input_streams, output_stream, CC_worker, config))

            result.count()

def process_windows(windowed_data, config):

    results = OrderedDict()

    threshold_improper_attachment = config['attachment_marker']['motionsense_improper_attachment']
    threshold_onbody = config['attachment_marker']['motionsense_onbody']
    threshold_offbody = config['attachment_marker']['motionsense_offbody']

    label_improper_attachment = config['labels']['motionsense_improper_attachment']
    label_onbody = config['labels']['motionsense_onbody']
    label_offbody = config['labels']['motionsense_offbody']

    if windowed_data:
        for key, data in windowed_data.items():
            one_minute_window = 0
            for k in data:
                if k.sample==0:
                    one_minute_window +=1
            if (one_minute_window/20)>threshold_offbody and (one_minute_window/20)<threshold_improper_attachment:
                results[key] = label_improper_attachment
            elif (one_minute_window/20)>threshold_onbody:
                results[key] = label_onbody
            else:
                results[key] = label_offbody
        return results
