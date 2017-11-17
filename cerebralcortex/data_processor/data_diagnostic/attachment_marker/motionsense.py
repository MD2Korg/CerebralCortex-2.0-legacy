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
from cerebralcortex.data_processor.data_diagnostic.util import get_stream_days
from collections import OrderedDict
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows, outlier_detection
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet

def attachment_marker(raw_stream_id: uuid, stream_name: str, owner_id: uuid, dd_stream_name, CC: CerebralCortex, config: dict):
    """
    Label sensor data as sensor-on-body, sensor-off-body, or improper-attachment.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore

    """
    # TODO: quality streams could be multiple so find the one computed with CC
    #using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
    attachment_marker_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(raw_stream_id + dd_stream_name + owner_id))

    stream_days = get_stream_days(raw_stream_id, attachment_marker_stream_id, CC)

    for day in stream_days:
        #load stream data to be diagnosed
        raw_stream = CC.get_datastream(raw_stream_id, day, data_type=DataSet.COMPLETE)

        if len(raw_stream.data)>0:
            windowed_data = window(raw_stream.data, config['general']['window_size'], True)
            results = process_windows(windowed_data, config)
            merged_windows = merge_consective_windows(results)

            input_streams = [{"owner_id":owner_id, "id": str(raw_stream_id), "name": stream_name}]
            output_stream = {"id":attachment_marker_stream_id, "name": dd_stream_name, "algo_type": config["algo_type"]["attachment_marker"]}
            store(merged_windows, input_streams, output_stream, CC, config)


def process_windows(windowed_data: OrderedDict, config: dict) -> OrderedDict:
    """
    :param windowed_data:
    :param config:
    :return:
    """
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
