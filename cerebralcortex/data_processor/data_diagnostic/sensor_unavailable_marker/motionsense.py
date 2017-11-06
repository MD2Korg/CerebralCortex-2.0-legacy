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
from cerebralcortex.data_processor.data_diagnostic.util import magnitude_motionsense
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet
from cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker import filter_battery_off_windows


def wireless_disconnection(stream_id: uuid, stream_name: str, owner_id: uuid, CC_obj: CerebralCortex, config: dict):
    """
    Analyze whether a sensor was unavailable due to a wireless disconnection
    or due to sensor powered off. This method automatically loads related
    accelerometer streams of an owner. All the labeled data (st, et, label)
    with its metadata are then stored in a datastore.
    Note: If an owner owns more than one accelerometer (for example, more
    than one motionsense accelerometer) then this might not work.
    :param stream_id: stream_id should be of "battery-powered-off"
    :param CC_obj:
    :param config:
    """

    results = OrderedDict()

    stream_end_time = CC_obj.get_stream_start_end_time(stream_id)["end_time"]
    day = stream_end_time

    #load stream data to be diagnosed
    stream = CC_obj.get_datastream(stream_id, day, data_type=DataSet.COMPLETE)

    windowed_data = window(stream.data, config['general']['window_size'], True)

    windowed_data = filter_battery_off_windows(stream_id, stream_name, windowed_data, owner_id, config, CC_obj)

    threshold = config['sensor_unavailable_marker']['motionsense']
    label = config['labels']['motionsense_unavailable']


    if windowed_data:
        for dp in windowed_data:
            if dp.data:
                prev_data = dp.data
            else:
                # compute magnitude of the window
                magnitude_vals = magnitude_motionsense(prev_data)

                if np.var(magnitude_vals) > threshold:
                    key = (dp.start_time, dp.end_time)
                    results[key] = label

            input_streams = [{"id": str(stream_id), "name": str(stream_name)}]
            merged_windows = merge_consective_windows(results)
            store(input_streams, merged_windows, CC_obj, config, config["algo_names"]["sensor_unavailable_marker"])






