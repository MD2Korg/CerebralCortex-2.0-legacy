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
from numpy.linalg import norm
from typing import List
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.vector import magnitude
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet
from cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker import filter_battery_off_windows


def wireless_disconnection(stream_id: uuid, all_stream_ids_names:dict, CC_obj: CerebralCortex, config: dict, start_time=None, end_time=None):
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

    #load stream data to be diagnosed
    stream = CC_obj.get_datastream(stream_id, data_type=DataSet.COMPLETE, start_time=start_time,
                                   end_time=end_time)
    windowed_data = window(stream.data, config['general']['window_size'], True)

    owner_id = stream._owner
    stream_name = stream._name

    windowed_data = filter_battery_off_windows(stream_id, stream_name, windowed_data, owner_id, config, CC_obj)

    threshold = config['sensor_unavailable_marker']['motionsense']
    label = config['labels']['motionsense_unavailable']


    if windowed_data:
        #prepare input streams metadata
        if stream_name == config["stream_names"]["motionsense_hrv_accel_right"]:
            motionsense_accel_stream_id = all_stream_ids_names[config["stream_names"]["motionsense_hrv_accel_right"]]
            output_stream_name = config["stream_names"]["motionsense_hrv_accel_right"]

        elif stream_name == config["stream_names"]["motionsense_hrv_accel_left"]:
            motionsense_accel_stream_id = all_stream_ids_names[config["stream_names"]["motionsense_hrv_accel_left"]]
            output_stream_name = config["stream_names"]["motionsense_hrv_accel_left"]

        input_streams = [{"id": str(stream_id), "name": str(stream_name)},
                         {"id": str(motionsense_accel_stream_id),
                          "name": output_stream_name}]

        for dp in windowed_data:
            if dp.data:
                prev_data = dp.data
            else:
                # compute magnitude of the window
                magnitude_vals = magnitude(prev_data)

                if np.var(magnitude_vals) > threshold:
                    key = (dp.start_time, dp.end_time)
                    results[key] = label

        merged_windows = merge_consective_windows(results)
        store(input_streams, merged_windows, CC_obj, config, config["algo_names"]["sensor_unavailable_marker"])


def magnitude(data: DataPoint) -> List:
    """

    :param data:
    :return:
    """

    if data is None or len(data) == 0:
        return []

    input_data = np.array([i.sample for i in data])
    data = norm(input_data, axis=1).tolist()

    return data



