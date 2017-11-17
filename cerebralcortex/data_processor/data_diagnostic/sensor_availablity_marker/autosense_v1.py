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
from datetime import timedelta

import numpy as np
from cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker import filter_battery_off_windows

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import magnitude_autosense_v1
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


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

    # load stream data to be diagnosed
    stream = CC_obj.get_datastream(stream_id, day, data_type=DataSet.COMPLETE)
    windowed_data = window(stream.data, config['general']['window_size'], True)

    owner_id = stream._owner
    stream_name = stream._name

    windowed_data = filter_battery_off_windows(stream_id, stream_name, windowed_data, owner_id, config, CC_obj)

    threshold = config['sensor_unavailable_marker']['autosense']
    label = config['labels']['autosense_unavailable']

    if windowed_data:
        # prepare input streams metadata
        x = all_stream_ids_names[config["stream_names"]["autosense_accel_x"]]
        y = all_stream_ids_names[config["stream_names"]["autosense_accel_y"]]
        z = all_stream_ids_names[config["stream_names"]["autosense_accel_z"]]

        input_streams = [{"id": str(stream_id), "name": stream_name},
                         {"id": str(x), "name": config["stream_names"]["autosense_accel_x"]},
                         {"id": str(y), "name": config["stream_names"]["autosense_accel_y"]},
                         {"id": str(z), "name": config["stream_names"]["autosense_accel_z"]}]

        for dp in windowed_data:
            if not dp.data and dp.start_time != "" and dp.end_time != "":
                start_time = dp.start_time - timedelta(seconds=config['general']['window_size'])
                end_time = dp.start_time

                autosense_accel_x = CC_obj.get_datastream(x, start_time=start_time, end_time=end_time,
                                                          data_type=DataSet.ONLY_DATA)
                autosense_accel_y = CC_obj.get_datastream(y, start_time=start_time, end_time=end_time,
                                                          data_type=DataSet.ONLY_DATA)
                autosense_accel_z = CC_obj.get_datastream(z, start_time=start_time, end_time=end_time,
                                                          data_type=DataSet.ONLY_DATA)

                magnitudeVals = magnitude_autosense_v1(autosense_accel_x, autosense_accel_y, autosense_accel_z)

                if np.var(magnitudeVals) > threshold:
                    key = (dp.start_time, dp.end_time)
                    results[key] = label

        merged_windows = merge_consective_windows(results)
        store(input_streams, merged_windows, CC_obj, config, config["algo_names"]["sensor_unavailable_marker"])
