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

import math
import uuid
from collections import OrderedDict
from datetime import timedelta

import numpy as np

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet
from cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker import filter_battery_off_windows


def wireless_disconnection(stream_id: uuid, all_stream_ids_names: dict, CC_obj: CerebralCortex, config: dict,
                           start_time=None, end_time=None):
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

    # load stream data to be diagnosed
    stream = CC_obj.get_datastream(stream_id, data_type=DataSet.COMPLETE, start_time=start_time,
                                   end_time=end_time)
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

                magnitudeVals = autosense_accel_magnitude(autosense_accel_x, autosense_accel_y, autosense_accel_z)

                if np.var(magnitudeVals) > threshold:
                    key = (dp.start_time, dp.end_time)
                    results[key] = label

        merged_windows = merge_consective_windows(results)
        store(input_streams, merged_windows, CC_obj, config, config["algo_names"]["sensor_unavailable_marker"])


def autosense_accel_magnitude(accel_x: float, accel_y: float, accel_z: float) -> list:
    """
    compute magnitude of x, y, and z
    :param accel_x:
    :param accel_y:
    :param accel_z:
    :return: magnitude values of a window as list
    """
    magnitudeList = []
    max_list_size = len(max(accel_x, accel_y, accel_z, key=len))

    for i in range(max_list_size):
        x = 0 if len(accel_x) - 1 < i else float(accel_x[i].sample)
        y = 0 if len(accel_y) - 1 < i else float(accel_y[i].sample)
        z = 0 if len(accel_z) - 1 < i else float(accel_z[i].sample)

        magnitude = math.sqrt(math.pow(x, 2) + math.pow(y, 2) + math.pow(z, 2));
        magnitudeList.append(magnitude)

    return magnitudeList
