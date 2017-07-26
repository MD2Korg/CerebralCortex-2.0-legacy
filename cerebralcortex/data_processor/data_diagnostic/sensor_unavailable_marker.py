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
from cerebralcortex.data_processor.signalprocessing.vector import magnitude
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


# TO-DO: use advanced sensor quality algorithms to detect whether a window contains good or bad data.


def wireless_disconnection(stream_id: uuid, CC_obj: CerebralCortex, config: dict, start_time=None, end_time=None):
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
    threshold = 0

    stream_info = CC_obj.get_datastream(stream_id, data_type=DataSet.ONLY_METADATA, start_time=start_time,
                                        end_time=end_time)

    owner_id = stream_info._owner
    name = stream_info._name

    stream_name = stream_info._name

    if name == config["sensor_types"]["autosense_ecg"]:
        threshold = config['sensor_unavailable_marker']['ecg']
        label = config['labels']['autosense_unavailable']
    if name == config["sensor_types"]["autosense_rip"]:
        threshold = config['sensor_unavailable_marker']['rip']
        label = config['labels']['autosense_unavailable']
    elif name == config["sensor_types"]["motionsense_accel"]:
        threshold = config['sensor_unavailable_marker']['motionsense']
        label = config['labels']['motionsense_unavailable']

    battery_off_data = CC_obj.get_datastream(stream_id, data_type=DataSet.ONLY_DATA, start_time=start_time,
                                             end_time=end_time)

    if battery_off_data:
        if name == config["sensor_types"]["motionsense_accel"]:
            motionsense_accel_stream_id = CC_obj.get_stream_id_by_owner_id(owner_id,
                                                                           config[
                                                                               "sensor_types"][
                                                                               "motionsense_accel"],
                                                                           "id")
            input_streams = [{"id": str(stream_id), "name": str(stream_name)},
                             {"id": str(motionsense_accel_stream_id),
                              "name": config["sensor_types"]["motionsense_accel"]}]
        else:
            x = CC_obj.get_stream_id_by_owner_id(owner_id,
                                                 config["sensor_types"]["autosense_accel_x"])
            y = CC_obj.get_stream_id_by_owner_id(owner_id,
                                                 config["sensor_types"]["autosense_accel_y"])
            z = CC_obj.get_stream_id_by_owner_id(owner_id,
                                                 config["sensor_types"]["autosense_accel_z"])
            input_streams = [{"id": str(stream_id), "name": stream_name},
                             {"id": str(x), "name": config["sensor_types"]["autosense_accel_x"]},
                             {"id": str(y), "name": config["sensor_types"]["autosense_accel_y"]},
                             {"id": str(z), "name": config["sensor_types"]["autosense_accel_z"]}]

        for dp in battery_off_data:
            if dp.start_time != "" and dp.end_time != "":
                # get a window prior to a battery powered off
                start_time = dp.start_time - timedelta(seconds=config['general']['window_size'])
                end_time = dp.start_time
                if name == config["sensor_types"]["motionsense_accel"]:
                    motionsense_accel_xyz = CC_obj.get_datastream(motionsense_accel_stream_id, start_time=start_time,
                                                                  end_time=end_time, data_type=DataSet.COMPLETE)
                    magnitudeValStream = magnitude(motionsense_accel_xyz)
                    magnitudeVals = []
                    for mv in magnitudeValStream.data:
                        magnitudeVals.append(mv.sample)

                else:
                    autosense_acc_x = CC_obj.get_datastream(x, start_time=start_time, end_time=end_time,
                                                            data_type=DataSet.ONLY_DATA)
                    autosense_acc_y = CC_obj.get_datastream(y, start_time=start_time, end_time=end_time,
                                                            data_type=DataSet.ONLY_DATA)
                    autosense_acc_z = CC_obj.get_datastream(z, start_time=start_time, end_time=end_time,
                                                            data_type=DataSet.ONLY_DATA)

                    magnitudeVals = autosense_calculate_magnitude(autosense_acc_x, autosense_acc_y, autosense_acc_z)

                if np.var(magnitudeVals) > threshold:
                    key = (dp.start_time, dp.end_time)
                    results[key] = label

        merged_windows = merge_consective_windows(results)
        store(input_streams, merged_windows, CC_obj, config, config["algo_names"]["sensor_unavailable_marker"])


def autosense_calculate_magnitude(accel_x: float, accel_y: float, accel_z: float) -> list:
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
