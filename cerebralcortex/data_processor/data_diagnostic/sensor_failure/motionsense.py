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
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.data_diagnostic.post_processing import store
from cerebralcortex.data_processor.data_diagnostic.util import merge_consective_windows
import numpy as np
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet


def sensor_failure_marker(accel_stream_id: uuid, gyro_stream_id, owner_id: uuid, dd_stream_name, CC: CerebralCortex, config: dict):
    """
    Label a window as packet-loss if received packets are less than the expected packets.
    All the labeled data (st, et, label) with its metadata are then stored in a datastore.
    :param stream_id:
    :param CC_obj:
    :param config:
    """

    #using stream_id, data-diagnostic-stream-id, and owner id to generate a unique stream ID for battery-marker
    sensor_failure_stream_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(accel_stream_id+gyro_stream_id+dd_stream_name+owner_id+"SENSOR FAILURE MARKER"))

    stream_end_days = CC.get_stream_start_end_time(sensor_failure_stream_id)["end_time"]

    if not stream_end_days:
        stream_end_days = []
        stream_days = CC.get_stream_start_end_time(accel_stream_id)
        days = stream_days["end_time"]-stream_days["start_time"]
        for day in range(days.days+1):
            stream_end_days.append((stream_days["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    else:
        stream_end_days = [(stream_end_days+timedelta(days=1)).strftime('%Y%m%d')]


    for day in stream_end_days:
        #load stream data to be diagnosed
        accel_stream = CC.get_datastream(accel_stream_id, day, data_type=DataSet.COMPLETE)
        gyro_stream = CC.get_datastream(gyro_stream_id, day, data_type=DataSet.COMPLETE)
        result = []
        if len(accel_stream.data)>0 and len(gyro_stream.data)>0:
            # 6 hours window 21600
            windowed_data_accel = window(accel_stream.data, 21600, True)
            results_accel = process_windows(windowed_data_accel, config)

            windowed_data_gyro = window(gyro_stream.data, 21600, True)
            results_gyro = process_windows(windowed_data_gyro, config)

            # if sensor failure period is more than 12 hours then mark it as a sensor failure
            if (results_accel>1 and results_gyro<1) or (results_accel<1 and results_gyro>1):
                start_time = accel_stream.data[0].start_time
                end_time = accel_stream.data[len(accel_stream.data)-1].start_time
                sample = config["labels"]["motionsense_failure"]
                result.append(DataPoint(start_time, end_time, sample))
            if len(result)>0:
                input_streams = [{"owner_id":owner_id, "id": str(accel_stream_id), "name": accel_stream.name, "id": str(gyro_stream_id), "name": gyro_stream.name}]
                output_stream = {"id":sensor_failure_stream_id, "name": dd_stream_name, "algo_type": config["algo_type"]["sensor_failure"]}
                store(result, input_streams, output_stream, CC, config)


def process_windows(windowed_data, config):
    total_failures = 0

    for key, data in windowed_data.items():
        dp = []
        for k in data:
            dp.append(float(k.sample[0]))
        signal_var = np.var(dp)
        if signal_var < config["sensor_failure"]["threshold"]:
            total_failures +=1
    return total_failures
