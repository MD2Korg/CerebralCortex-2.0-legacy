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

import statistics as stat
from collections import OrderedDict
from numpy.linalg import norm
from typing import List
import math
import json
import uuid
from datetime import timedelta
import numpy as np
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.kernel.datatypes.datastream import DataPoint


def merge_consective_windows(data: OrderedDict) -> List[DataPoint]:
    """
    Merge two or more windows if the time difference between them is 0
    :param data:
    :return:
    """
    merged_windows = []
    element = None
    start = None
    end = None
    val = None
    if data:
        for key, val in data.items():
            if element is None:
                element = val
                start = key[0]
                end = key[1]
            elif element == val and (end == key[0]):
                element = val
                end = key[1]
            else:
                merged_windows.append(DataPoint(start, end, element))  # [(export_data, end)] = element
                element = val
                start = key[0]
                end = key[1]
        if val is not None:
            merged_windows.append(DataPoint(start, end, val))  # merged_windows[(export_data, end)] = val

    return merged_windows


def outlier_detection(window_data: list) -> list:
    """
    removes outliers from a list
    This algorithm is modified version of Chauvenet's_criterion (https://en.wikipedia.org/wiki/Chauvenet's_criterion)
    :param window_data:
    :return:
    """
    if not window_data:
        raise ValueError("List is empty.")

    vals = []
    for dp in window_data:
        vals.append(float(dp.sample))

    median = stat.median(vals)
    standard_deviation = stat.stdev(vals)
    normal_values = list()

    for val in window_data:
        if (abs(float(val.sample)) - median) < standard_deviation:
            normal_values.append(float(val.sample))

    return normal_values


def get_stream_days(raw_stream_id: uuid, dd_stream_id: uuid, CC: CerebralCortex) -> List:
    """
    Returns a list of days that needs be diagnosed for a participant
    :param raw_stream_id:
    :param dd_stream_id:
    """
    dd_stream_days = CC.get_stream_start_end_time(dd_stream_id)["end_time"]

    if not dd_stream_days:
        stream_days = CC.get_stream_start_end_time(raw_stream_id)
        days = stream_days["end_time"]-stream_days["start_time"]
        for day in range(days.days+1):
            stream_days.append((stream_days["start_time"]+timedelta(days=day)).strftime('%Y%m%d'))
    else:
        stream_days = [(dd_stream_days+timedelta(days=1)).strftime('%Y%m%d')]
    return stream_days


def magnitude_datapoints(data: DataPoint) -> List:
    """

    :param data:
    :return:
    """

    if data is None or len(data) == 0:
        return []

    input_data = np.array([i.sample for i in data])
    data = norm(input_data, axis=1).tolist()

    return data


def magnitude_list(data: List) -> List:
    """

    :param data:
    :return:
    """

    if data is None or len(data) == 0:
        return []

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except:
            data = data
    try:
        input_data = np.array([i for i in data])
        data = norm(input_data, axis=1).tolist()
    except Exception as e:
        print("Error in calculating magnigutude ----> ")
        print("Data: ", data)
        print("NP Array: ", input_data)
        print(e)
        raise Exception

    return data


def magnitude_autosense_v1(accel_x: float, accel_y: float, accel_z: float) -> list:
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
