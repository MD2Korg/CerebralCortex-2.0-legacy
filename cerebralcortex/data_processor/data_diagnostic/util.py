# Copyright (c) 2016, MD2K Center of Excellence
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

from datetime import datetime
from pytz import timezone
import math
from typing import List
from collections import OrderedDict
import statistics as stat
import csv

from cerebralcortex.kernel.datatypes.datastream import DataPoint

def datetime_to_epoch(date_obj):
    localtz = timezone('US/Central')

    epoch = datetime.utcfromtimestamp(0)
    epoch = localtz.localize(epoch)
    return (date_obj - epoch).total_seconds() * 1000.0


def write_to_csv(data: OrderedDict, file_name):
    ofile = open('/home/ali/Documents/' + file_name, "w")
    writer = csv.writer(ofile, delimiter=",")

    for row, val in data.items():
        rr = str(row), str(val)
        writer.writerow(rr)

    ofile.close()


def merge_consective_windows(data: OrderedDict) -> OrderedDict:
    """
    Merge two or more windows if the time difference between them is 0
    :param data:
    :return:
    """
    merged_windows = OrderedDict()
    element = None
    start = None
    end = None
    val = None
    for key, val in data.items():
        if element is None:
            element = val
            start = key[0]
            end = key[1]
        elif element == val:
            element = val
            end = key[1]
        else:
            merged_windows[(start, end)] = element
            element = val
            start = key[0]
            end = key[1]
    if val is not None:
        merged_windows[(start, end)] = val

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

def map_data_to_datapoints(data: list) -> DataPoint:
    """
    :param data:
    :return:
    """
    datapoint_list = []
    for key, value in data.items():
        datapoint_list.append(DataPoint(key[0], key[1], value))
    return datapoint_list

def motionsense_magnitude(accel_xyz: List[DataPoint]) -> DataPoint:
    """
    compute magnitude of x, y, and z
    :param accel_xyz:
    :return: list of DataPoint
    """
    magnitudeList = []

    for dp in accel_xyz:
        data = dp.sample
        magnitude = math.sqrt(math.pow(data[0], 2) + math.pow(data[1], 2) + math.pow(data[2], 2))
        magnitudeList.append(DataPoint(dp.start_time, dp.end_time, magnitude))

    return magnitudeList