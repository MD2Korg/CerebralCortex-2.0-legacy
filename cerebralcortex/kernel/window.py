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
import math
from collections import OrderedDict

import numpy as np


def epoch_align(timestamp, offset, after=False):
    new_timestamp = math.floor(timestamp / offset) * offset

    if after:
        new_timestamp += offset

    return new_timestamp


def window(data: list,
           window_size: int):
    """
    Special case of a sliding window with no overlaps
    :param data:
    :param window_size:
    :return:
    """
    return window_sliding(data, window_size=window_size, window_offset=window_size)


def window_sliding(data: list,
                   window_size: int,
                   window_offset: int):
    """
    Sliding Window Implementation

    :param data: list
    :param window_size: int
    :param window_offset: int
    :return: OrderedDict representing [(st,et),[dp,dp,dp,dp...],
                                       (st,et),[dp,dp,dp,dp...],
                                        ...]
    """
    if data is None:
        return None

    if len(data) == 0:
        return None

    starttime = data[0].get_timestamp_epoch() / 1000
    endtime = data[-1].get_timestamp_epoch() / 1000
    windowed_datastream = OrderedDict()

    for ts in np.arange(starttime, endtime, window_offset):
        key = (ts, ts + window_size)
        values = [dp for dp in data if ts <= dp.get_timestamp_epoch() / 1000 < ts + window_size]
        windowed_datastream[key] = values
    return windowed_datastream
