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
from cerebralcortex.kernel.datatypes.span import Span
from collections import OrderedDict
import numpy as np

def window(datastream, windowsize):
    rdd = datastream.rdd
    result = rdd.map(lambda x: (int(x.timestamp / windowsize) * windowsize, (x.timestamp, x.sample)))

    newMeta = {}

    return Span([datastream], newMeta, result)


def epochAlign(timestamp, offset, after=False):

    newTimestamp = Math.floor(timestamp / offset)*offset

    if after:
        newTimestamp += offset

    return newTimestamp

def window_sliding(data: list,
                   window_size: int,
                   window_offset: int):
    """
    Sliding Window Implementation

    :param data: list
    :param window_size: int
    :param window_offset: int
    :return: [(st,et),[dp,dp,dp,dp...],
              (st,et),[dp,dp,dp,dp...],
              ...]
    """
    if data==None:
        return None
    else:
        starttime = data[0].get_timestamp_epoch()/1000
        endtime = data[-1].get_timestamp_epoch()/1000
        windowed_datastream = OrderedDict()

        for ts in np.arange(starttime,endtime,window_offset):
            key = (ts,ts+window_size)
            values = [dp for dp in data if ts <= dp.get_timestamp_epoch()/1000 < ts+window_size]
            windowed_datastream[key] = values
    return windowed_datastream