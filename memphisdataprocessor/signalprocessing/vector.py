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

import numpy as np
from numpy.linalg import norm
from sklearn import preprocessing

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def normalize(datastream: DataStream) -> DataStream:
    """

    :param datastream:
    :return:
    """
    input_data = np.array([i.get_sample() for i in datastream.get_datapoints()])

    data = preprocessing.normalize(input_data, axis=0)

    result_data = [DataPoint.from_tuple(timestamp=i.get_timestamp(), sample=None)
                   for i in datastream.get_datapoints()]
    for i, dp in enumerate(result_data):
        dp.set_sample(data[i])

    result = DataStream.from_datastream(input_streams=[datastream])
    result.set_datapoints(result_data)

    return result


def magnitude(datastream: DataStream) -> DataStream:
    """

    :param datastream:
    :return:
    """
    input_data = np.array([i.get_sample() for i in datastream.get_datapoints()])

    data = norm(input_data, axis=1).tolist()

    result_data = [DataPoint.from_tuple(timestamp=i.get_timestamp(), sample=None)
                   for i in datastream.get_datapoints()]

    for i, dp in enumerate(result_data):
        dp.set_sample(data[i])

    result = DataStream.from_datastream(input_streams=[datastream])
    result.set_datapoints(result_data)

    return result
