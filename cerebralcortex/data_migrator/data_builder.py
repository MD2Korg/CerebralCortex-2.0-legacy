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


import bz2
import datetime
from typing import List

from pytz import timezone

from cerebralcortex.kernel.datatypes.datastream import DataPoint


def bz2file_to_datapoints(filename: str, data_block_size) -> List[DataPoint]:
    """
    Read bz2 compressed files and map the data to Datapoints structure
    :param filename:
    :return:
    """
    datapoints = []
    bz_file = bz2.BZ2File(filename)

    line_number = 1

    for line in bz_file:
        row = line.decode("utf-8").replace("\r\n", "").split(
            ",")
        if (len(row) == 5):
            sample = str([row[2], row[3], row[4]])
        else:
            sample = str([row[2]])

        start_time = datetime.datetime.fromtimestamp(int(row[0]) / 1000)
        localtz = timezone('US/Central')
        start_time = localtz.localize(start_time)
        end_time = ""

        if line_number > data_block_size:
            yield datapoints
            datapoints.clear()
            line_number = 1
        else:
            datapoints.append(DataPoint(start_time=start_time, end_time=end_time, sample=sample))
            line_number += 1

    yield datapoints
