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

from datetime import datetime, timezone


class DataPoint:
    def __init__(self,
                 datastream: int = None,
                 timestamp: datetime = None,
                 sample: object = None):
        """

        :param datastream: Cerebral Cortex provided data stream identifier
        :param timestamp: Python datetime object with timezone information
        :param sample: Python object representing any data
        """

        self._datastreamID = datastream
        self._timestamp = timestamp
        self._sample = sample

    def get_sample(self) -> object:
        return self._sample

    def set_sample(self, value):
        self._sample = value

    def get_timestamp(self) -> datetime:
        return self._timestamp

    def get_timestamp_epoch(self, tzinfo=timezone.utc) -> int:
        # TODO: Handle timezone information
        if self._timestamp is None:
            raise ValueError

        return int(self._timestamp.timestamp() * 1e6)

    def get_datastream_id(self) -> int:
        return self._datastreamID

    @classmethod
    def from_tuple(cls, datastream, timestamp, sample):
        return cls(datastream=datastream, timestamp=timestamp, sample=sample)
