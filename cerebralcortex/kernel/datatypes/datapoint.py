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
from typing import Any


class DataPoint:
    def __init__(self,
                 startTime: datetime = None,
                 endTime: datetime = None,
                 sample: Any = None,
                 metadata: dict=None) -> None:
        """
        :param startTime: Python datetime object with timezone information
        :param sample: Python object representing any data
        """

        self.startTime = startTime
        self.endTime = endTime
        self._sample = sample
        self.metadata = metadata

    def get_sample(self) -> Any:
        """
        :return:
        """
        return self._sample

    def set_sample(self, value: Any) -> None:
        """
        :param value:
        """
        self._sample = value

    def getStartTime(self) -> datetime:
        """
        :return:
        """
        return self.startTime

    def getEndTime(self) -> datetime:
        """
        :return:
        """
        return self.endTime

    def getMetadata(self):
        return self.metadata

    def get_timestamp_epoch(self, tzinfo: timezone = timezone.utc) -> int:
        # TODO: Handle timezone information
        """
        :param tzinfo:
        :return:
        """
        if self.startTime is None:
            raise ValueError

        return int(self.startTime.timestamp() * 1e6)

    @classmethod
    def from_tuple(cls, startTime: datetime, endTime: datetime, sample: Any, metadata: Any):
        """
        :param startTime:
        :param endTime:
        :param sample:
        :param metadata:
        :return:
        """
        return cls(startTime=startTime, endTime=endTime, sample=sample, metadata=metadata)

    def __str__(self):
        return str(self._datastreamID)+" - "+str(self.startTime) + " - " + str(self._sample)
