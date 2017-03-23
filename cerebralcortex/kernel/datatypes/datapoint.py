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
from typing import Any


class DataPoint:
    def __init__(self,
                 start_time: datetime = None,
                 end_time: datetime = None,
                 sample: Any = None):
        self._start_time = start_time
        self._end_time = end_time
        self._sample = sample

    @property
    def sample(self):
        return self._sample

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    # @property
    # def datastream_id(self):
    #     return self._datastream_id

    @classmethod
    def from_tuple(cls, start_time: datetime, sample: Any, end_time: datetime = None):
        return cls(None, start_time, end_time, sample)

    def __str__(self):
        return str(self.start_time) + " - " + str(self.sample)

    def __repr__(self):
        return 'DataPoint(' + ', '.join(map(str, [self.start_time, self.end_time, self.sample]))
