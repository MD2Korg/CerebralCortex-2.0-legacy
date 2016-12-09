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

from metadata import Metadata


class DataPoint:
    """A data point class"""

    def __init__(self, sample, timestamp, metadata=Metadata()):
        self.id = None
        self.datastream = None
        self.timestamp = timestamp
        self.sample = sample
        self.metadata = metadata

    def __str__(self):
        return 'DP: (' + str(self.timestamp) + ',' + str(self.sample) + ')'


class Window:
    """A window class"""

    def __init__(self, sample, startTime, endTime=None, metadata=Metadata()):
        self.id = None
        self.datastream = None
        self.startTime = startTime
        self.endTime = endTime
        self.sample = sample
        self.metadata = metadata

    def __str__(self):
        return 'Window: (' + str(self.startTime) + ',' + str(self.endTime) + ',' + str(self.sample) + ')'


class DataStream:
    """A data stream class"""

    def __init__(self, user, processing=None, sharing=None, metadata=Metadata()):
        self.id = None
        self.user = user
        self.processing = processing
        self.sharing = sharing
        self.metadata = metadata


class User:
    """A user class"""

    def __init__(self, user, metadata=Metadata()):
        self.id = None
        self.user = user
        self.metadata = metadata


class Processing:
    """A processing  class"""

    def __init__(self, metadata=Metadata()):
        self.id = None
        self.metadata = metadata


class SharingPolicy:
    """A sharing policy class"""

    def __init__(self, metadata=Metadata()):
        self.id = None
        self.metadata = metadata
