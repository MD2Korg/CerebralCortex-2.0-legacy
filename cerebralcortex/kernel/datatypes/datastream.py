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

from uuid import UUID

from pyspark.sql import SparkSession

from cerebralcortex.kernel.datatypes.metadata import Metadata


class DataStream:
    def __init__(self,
                 id: int = None,
                 data: SparkSession = None,
                 user: UUID = None,
                 processing: dict = None,
                 sharing: dict = None,
                 metadata: dict = Metadata(),
                 windows: list[SparkSession] = None):
        """

        :param id:
        :param data:
        :param user:
        :param processing:
        :param sharing:
        :param metadata:
        :param windows:
        """
        self._id = id
        self._user = user
        self._processing = processing
        self._sharing = sharing
        self._metadata = metadata
        self._datapoints = data
        self._windows = windows

    def add_window(self,
                   window_rdd: SparkSession,
                   window_metadata: dict):
        self.windows.append(window_rdd)
        self.metadata.add_window_to_metadata(window_metadata)
