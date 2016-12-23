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
from pprint import pprint
from uuid import UUID

from pyspark import RDD
from pyspark.sql import SparkSession

from cerebralcortex.kernel.datatypes.metadata import Metadata


class DataStream:
    def __init__(self,
                 user: UUID,
                 id: int = None,
                 data: RDD = None,
                 processing: dict = None,
                 sharing: dict = None,
                 metadata: Metadata = Metadata(),
                 windows: list = None):
        """
        The primary object in Cerebral Cortex which represents data
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

    def get_datapoints(self):
        return self._datapoints


    def add_window(self,
                   window_rdd: SparkSession,
                   window_metadata: dict):
        self._windows.append(window_rdd)
        self._metadata.add_window_to_metadata(window_metadata)

    def get_metadata(self, search_param=None):
        return search_param

    def save(self):
        """
        Persist this datastream object to the datastores
        """

        print(self._user)
        pprint(self._datapoints.map(lambda dp: dp.get_timestamp()).takeSample(False, 10))

        pass

    def update_metadata(self, metadata):
        # Use metadata to update self._metadata

        pass

    def get_user(self):
        return self._user

    @classmethod
    def from_datastream(self, datastream):
        result = DataStream(user=datastream._user,
                            data=datastream._datapoints)

        return result
