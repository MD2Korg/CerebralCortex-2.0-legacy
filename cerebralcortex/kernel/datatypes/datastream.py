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
from typing import List
from uuid import UUID

from cerebralcortex import CerebralCortex
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.metadata import Metadata
from cerebralcortex.kernel.datatypes.spanstream import SpanStream

from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata


class DataStream:
    def __init__(self,
                 #cerebralcortex: CerebralCortex,
                 user_id: UUID,
                 study_ids: dict,
                 processing_module_id: int,
                 datastream_type: str,
                 source_ids: dict = None,
                 identifier: int = None,
                 data: List[DataPoint] = None
                 ) -> None:
        """
        The primary object in Cerebral Cortex which represents data
        :param cerebralcortex: Reference to the Cerebral Cortex object
        :param identifier:
        :param data:
        :param user_id:
        :param processing:
        :param sharing:
        :param metadata:
        :param windows:
        """

        #self._cc = cerebralcortex
        self._id = identifier
        self._user_id = user_id
        self._study_ids = study_ids
        self._processing_module_id = processing_module_id
        self._datastream_type = datastream_type
        self._source_ids = source_ids
        self._datapoints = data


    def get_datastream_metadata(self):
        datastreamInfo = Metadata("").getDatastreamInfo(self._id)
        datastreamMetadata = datastreamInfo[0][3]
        return datastreamMetadata

    def get_user_metadata(self):
        userInfo = Metadata("").getDatastreamInfo(self._user_id)
        userMetadata = userInfo[0][1]
        return userMetadata

    def get_study_metadata(self) -> list:
        """
        :return: list of study metadata
        """
        studyMetadata = []
        for studyID in self._study_ids:
            studyInfo = Metadata("").getDatastreamInfo(studyID)
            studyMetadata.append(studyInfo[0][2])
        return studyMetadata

    def get_processing_module_metadata(self):
        processingModuleInfo = Metadata("").getDatastreamInfo(self._processing_module_id)
        procesingModuleMetadata = processingModuleInfo[0][1]
        return procesingModuleMetadata



    def get_cc(self) -> CerebralCortex:
        """
        :return:
        """
        return self._cc

    def get_identifier(self) -> int:
        """
        :return:
        """
        return self._id

    def get_datapoints(self) -> List[DataPoint]:
        """
        :return:
        """
        return self._datapoints

    def set_datapoints(self, data: list) -> None:
        """
        :param data:
        """
        self._datapoints = data

    def get_user_id(self) -> UUID:
        """
        :return:
        """
        return self._user_id

    def get_study_ids(self):
        return self._study_ids

    def get_processing_module_id(self):
        return self._processing_module_id

    def get_datastream_type(self):
        return self._datastream_type

    def get_source_ids(self):
        return self._source_ids

    @classmethod
    def from_datastream(cls, input_streams: list):
        """
        :param input_streams:
        :return:
        """
        result = cls(cerebralcortex=input_streams[0]._cc,
                     user=input_streams[0].get_user())

        # TODO: Something with provenance tracking from datastream list

        return result

    def add_span_stream(self, spanstream: SpanStream) -> None:
        """
        :param spanstream:
        """
        self._spanstreams.append(spanstream)

    def __str__(self):
        return str(self._id)+" - "+str(self._user_id) + " - " + str(self._processing) + " - " + str(self._metadata) + " - " + str(self._datapoints)
