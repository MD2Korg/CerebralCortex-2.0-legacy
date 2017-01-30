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

from cerebralcortex import CerebralCortex
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.metadata import Metadata as MetadataStruct # TODO: change class name to solve unambiguity between Metadata struct class and DataStoreEngine.Metadata
from cerebralcortex.kernel.datatypes.processing import Processing
from cerebralcortex.kernel.datatypes.spanstream import SpanStream
from cerebralcortex.kernel.datatypes.study import Study
from cerebralcortex.kernel.datatypes.user import User


class DataStream:
    def __init__(self,
                 cerebralcortex: CerebralCortex,
                 userObj: User,
                 studyObjList: List[Study], #all the study info related to a datastream
                 processingModuleObj: Processing,
                 datastream_type: str,
                 metadata: MetadataStruct,
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

        self._cc = cerebralcortex
        self._id = identifier
        self.userObj = userObj
        self.studyObjList = studyObjList
        self.processingModuleObj = processingModuleObj
        self._datastream_type = datastream_type
        self.metadata = metadata
        self._source_ids = source_ids
        self._datapoints = data



    def getMetadata(self):
        return self.metadata

    def getStudyIDs(self):
        studyIDs = []
        for studyObj in self.studyObjList:
            studyID = studyObj.getStudyID()
            studyIDs.append(studyID)
        return studyIDs

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
        return str(self._id)+" - "+str(self.userObj.getMetadata()) + " - " + str(self.processingModuleObj.getMetadata()) + " - " + str(self.metadata) + " - " + str(self._datapoints)
