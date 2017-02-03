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
from typing import List, Dict

from cerebralcortex import CerebralCortex
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.enumerations import StreamTypes
from cerebralcortex.kernel.datatypes.metadata import \
    Metadata as MetadataStruct  # TODO: change class name to solve unambiguity between Metadata struct class and DataStoreEngine.Metadata
from cerebralcortex.kernel.datatypes.processing import Processing
from cerebralcortex.kernel.datatypes.study import Study
from cerebralcortex.kernel.datatypes.user import User


class DataStream:
    def __init__(self,
                 cerebral_cortex: CerebralCortex,
                 user: User,
                 study_list: List[Study] = None,  # all the study info related to a datastream
                 processing_module: Processing = None,
                 # datastream_type: str = None,
                 metadata: MetadataStruct = None,
                 source_ids: dict = None,
                 identifier: int = None,
                 data: List[DataPoint] = None
                 ) -> None:
        """
        The primary object in Cerebral Cortex which represents data
        """

        self._cc = cerebral_cortex
        self._id = identifier
        self._user = user
        self._study_list = study_list
        self._processing_module = processing_module
        self._datastream_type = StreamTypes.DATASTREAM
        self._metadata = metadata
        self._source_ids = source_ids
        self._data = data

    @property
    def cerebral_cortex(self) -> CerebralCortex:
        return self._cc

    @cerebral_cortex.setter
    def cerebral_cortex(self, cc: CerebralCortex):
        self._cc = cc

    @property
    def identifier(self):
        return self._id

    @property
    def datapoints(self) -> List[DataPoint]:
        return self._data

    @datapoints.setter
    def datapoints(self, value: List[DataPoint]):
        self._data = value

    @property
    def datastream_type(self):
        return self._datastream_type

    @property
    def source_ids(self):
        return self._source_ids

    @property
    def user(self):
        return self._user

    @property
    def processing_module(self):
        return self._processing_module

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value: Dict):
        self._metadata = value

    @property
    def study_ids(self):
        study_ids = []
        for studyObj in self.study_list:
            study_id = studyObj.getStudyID()
            study_ids.append(study_id)
        return study_ids

    @property
    def study_list(self):
        return self._study_list

    @classmethod
    def from_datastream(cls, input_streams: List):
        """
        :param input_streams:
        :return:
        """
        result = cls(cerebral_cortex=input_streams[0].cerebral_cortex,
                     user=input_streams[0].user)

        # TODO: Something with provenance tracking from datastream list

        return result

    def __str__(self):
        return str(self.identifier) + " - " + str(self.user.getMetadata()) + " - " + str(
            self.processing_module.getMetadata()) + " - " + str(self.metadata) + " - " + str(self.datapoints)
