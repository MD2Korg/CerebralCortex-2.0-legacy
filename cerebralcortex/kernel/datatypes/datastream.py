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
from typing import List

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.enumerations import StreamTypes
from cerebralcortex.kernel.datatypes.subtypes import StreamReference, DataDescriptor, ExecutionContext
from cerebralcortex.kernel.datatypes.user import User


class DataStream:
    def __init__(self,
                 identifier: int = None,
                 user: User = None,
                 name: str = None,
                 description: str = None,
                 data_descriptor: List[DataDescriptor] = None,
                 execution_context: ExecutionContext = None,
                 annotations: List[StreamReference] = None,
                 data: List[DataPoint] = None
                 ):
        self._identifier = identifier
        self._user = user
        self._name = name
        self._description = description
        self._data_descriptor = data_descriptor
        self._datastream_type = StreamTypes.DATASTREAM
        self._execution_context = execution_context
        self._annotations = annotations
        self._data = data

    def find_annotation_references(self, identifier: int = None, name: str = None):
        result = self._annotations
        found = False

        if identifier:
            found = True
            result = [a for a in result if a.stream_identifier == identifier]

        if name:
            found = True
            result = [a for a in result if a.name == name]

        if not found:
            return []

        return result

    @property
    def annotations(self):
        return self._annotations

    @annotations.setter
    def annotations(self, value):
        self._annotations = value

    @property
    def identifier(self):
        return self._identifier

    @property
    def user(self):
        return self._user

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    @property
    def data_descriptor(self):
        return self._data_descriptor

    @data_descriptor.setter
    def data_descriptor(self, value):
        self._data_descriptor = value

    @property
    def execution_context(self):
        return self._execution_context

    @execution_context.setter
    def execution_context(self, value):
        self._execution_context = value

    @property
    def datastream_type(self):
        return self._datastream_type

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value

    @classmethod
    def from_datastream(cls, input_streams: List):
        result = cls(user=input_streams[0].user)

        # TODO: Something with provenance tracking from datastream list

        return result

    def __str__(self):
        return str(self.identifier) + " - " + str(self.user.identifier) + " - " + str(self.data)
