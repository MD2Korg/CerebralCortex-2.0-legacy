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

from typing import Any, List, Dict


class StreamReference:
    def __init__(self,
                 name: str = None,
                 stream_identifier: int = None):
        self._name = name
        self._stream_identifier = stream_identifier

    @property
    def name(self):
        return self._name

    @property
    def stream_identifier(self):
        return self._stream_identifier


class KeyValue:
    def __init__(self, name: int, value: Any):
        self._name = name
        self._value = value


class DataDescriptor:
    def __init__(self,
                 type_string: str = None,
                 unit: str = None,
                 descriptive_statistic: str = None):
        self._type = type_string
        self._unit = unit
        self._descriptive_statistic = descriptive_statistic

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def unit(self):
        return self._unit

    @unit.setter
    def unit(self, value):
        self._unit = value

    @property
    def descriptive_statistic(self):
        return self._descriptive_statistic

    @descriptive_statistic.setter
    def descriptive_statistic(self, value):
        self._descriptive_statistic = value


class ExecutionContext:
    def __init__(self,
                 processing_module: int = None,
                 input_parameters: List[KeyValue] = None,
                 input_streams: List[StreamReference] = None,
                 metadata: Dict = None):
        self._processing_module = processing_module
        self._input_parameters = input_parameters
        self._input_streams = input_streams
        self._metadata = metadata
