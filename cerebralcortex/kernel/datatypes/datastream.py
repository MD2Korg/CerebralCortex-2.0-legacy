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

from cerebralcortex import CerebralCortex
from cerebralcortex.kernel.datatypes.metadata import Metadata
from cerebralcortex.kernel.datatypes.spanstream import SpanStream


class DataStream:
    def __init__(self,
                 cerebralcortex: CerebralCortex,
                 user: UUID,
                 id: int = None,
                 data: list = None,
                 processing: dict = None,
                 sharing: dict = None,
                 metadata: Metadata = Metadata(),
                 spanstreams: list = None):
        """
        The primary object in Cerebral Cortex which represents data
        :param cerebralcortex: Reference to the Cerebral Cortex object
        :param id:
        :param data:
        :param user:
        :param processing:
        :param sharing:
        :param metadata:
        :param windows:
        """

        self._cc = cerebralcortex
        self._id = id
        self._user = user
        self._processing = processing
        self._sharing = sharing
        self._metadata = metadata
        self._datapoints = data
        self._spanstreams = spanstreams

    def get_datapoints(self):
        return self._datapoints

    def get_user(self):
        return self._user

    @classmethod
    def from_datastream(cls, inputstreams: list, data: list):
        """

        :param inputstreams:
        :param data:
        :return:
        """
        result = cls(cerebralcortex=inputstreams[0]._cc,
                     user=inputstreams[0]._user,
                     data=data)

        # TODO: Something with provenance tracking from datastream list

        return result

    def addSpanStream(self, spanstream: SpanStream):
        self._spanstreams.append(spanstream)
