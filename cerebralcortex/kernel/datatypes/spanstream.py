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

from pyspark import RDD

from cerebralcortex import CerebralCortex
from cerebralcortex.kernel.datatypes.metadata import Metadata


class SpanStream:
    def __init__(self,
                 cerebralcortex: CerebralCortex,
                 id: int = None,
                 data: RDD = None,
                 processing: dict = None,
                 metadata: Metadata = Metadata()):
        """
        The primary object in Cerebral Cortex which represents data
        :param cerebralcortex: Reference to the Cerebral Cortex object
        :param id:
        :param data:
        :param processing:
        :param metadata:
        """

        self._cc = cerebralcortex
        self._id = id
        self._processing = processing
        self._metadata = metadata
        self._spans = data

    def set_spans(self, spandata):
        self._spans = spandata

    def getID(self):
        return self._id

    @classmethod
    def from_stream(cls, inputstreams: list, data: list) -> SpanStream:
        result = SpanStream(cerebralcortex=inputstreams[0]._cc,
                            user=inputstreams[0]._user,
                            data=data)

        return result
