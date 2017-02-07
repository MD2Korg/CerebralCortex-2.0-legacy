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

from cerebralcortex import CerebralCortex
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.datatypes.enumerations import StreamTypes
from cerebralcortex.kernel.datatypes.metadata import Metadata as MetadataStruct
from cerebralcortex.kernel.datatypes.study import Study
from cerebralcortex.kernel.datatypes.user import User
from cerebralcortex.kernel.datatypes.processing import Processing


class AnnotationStream(DataStream):
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
                 ):
        """
        The primary object in Cerebral Cortex which represents data
        :param cerebralcortex: Reference to the Cerebral Cortex object
        :param identifier:
        :param data:
        :param processing:
        :param metadata:
        """

        super().__init__(cerebral_cortex, user, study_list, processing_module, metadata, source_ids, identifier, data)

        self._datastream_type = StreamTypes.ATTRIBUTE
