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

import json
from collections import OrderedDict

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.schema_builder.data_descriptor import data_descriptor
from cerebralcortex.kernel.schema_builder.execution_context import execution_context


def store(data: OrderedDict, input_streams: dict, output_streams: dict, CC_obj: CerebralCortex):
    """
    Store diagnostic results with its metadata in the data-store
    :param input_streams:
    :param data:
    :param CC_obj:
    :param config:
    :param algo_type:
    """
    if data:
        #basic output stream info
        owner = input_streams[0]["owner_id"]
        dd_stream_id = output_streams["id"]
        dd_stream_name = output_streams["name"]
        stream_type = "ds"



        ds = DataStream(identifier=dd_stream_id, owner=owner, name=dd_stream_name, data_descriptor=data_descriptor,
                        execution_context=execution_context, annotations=annotations,
                        stream_type=stream_type, data=data)

        CC_obj.save_datastream(ds,"datastream")


