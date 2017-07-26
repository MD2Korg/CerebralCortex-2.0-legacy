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

import datetime
import glob
import os
import uuid

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_migrator.data_builder import bz2file_to_datapoints
from cerebralcortex.data_migrator.schema_builder import get_annotations, get_data_descriptor, get_execution_context
from cerebralcortex.data_migrator.util import read_file
from cerebralcortex.kernel.datatypes.datastream import DataStream


def migrate(folder_path: str, data_block_size):
    """
    Migrate data from old CerebralCortex structure to new CerebralCortex structure
    :param folder_path:
    """

    configuration_file = os.path.join(os.path.dirname(__file__), '../../cerebralcortex.yml')
    CC = CerebralCortex(configuration_file, master="local[*]", name="Data Migrator API", time_zone="US/Central")

    if not folder_path:
        raise ValueError("Path to the data directory cannot be empty.")

    for filename in glob.iglob(folder_path + '/**/*.json', recursive=True):
        print(str(datetime.datetime.now()) + " -- Started processing file " + filename)

        tmp = filename.split("/")
        tmp = tmp[len(tmp) - 1].split("+")
        owner_id = tmp[0]
        stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(tmp[0] + " " + tmp[1])))

        name = ''
        for i in tmp[3:]:
            name += i + " "

        name = name.strip().replace(".json", "")
        name = tmp[1] + " " + name

        pm_algo_name = tmp[2]

        data_filename = filename.replace(".json", ".csv.bz2")
        old_schema = read_file(filename)
        execution_context = get_execution_context(pm_algo_name, old_schema)
        data_descriptor = get_data_descriptor(old_schema)
        annotations = get_annotations()
        print(str(datetime.datetime.now()) + " -- Schema building is complete ")
        print(str(datetime.datetime.now()) + " -- Started unzipping file and adding records in Cassandra ")
        for data_block in bz2file_to_datapoints(data_filename, data_block_size):
            persist_data(execution_context, data_descriptor, annotations, stream_id, name, owner_id, data_block, CC)
        print(str(datetime.datetime.now()) + " -- Completed processing file " + filename)


def persist_data(execution_context, data_descriptor, annotations, stream_id, name, owner_id, data, CC_obj):
    """
    Parse old schema and map it into new CerebralCortex data provenance sechema
    :param filename:
    :param data:
    :param CC_obj:
    """

    stream_type = "datastream"
    ds = DataStream(identifier=stream_id, owner=owner_id, name=name, data_descriptor=data_descriptor,
                    execution_context=execution_context, annotations=annotations,
                    stream_type=stream_type, data=data)

    CC_obj.save_datastream(ds)


# sample usage
migrate("/home/ali/IdeaProjects/MD2K_DATA/Rice/8be4f601-70ce-3e13-a321-b85ee84b37ce", 1000)
