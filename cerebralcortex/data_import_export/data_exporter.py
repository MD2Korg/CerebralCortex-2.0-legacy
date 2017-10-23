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

import bz2
import json
import os
import shutil
import uuid
from datetime import datetime, timedelta
from typing import List

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_import_export.util import calculate_time
from cerebralcortex.kernel.DataStoreEngine.Data.Data import Data
from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata


class DataExporter():
    def __init__(self, CC_obj: CerebralCortex, export_dir_path: str, owner_ids: List = None,
                 owner_user_names: List = None, owner_name_regex: str = None, start_time: str = None,
                 end_time: str = None):
        """
        :param CC_obj:
        :param export_dir_path:
        :param owner_ids: owner_user_name and owner_name_regex must be None if using owner_id
        :param owner_user_names: owner_id and owner_name_regex must be None if using owner_user_name
        :param owner_name_regex: owner_id and owner_user_name must be None if using owner_name_reges
        :param start_time:
        :param end_time:
        """

        self.streamData = Data(CC_obj)
        self.export_dir_path = export_dir_path
        self.metadata = Metadata(CC_obj)
        self.owner_ids = owner_ids
        self.owner_user_names = owner_user_names
        self.owner_name_regex = str(owner_name_regex)
        self.start_time = start_time
        self.end_time = end_time

    def start(self):
        if self.owner_ids and self.owner_ids != 'None':
            for owner_id in self.owner_ids:
                owner_name = self.metadata.owner_id_to_name(owner_id)
                self.export_data(owner_id=owner_id, owner_name=owner_name)
        elif self.owner_user_names and self.owner_user_names != 'None':
            for owner_user_name in self.owner_user_names:
                owner_id = self.metadata.owner_name_to_id(owner_user_name)
                self.export_data(owner_id=owner_id, owner_name=owner_user_name)
        elif self.owner_name_regex and self.owner_name_regex != 'None':
            owner_idz = self.metadata.get_owner_ids_by_owner_name_regex(self.owner_name_regex)
            for owner_id in owner_idz:
                owner_name = self.metadata.owner_id_to_name(owner_id["identifier"])
                self.export_data(owner_id=owner_id["identifier"], owner_name=owner_name)

    @calculate_time
    def export_data(self, owner_id=None, owner_name=None):

        rows = self.metadata.get_stream_metadata_by_owner_id(owner_id)
        if rows == "NULL":
            print("No data found for => owner-id: " + owner_id + " - owner-name: " + owner_name)
            return

        for row in rows:
            stream_id = row["identifier"]
            data_start_time = row["start_time"]
            data_end_time = row["end_time"]
            stream_metadata = {
                "identifier": stream_id,
                "owner_id": row["owner"],
                "name": row["name"],
                "data_available": {
                    "start_time": str(data_start_time),
                    "end_time": str(data_end_time)
                }
            }

            data_descriptor = json.loads(row["data_descriptor"])
            execution_context = json.loads(row["execution_context"])
            annotations = json.loads(row["annotations"])

            stream_metadata.update({"data_descriptor": data_descriptor})
            stream_metadata.update({"execution_context": execution_context})
            stream_metadata.update({"annotations": annotations})

            file_path = self.export_dir_path + owner_name
            if not os.path.exists(file_path):
                os.mkdir(file_path)

            # write metadata to json file
            self.write_to_file(file_path + "/" + stream_id + ".json", json.dumps(stream_metadata))

            # load and write stream raw data to bz2
            delta = data_end_time - data_start_time

            for i in range(delta.days + 1):
                day = data_start_time + timedelta(days=i)
                day = datetime.strftime(day, "%Y%m%d")
            self.writeStreamDataToZipFile(stream_id, day, file_path)

    def writeStreamDataToZipFile(self, stream_id: uuid, day, file_path: str):
        """

        :param stream_id:
        :param file_path:
        """
        if stream_id:
            where_clause = "identifier='" + stream_id + "' and day='" + str(day) + "'"
        else:
            raise ValueError("Missing owner ID.")

        if self.start_time and self.end_time:
            where_clause += " and start_time>=cast('" + str(
                self.start_time) + "' as timestamp) and start_time<=cast('" + str(self.end_time) + "' as timestamp)"
        elif self.start_time and not self.end_time:
            where_clause += " and start_time>=cast('" + str(self.start_time) + "' as timestamp)"
        elif not self.start_time and self.end_time:
            where_clause += " start_time<=cast('" + str(self.end_time) + "' as timestamp)"

        df = self.streamData.load_data_from_cassandra(self.streamData.datapointTable, where_clause, 1)
        df.write \
            .format("csv") \
            .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
            .save(file_path + "/" + stream_id)

        os.system("cat " + file_path + "/" + stream_id + "/p* > " + file_path + "/" + stream_id + ".gz")
        if os.path.exists(file_path + "/" + stream_id + "/"):
            shutil.rmtree(file_path + "/" + stream_id + "/", ignore_errors=True)

    def write_to_bz2(self, file_name, data):
        with open(file_name, 'wb+') as outfile:
            compressed_data = bz2.compress(data, 9)
            outfile.write(compressed_data)

    def write_to_file(self, file_name: str, data: str):
        """
        :param file_name:
        :param data:
        """
        with open(file_name, 'w+') as outfile:
            outfile.write(data)
