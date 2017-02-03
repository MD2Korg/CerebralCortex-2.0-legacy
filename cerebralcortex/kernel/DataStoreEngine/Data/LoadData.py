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

import json

from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata
from cerebralcortex.kernel.datatypes.datastream import *
from cerebralcortex.kernel.datatypes.metadata import Metadata as MetadataStruct
from cerebralcortex.kernel.datatypes.processing import Processing
from cerebralcortex.kernel.datatypes.study import Study
from cerebralcortex.kernel.datatypes.user import User


class LoadData:
    def load_data(self, table_name: str, where_clause: str) -> object:
        """
        Establish connection with cassandra, load data, and filter based on the condition passed in whereClause argument
        :return:
        :param table_name:
        :param where_clause:
        :return: spark dataframe
        """
        dataframe = self.sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=table_name,
                                                                                          keyspace=self.keyspaceName).load().filter(
            where_clause)

        return dataframe

    def get_datastream(self, datastream_id: int, start_time: int = "", end_time: int = "") -> DataStream:
        """
        :param datastream_id:
        :param start_time:
        :param end_time:
        :return: spark dataframe
        """
        datastream_id = str(datastream_id)
        start_time = str(start_time)
        end_time = str(end_time)

        where_clause = "datastream_id=" + datastream_id

        if datastream_id == "":
            raise Exception("datastreamID cannot be empty")

        if start_time != "":
            where_clause += " and datetime>='" + str(start_time) + "'"

        if end_time != "":
            where_clause += " and datetime<='" + str(end_time) + "'"

        datapoints = self.map_dataframe_to_datapoint(self.load_data(self.datapointTable, where_clause))
        datastream = self.map_datapoint_and_metadata_to_datastream(datastream_id, datapoints)

        return datastream


    @classmethod
    def map_dataframe_to_datapoint(cls, dataframe: object) -> list:
        """
        Converts a PySpark DataFrame into a list of datapoing objects
        :param dataframe:
        :return: list of datapoint objects
        """
        temps = []
        rows = dataframe.collect()
        for row in rows:
            dp = DataPoint(row["start_time"], row["end_time"], row["sample"], row["metadata"])
            temps.append(dp)
        return temps

    def map_datapoint_and_metadata_to_datastream(self, datastream_id: int, data: list) -> DataStream:
        """
        This method will map the datapoint and metadata to datastream object
        :param datastream_id:
        :param data: list
        :return: datastream object
        """

        # query datastream(mysql) for metadata and user-id
        datastream_info = Metadata(self.configuration).get_datastream_info(datastream_id)

        # load data from MySQL
        study_objs = []
        studies = json.loads(datastream_info[0][1])
        for study_id in studies:
            study_info = Metadata(self.configuration).get_study_info(study_id)
            study = Study(study_info[0][0], study_info[0][1], MetadataStruct(study_info[0][2]))
            study_objs.append(study)

        user_info = Metadata(self.configuration).getUserInfo(datastream_info[0][2])
        processing_module_info = Metadata(self.configuration).getProcessingModuleInfo(datastream_info[0][3])

        # create/populate objects
        user = User(datastream_info[0][2], MetadataStruct(user_info[0][1]))
        processing_module = Processing(processing_module_info[0][0], MetadataStruct(processing_module_info[0][1]))

        source_ids = datastream_info[0][4]
        datastream_type = datastream_info[0][5]
        datastream_metadata = MetadataStruct(datastream_info[0][6])

        return DataStream(None, user, study_objs, processing_module, datastream_type, datastream_metadata, source_ids,
                          datastream_id, data)
