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

import ast
import json
import uuid
from datetime import datetime

from pytz import timezone

from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet
from cerebralcortex.kernel.datatypes.datastream import DataStream, DataPoint


class LoadData:
    def get_stream(self, stream_id: uuid, start_time: datetime = None, end_time: datetime = None,
                   data_type=DataSet.COMPLETE) -> DataStream:

        """
        :param stream_id:
        :param start_time:
        :param end_time:
        :param data_type: this parameter accepts only three types (i.e., all, data, metadata)
        :return: spark dataframe
        """
        start_time = str(start_time)
        end_time = str(end_time)

        where_clause = "identifier='" + stream_id + "'"

        if stream_id == 'None':
            raise Exception("Stream identifier cannot be null.")

        if start_time != 'None':
            where_clause += " and start_time>=cast('" + start_time + "' as timestamp)"

        if end_time != 'None':
            where_clause += " and start_time<=cast('" + end_time + "' as timestamp)"

        if data_type == DataSet.COMPLETE:
            datapoints = self.map_dataframe_to_datapoint(
                self.load_data_from_cassandra(self.datapointTable, where_clause))
            stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datapoints)
        elif data_type == DataSet.ONLY_DATA:
            return self.map_dataframe_to_datapoint(self.load_data_from_cassandra(self.datapointTable, where_clause))
        elif data_type == DataSet.ONLY_METADATA:
            datapoints = []
            stream = self.map_datapoint_and_metadata_to_datastream(stream_id, datapoints)
        else:
            raise ValueError("Invalid type parameter.")

        return stream

    def map_dataframe_to_datapoint(self, dataframe: object) -> list:
        """
        Converts a PySpark DataFrame into a list of datapoint objects
        :param dataframe:
        :return: list of datapoint objects
        """
        datapointsList = []
        rows = dataframe.collect()

        for row in rows:
            localtz = timezone(self.CC_obj.time_zone)
            start_time = localtz.localize(row["start_time"])
            if row["end_time"] != None:
                end_time = localtz.localize(row["end_time"])
            else:
                end_time = ""

            dp = DataPoint(start_time, end_time, ast.literal_eval(row["sample"]))
            datapointsList.append(dp)
        return datapointsList

    def map_datapoint_and_metadata_to_datastream(self, stream_id: int, data: list) -> DataStream:
        """
        This method will map the datapoint and metadata to datastream object
        :param stream_id:
        :param data: list
        :return: datastream object
        """

        # query datastream(mysql) for metadata
        datastream_info = Metadata(self.CC_obj).get_stream_info(stream_id)

        ownerID = datastream_info[0]["owner"]
        name = datastream_info[0]["name"]
        data_descriptor = json.loads(datastream_info[0]["data_descriptor"])
        execution_context = json.loads(datastream_info[0]["execution_context"])
        annotations = json.loads(datastream_info[0]["annotations"])
        stream_type = datastream_info[0]["type"]
        start_time = datastream_info[0]["start_time"]
        end_time = datastream_info[0]["end_time"]

        return DataStream(stream_id, ownerID, name, data_descriptor, execution_context, annotations,
                          stream_type, start_time, end_time, data)

    def load_data_from_cassandra(self, table_name: str, where_clause: str) -> object:
        """
        Establish connection with cassandra, load data, and filter based on the condition passed in whereClause argument
        :return:
        :param table_name:
        :param where_clause:
        :return: spark dataframe
        """
        # TO-DO, replace .filter with .where() for performance

        dataframe = self.sqlContext.read.format("org.apache.spark.sql.cassandra"). \
            options(table=table_name, keyspace=self.keyspaceName, pushdownss=True).load(). \
            select("start_time", "end_time", "sample"). \
            filter(where_clause). \
            orderBy('start_time', ascending=True)
        return dataframe

    @staticmethod
    def get_epoch_time(dt: datetime) -> datetime:
        """
        :param dt:
        :return:
        """
        epoch = datetime.utcfromtimestamp(0)
        return (dt - epoch).total_seconds() * 1000.0
