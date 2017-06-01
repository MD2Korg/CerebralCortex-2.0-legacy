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

from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata
from cerebralcortex.kernel.datatypes.datastream import DataStream


class StoreData:
    def store_stream(self, datastream: DataStream):
        """
        :param datastream:
        """

        ownerID = datastream.owner
        name = datastream.name
        data_descriptor = datastream.data_descriptor
        execution_context = datastream.execution_context
        annotations = datastream.annotations
        stream_type = datastream.datastream_type
        data = datastream.data

        if data:
            if isinstance(data, list):
                total_dp = len(data) - 1
                new_start_time = data[0].start_time
                new_end_time = data[total_dp].start_time
            else:
                new_start_time = data.start_time
                new_end_time = data.start_time


            result = Metadata(self.CC_obj).is_id_created(ownerID, name, execution_context)

            stream_identifier = result["id"]
            Metadata(self.CC_obj).store_stream_info(stream_identifier, ownerID, name,
                                                    data_descriptor, execution_context,
                                                    annotations,
                                                    stream_type, new_start_time, new_end_time, result["status"])

            dataframe = self.map_datapoint_to_dataframe(stream_identifier, data)

            self.store_data(dataframe, self.datapointTable)

    def store_data(self, dataframe_data: object, table_name: str):
        """
        :param dataframe_data: pyspark Dataframe
        :param table_name: Cassandra table name
        """

        if table_name == "":
            raise Exception("Table name cannot be null.")
        elif dataframe_data == "":
            raise Exception("Data cannot be null.")
        dataframe_data.write.format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table=table_name, keyspace=self.keyspaceName) \
            .save()

    def map_datapoint_to_dataframe(self, stream_id, datapoints):

        temp = []
        no_end_time = 0
        for i in datapoints:
            day = i.start_time
            day = day.strftime("%Y%m%d")
            if isinstance(i.sample, str):
                sample = i.sample
            else:
                sample = json.dumps(i.sample)

            if i.end_time:
                dp = str(stream_id), day, i.start_time, i.end_time, sample
            else:
                dp = str(stream_id), day, i.start_time, sample
                if no_end_time != 1:
                    no_end_time = 1

            temp.append(dp)

        temp_RDD = self.sparkContext.parallelize(temp)
        if (no_end_time == 1):
            df = self.sqlContext.createDataFrame(temp_RDD,
                                                 schema=["identifier", "day", "start_time", "sample"]).coalesce(400)
        else:
            df = self.sqlContext.createDataFrame(temp_RDD,
                                                 schema=["identifier", "day", "start_time", "end_time",
                                                         "sample"]).coalesce(400)
        return df
