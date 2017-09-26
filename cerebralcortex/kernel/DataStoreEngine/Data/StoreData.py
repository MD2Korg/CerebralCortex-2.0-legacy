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
import uuid
from typing import List
from dateutil.parser import parse
from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata
from cerebralcortex.kernel.datatypes.datastream import DataStream, DataPoint

from cassandra.cluster import Cluster
from cassandra.query import *

class StoreData:
    def store_stream(self, datastream: DataStream, type):
        """
        :param datastream:
        :param type: support types are formatted json object or CC Datastream objects
        """
        if (type == "json"):
            datastream = self.json_to_datastream(datastream)
        elif (type != "json" or type != "datastream"):
            raise ValueError(type + " is not supported data type")

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
                if not datastream._start_time:
                    new_start_time = data[0].start_time
                else:
                    new_start_time = datastream._start_time
                if not datastream._end_time:
                    new_end_time = data[total_dp].start_time
                else:
                    new_end_time = datastream._end_time
            else:
                if not datastream._start_time:
                    new_start_time = data.start_time
                else:
                    new_start_time = datastream._start_time
                if not datastream._end_time:
                    new_end_time = data.start_time
                else:
                    new_end_time = datastream._end_time

            stream_identifier = datastream.identifier
            result = Metadata(self.CC_obj).is_id_created(stream_identifier)

            Metadata(self.CC_obj).store_stream_info(stream_identifier, ownerID, name,
                                                    data_descriptor, execution_context,
                                                    annotations,
                                                    stream_type, new_start_time, new_end_time, result["status"])

            self.add_to_cassandra(stream_identifier, data, 10000)
            #dataframe = self.map_datapoint_to_dataframe(stream_identifier, data)

            #self.store_data(dataframe, self.datapointTable)

    def store_data(self, dataframe_data: object, table_name: str):
        """
        :param dataframe_data: pyspark Dataframe
        :param table_name: Cassandra table name
        """

        if table_name == "":
            raise Exception("Table name cannot be null.")
        elif dataframe_data == "":
            raise Exception("Data cannot be null.")
        # dataframe_data.write.format("org.apache.spark.sql.cassandra") \
        #     .mode('append') \
        #     .options(table=table_name, keyspace=self.keyspaceName) \
        #     .option("spark.cassandra.connection.host", self.hostIP) \
        #     .option("spark.cassandra.auth.username", self.dbUser) \
        #     .option("spark.cassandra.auth.password", self.dbPassword) \
        #     .save()

    def map_datapoint_to_dataframe(self, stream_id: uuid, datapoints: DataPoint) -> List:
        """

        :param stream_id:
        :param datapoints:
        :return:
        """
        temp = []
        no_end_time = 0
        # for i in datapoints:
        #     day = i.start_time
        #     day = day.strftime("%Y%m%d")
        #     if isinstance(i.sample, str):
        #         sample = i.sample
        #     else:
        #         sample = json.dumps(i.sample)
        #
        #     if i.end_time:
        #         dp = str(stream_id), day, i.start_time, i.end_time, sample
        #     else:
        #         dp = str(stream_id), day, i.start_time, sample
        #         if no_end_time != 1:
        #             no_end_time = 1
        #
        #     temp.append(dp)
        #
        # temp_RDD = self.CC_obj.getOrCreateSC(type="sparkContext").parallelize(temp)
        # if (no_end_time == 1):
        #     df = self.CC_obj.getOrCreateSC(type="sqlContext").createDataFrame(temp_RDD,
        #                                                                       schema=["identifier", "day", "start_time",
        #                                                                               "sample"]).coalesce(400)
        # else:
        #     df = self.CC_obj.getOrCreateSC(type="sqlContext").createDataFrame(temp_RDD,
        #                                                                       schema=["identifier", "day", "start_time",
        #                                                                               "end_time",
        #                                                                               "sample"]).coalesce(400)
        # return df
    def add_to_cassandra(self, stream_id: uuid, datapoints: DataPoint, batch_size):
        cluster = Cluster(
            ['127.0.0.1'],
            port=9042)

        session = cluster.connect('cerebralcortex')

        insert_qry = session.prepare("INSERT INTO data (identifier, day, start_time, sample) VALUES (?, ?, ?, ?)")
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        for data_block in self.datapoints_to_cassandra_sql_batch(stream_id, datapoints, insert_qry, batch, batch_size):
            session.execute(data_block)
            data_block.clear()

    def datapoints_to_cassandra_sql_batch(self, stream_id: uuid, datapoints: DataPoint, insert_qry, batch, batch_size):

        dp_number = 1
        for i in datapoints:
            day = i.start_time
            day = day.strftime("%Y%m%d")
            if isinstance(i.sample, str):
                sample = i.sample
            else:
                sample = json.dumps(i.sample)

            # if i.end_time:
            #     dp = str(stream_id), day, i.start_time, i.end_time, sample
            # else:
            #     dp = str(stream_id), day, i.start_time, sample
            #     if no_end_time != 1:
            #         no_end_time = 1





            if dp_number > batch_size:
                yield batch
                batch.clear()
                dp_number = 1
            else:
                batch.add(insert_qry, (uuid.UUID(stream_id), day, i.start_time, sample))
                dp_number += 1
        yield batch


    #################################################################
    ## json to CC objects and dataframe conversion
    #################################################################

    def json_to_datapoints(self, json_obj):
        if isinstance(json_obj["value"], str):
            sample = json_obj["value"]
        else:
            sample = json.dumps(json_obj["value"])
        start_time = parse(json_obj["starttime"])

        if "endtime" in json_obj:
            return DataPoint(start_time=start_time, end_time=json_obj["endtime"], sample=sample)
        else:
            return DataPoint(start_time=start_time, sample=sample)

    def json_to_datastream(self, json_obj):

        data = json_obj["data"]
        metadata = json_obj["metadata"]
        identifier = metadata["identifier"]
        owner = metadata["owner"]
        name = metadata["name"]
        data_descriptor = metadata["data_descriptor"]
        execution_context = metadata["execution_context"]


        if "annotations" in metadata:
            annotations = metadata["annotations"]
        else:
            annotations={}
        stream_type = "ds"  # TODO: it must be defined in json object
        start_time = parse(data[0]["starttime"])
        end_time = parse(data[len(data) - 1]["starttime"])
        datapoints = list(map(self.json_to_datapoints, data))

        return DataStream(identifier,
                          owner,
                          name,
                          data_descriptor,
                          execution_context,
                          annotations,
                          stream_type,
                          start_time,
                          end_time,
                          datapoints)