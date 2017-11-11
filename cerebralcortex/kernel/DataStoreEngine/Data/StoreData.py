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
from dateutil.parser import parse
from influxdb import InfluxDBClient

from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata
from cerebralcortex.kernel.datatypes.datastream import DataStream, DataPoint
from cerebralcortex.kernel.utils.logging import cc_log
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
        elif (type != "json" and type != "datastream"):
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

            self.add_to_cassandra(stream_identifier, data)


            #dataframe = self.map_datapoint_to_dataframe(stream_identifier, data)
            #self.store_data(dataframe, self.datapointTable)

    #TODO: Remove commented code after final testing of data->cassandra without sparkContext
    # def store_data(self, dataframe_data: object, table_name: str):
    #     """
    #     :param dataframe_data: pyspark Dataframe
    #     :param table_name: Cassandra table name
    #     """
    #
    #     if table_name == "":
    #         raise Exception("Table name cannot be null.")
    #     elif dataframe_data == "":
    #         raise Exception("Data cannot be null.")
    #     dataframe_data.write.format("org.apache.spark.sql.cassandra") \
    #         .mode('append') \
    #         .options(table=table_name, keyspace=self.keyspaceName) \
    #         .option("spark.cassandra.connection.host", self.hostIP) \
    #         .option("spark.cassandra.auth.username", self.dbUser) \
    #         .option("spark.cassandra.auth.password", self.dbPassword) \
    #         .save()

    # def map_datapoint_to_dataframe(self, stream_id: uuid, datapoints: DataPoint) -> List:
    #     """
    #
    #     :param stream_id:
    #     :param datapoints:
    #     :return:
    #     """
    #     temp = []
    #     no_end_time = 0
    #     for i in datapoints:
    #         day = i.start_time
    #         day = day.strftime("%Y%m%d")
    #         if isinstance(i.sample, str):
    #             sample = i.sample
    #         else:
    #             sample = json.dumps(i.sample)
    #
    #         if i.end_time:
    #             dp = str(stream_id), day, i.start_time, i.end_time, sample
    #         else:
    #             dp = str(stream_id), day, i.start_time, sample
    #             if no_end_time != 1:
    #                 no_end_time = 1
    #
    #         temp.append(dp)
    #
    #     temp_RDD = self.CC_obj.getOrCreateSC(type="sparkContext").parallelize(temp)
    #     if (no_end_time == 1):
    #         df = self.CC_obj.getOrCreateSC(type="sqlContext").createDataFrame(temp_RDD,
    #                                                                           schema=["identifier", "day", "start_time",
    #                                                                                   "sample"]).coalesce(400)
    #     else:
    #         df = self.CC_obj.getOrCreateSC(type="sqlContext").createDataFrame(temp_RDD,
    #                                                                           schema=["identifier", "day", "start_time",
    #                                                                                   "end_time",
    #                                                                                   "sample"]).coalesce(400)
    #     return df

    #         .mode('append') \
    #         .options(table=table_name, keyspace=self.keyspaceName) \
    #         .option("spark.cassandra.connection.host", self.hostIP) \
    #         .option("spark.cassandra.auth.username", self.dbUser) \
    #         .option("spark.cassandra.auth.password", self.dbPassword) \

    def add_to_cassandra(self, stream_id: uuid, datapoints: DataPoint):
        """

        :param stream_id:
        :param datapoints:
        :param batch_size:
        """
        cluster = Cluster([self.hostIP], port=self.hostPort)

        session = cluster.connect('cerebralcortex')

        insert_without_endtime_qry = session.prepare("INSERT INTO data (identifier, day, start_time, sample) VALUES (?, ?, ?, ?)")
        insert_with_endtime_qry = session.prepare("INSERT INTO data (identifier, day, start_time, end_time, sample) VALUES (?, ?, ?, ?, ?)")
        st = datetime.now()
        if isinstance(stream_id, str):
            stream_id = uuid.UUID(stream_id)

        for data_block in self.datapoints_to_cassandra_sql_batch(stream_id, datapoints, insert_without_endtime_qry, insert_with_endtime_qry):
            session.execute(data_block)
            data_block.clear()
        session.shutdown();
        cluster.shutdown();


    def datapoints_to_cassandra_sql_batch(self, stream_id: uuid, datapoints: DataPoint, insert_without_endtime_qry: str, insert_with_endtime_qry: str):

        """

        :param stream_id:
        :param datapoints:
        :param insert_without_endtime_qry:
        :param insert_with_endtime_qry:
        :param batch_size:
        """
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        batch.clear()
        dp_number = 1
        for i in datapoints:
            day = i.start_time
            day = day.strftime("%Y%m%d")
            if isinstance(i.sample, str):
                sample = i.sample
            else:
                sample = json.dumps(i.sample)

            if i.end_time:
                insert_qry = insert_with_endtime_qry
            else:
                insert_qry = insert_without_endtime_qry
        # TODO: remove when final testing is done
            #batch.add(insert_qry, (stream_id, day, i.start_time, sample))
        #return batch

            if dp_number > 64500:
                yield batch
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                #just to make sure batch does not have any existing entries.
                batch.clear()
                dp_number = 1
            else:
                if i.end_time:
                    batch.add(insert_qry, (stream_id, day, i.start_time, i.end_time, sample))
                else:
                    batch.add(insert_qry, (stream_id, day, i.start_time, sample))
                dp_number += 1
        yield batch


    #################################################################
    ## json to CC objects and dataframe conversion
    #################################################################

    def json_to_datastream(self, json_obj: dict) -> DataStream:

        """
        :param json_obj:
        :return:
        """
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
        if "stream_type" in metadata:
            stream_type = metadata["stream_type"]
        else:
            stream_type = "ds"  # TODO: it must be defined in json object

        start_time = parse(data[0]["starttime"])
        end_time = parse(data[len(data) - 1]["starttime"])

        st = datetime.now()
        datapoints = list(map(self.json_to_datapoints, data))
        et = datetime.now()
        print("Map time: ",et-st)

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

    @staticmethod
    def json_to_datapoints(json_obj: dict) -> DataPoint:
        """
        :param json_obj:
        :return:
        """
        if isinstance(json_obj["value"], str):
            sample = json_obj["value"]
        else:
            sample = json.dumps(json_obj["value"])
        start_time = parse(json_obj["starttime"])

        if "endtime" in json_obj:
            return DataPoint(start_time=start_time, end_time=json_obj["endtime"], sample=sample)
        else:
            return DataPoint(start_time=start_time, sample=sample)

    def store_data_to_influxdb(self, datastream: DataStream):

        """
        :param datastream:
        """
        st = datetime.now()
        client = InfluxDBClient(host=self.influxdbIP, port=self.influxdbPort, username=self.influxdbUser, password=self.influxdbPassword, database=self.influxdbDatabase)
        datapoints = datastream.data
        stream_identifier = datastream.identifier
        stream_owner_id = datastream.owner
        stream_owner_name = Metadata(self.CC_obj).owner_id_to_name(stream_owner_id)
        stream_name = datastream.name

        if datastream.data_descriptor:
            total_dd_columns = len(datastream.data_descriptor)
            data_descriptor = datastream.data_descriptor
        else:
            data_descriptor = []
            total_dd_columns = 0

        influx_data = []
        for datapoint in datapoints:
            object = {}
            object['measurement'] = stream_name
            object['tags'] = {'stream_id':stream_identifier, 'owner_id': stream_owner_id, 'owner_name': stream_owner_name}

            object['time'] = datapoint.start_time
            values = datapoint.sample

            if isinstance(values, tuple):
                values = list(values)
            else:
                try:
                    values = [float(values)]
                except:
                    try:
                        values = list(map(float, values.split(',')))
                    except:
                        values = values


            try:
                object['fields'] = {}
                if isinstance(values, list):
                    for i, sample_val in enumerate(values):
                        if len(values)==total_dd_columns:
                            dd = data_descriptor[i]
                            if "NAME" in dd:
                                object['fields'][dd["NAME"]] = sample_val
                            else:
                                object['fields']['value_'+str(i)] = sample_val
                        else:
                            object['fields']['value_'+str(i)] = sample_val
                else:
                    dd = data_descriptor[0]
                    if not values:
                        values = "NULL"
                    try:
                        values = float(values)
                    except:
                        values = values
                    if "NAME" in dd:
                        object['fields'][dd["NAME"]] = values
                    else:
                        object['fields']['value_0'] = values
            except:
                try:
                    values = json.dumps(values)
                    object['fields']['value_0'] = values
                except:
                    cc_log("Datapoint sample values conversion: "+str(values),"WARNING")
                    object['fields']['value_0'] = str(values)

            influx_data.append(object)
        et = datetime

        #print('InfluxDB - Yielding:', stream_owner_id, len(influx_data), stream_identifier)

        try:
            client.write_points(influx_data)
            et2 = datetime.now()
            #print("Influx Time BreakDown: Processing: ", et-st, " Inserting: ",et2-et, " Size: ",len(influx_data))
        except:
            cc_log()
