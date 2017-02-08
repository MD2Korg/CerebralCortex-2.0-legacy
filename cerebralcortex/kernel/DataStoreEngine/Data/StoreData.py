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

from cerebralcortex.kernel.DataStoreEngine.Metadata.Metadata import Metadata


class StoreData:
    def store_data(self, dataframe_data: object, table_name: str):
        """
        :param dataframe_data: pyspark Dataframe
        :param table_name: Cassandra table name
        """
        dataframe_data.write.format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table=table_name, keyspace=self.keyspaceName) \
            .save()

    # These two methods will be moved to struct classes
    def save_datapoint(self, df: object):
        """
        :param df:
        """
        self.store_data(df, self.datapointTable)

    # def saveSpan(self, df: object):
    #     """
    #     :param df:
    #     """
    #     self.storeData(df, self.spanTable)

    def map_datapoint_to_dataframe(self, datastream_id, datapoints):
        temp = []
        for i in datapoints:
            day = i.getStartTime()
            day = day.strftime("%Y%m%d")
            dp = datastream_id, day, i.getStartTime(), i.getEndTime(), i.sample, i.getMetadata()
            temp.append(dp)

        temp_RDD = self.sparkContext.parallelize(temp)
        df = self.sqlContext.createDataFrame(temp_RDD,
                                             ["datastream_id", "day", "start_time", "end_time", "sample", "metadata"])

        return df

    def store_datastream(self, datastream):
        datastream_identifier = datastream.get_identifier()
        study_ids = datastream.getStudyIDs()  # TO-DO, only add study-ids if they exist
        user_id = datastream.userObj.getID()

        processing_module_id = datastream.processingModuleObj.getID()
        datastream_type = datastream.get_datastream_type()
        metadata = datastream.getMetadata().getMetadata()
        source_ids = datastream.get_source_ids()
        data = datastream.data

        # if datastream_identifier is empty then create a new datastream_identifier in MySQL database and return the newly added datastream_identifier
        lastAddedRecordID = Metadata(self.configuration).storeDatastrem(datastream_identifier, study_ids, user_id,
                                                                        processing_module_id, source_ids,
                                                                        datastream_type,
                                                                        metadata)

        if datastream_identifier == "":
            datastream_identifier = lastAddedRecordID

        dataframe = self.map_datapoint_to_dataframe(datastream_identifier, data)

        self.save_datapoint(dataframe)
