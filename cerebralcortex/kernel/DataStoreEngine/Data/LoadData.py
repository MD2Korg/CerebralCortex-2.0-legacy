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
    def loadData(self, tableName: str, whereClause: str) -> object:
        """
        Establish connection with cassandra, load data, and filter based on the condition passed in whereClause argument
        :return:
        :param tableName:
        :param whereClause:
        :return: spark dataframe
        """
        dataFrame = self.sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=tableName,
                                                                                          keyspace=self.keyspaceName).load().filter(
            whereClause)

        return dataFrame

    def getDatastream(self, datastreamID: int, startTime: int = "", endTime: int = "") -> DataStream:
        """
        :param datastreamID:
        :param startTime:
        :param endTime:
        :return: spark dataframe
        """
        datastreamID = str(datastreamID)
        startTime = str(startTime)
        endTime = str(endTime)

        whereClause = "datastream_id=" + datastreamID

        if (datastreamID == ""):
            raise "datastreamID cannot be empty"
        if (startTime != ""):
            whereClause += " and datetime>='" + str(startTime) + "'"
        if (endTime != ""):
            whereClause += " and datetime<='" + str(endTime) + "'"

        datapoints = self.mapDataframeToDatapoint(self.loadData(self.datapointTable, whereClause))
        datastream = self.mapDatapointAndMetadataToDatastream(datastreamID, datapoints)

        return datastream


    @classmethod
    def mapDataframeToDatapoint(self, df: object) -> list:
        """
        Converts a PySpark DataFrame into a list of datapoing objects
        :param df:
        :return: list of datapoint objects
        """
        temps = []
        rows = df.collect()
        for row in rows:
            dp = DataPoint(row["start_time"], row["end_time"], row["sample"], row["metadata"])
            temps.append(dp)
        return temps

    def mapDatapointAndMetadataToDatastream(self, datastreamID: int, data: list) -> DataStream:
        """
        This method will map the datapoint and metadata to datastream object
        :param datastreamID:
        :param data: list
        :return: datastream object
        """

        # query datastream(mysql) for metadata and user-id
        datastreamInfo = Metadata(self.configuration).getDatastreamInfo(datastreamID)

        # load data from MySQL
        studyObjList = []
        studyList = json.loads(datastreamInfo[0][1])
        for studyID in studyList:
            studyInfo = Metadata(self.configuration).getStudyInfo(studyID)
            study = Study(studyInfo[0][0], studyInfo[0][1], MetadataStruct(studyInfo[0][2]))
            studyObjList.append(study)

        userInfo = Metadata(self.configuration).getUserInfo(datastreamInfo[0][2])
        processingModuleInfo = Metadata(self.configuration).getProcessingModuleInfo(datastreamInfo[0][3])

        # create/populate objects
        user = User(datastreamInfo[0][2], MetadataStruct(userInfo[0][1]))
        processingModule = Processing(processingModuleInfo[0][0], MetadataStruct(processingModuleInfo[0][1]))

        sourceIDs = datastreamInfo[0][4]
        datastreamType = datastreamInfo[0][5]
        datastreamMetadata = MetadataStruct(datastreamInfo[0][6])

        return DataStream(user, studyObjList, processingModule, datastreamType, datastreamMetadata, sourceIDs,
                          datastreamID, data)
