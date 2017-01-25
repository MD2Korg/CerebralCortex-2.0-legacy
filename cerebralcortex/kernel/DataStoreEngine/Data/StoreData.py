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

    def storeData(self, dfData: object, tableName: str):
        """
        :param dfData:
        :param tableName:
        """
        dfData.write.format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table=tableName, keyspace=self.keyspaceName) \
            .save()

    #These two methods will be moved to struct classes
    def saveDatapoint(self, df: object):
        """
        :param df:
        """
        self.storeData(df, self.datapointTable)

    def saveSpan(self, df: object):
        """
        :param df:
        """
        self.storeData(df, self.spanTable)

    def mapDatapointToDataframe(self, datastreamID, sc, sqlContext, datapointList):
        temp = []
        for i in datapointList:
            day = i.get_timestamp()
            day = day.strftime("%Y%m%d")
            dp = datastreamID,day, i.get_timestamp(), "", i.get_sample()
            temp.append(dp)


        tempRDD = sc.parallelize(temp)
        df = sqlContext.createDataFrame(tempRDD)

        return df

    def storeDatastream(self, datastreamObj):
        datastreamID = datastreamObj.get_identifier()
        userID = datastreamObj.get_user_id()
        studyIDs = datastreamObj.get_study_ids()
        processingModuleID = datastreamObj.get_processing_module_id()
        datastreamType = datastreamObj.get_datastream_type()
        sourceIDs = datastreamObj.get_source_ids()
        data = datastreamObj.get_datapoints()

        #if datastreamID is empty then create a new datastreamID in MySQL database and return the newly added datastreamID
        if(datastreamID==""):
            Metadata().storeDatastrem(userID,processingModuleID,)





        #query datastream(mysql) to store/update metadata
        #call get datapoint method to get a list
        #populate struct of datastream


        pass
