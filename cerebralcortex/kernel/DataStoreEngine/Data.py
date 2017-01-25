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


class Data:
    def __init__(self, configuration, sqlContext):
        """
        Constructor
        :param sqlContext:
        :param keyspaceName:
        """
        self.configuration = configuration

        self.keyspaceName = self.configuration['cassandra']['keyspace']
        self.user = self.configuration['cassandra']['db_user']
        self.password = self.configuration['cassandra']['db_pass']
        self.datapoint_table = self.configuration['cassandra']['datapoint_table']
        self.span_table = self.configuration['cassandra']['span_table']

        self.sqlContext = sqlContext

    def loadData(self, tableName: str, whereClause: str) -> object:
        """
        Establish connection with cassandra, load data, and filter based on the condition passed in whereClause argument
        :param tableName:
        :param whereClause:
        :return: spark dataframe
        """
        df = self.sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=tableName,
                                                                                   keyspace=self.keyspaceName).load().filter(
            whereClause)

        return df

    def get_datastream(self, datastreamID: int, startTime: int = "", endTime: int = "") -> object:
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
            whereClause += " and datetime>='" + startTime + "'"
        if (endTime != ""):
            whereClause += " and datetime<='" + endTime + "'"

        return self.loadData(self.datapoint_table, whereClause)

    def get_span(self, spanID: int, startTime: int = "", endTime: int = "") -> object:
        """
        :param spanID:
        :param startTime:
        :param endTime:
        :return: spark dataframe
        """
        spanID = str(spanID)
        startTime = str(startTime)
        endTime = str(endTime)

        whereClause = "span_id=" + spanID

        if (spanID == ""):
            raise "spanID cannot be empty"
        if (startTime != ""):
            whereClause += " and datetime>=" + startTime
        if (endTime != ""):
            whereClause += " and datetime<=" + endTime

        return self.loadData(self.span_table, whereClause)

    def storeData(self, dfData: object, tableName: str):
        """
        :param dfData:
        :param tableName:
        """
        dfData.write.format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table=tableName, keyspace=self.keyspaceName) \
            .save()

    def save_datapoint(self, df: object):
        """
        :param df:
        """
        self.storeData(df, self.datapoint_table)

    def save_span(self, df: object):
        """
        :param df:
        """
        self.storeData(df, self.span_table)
