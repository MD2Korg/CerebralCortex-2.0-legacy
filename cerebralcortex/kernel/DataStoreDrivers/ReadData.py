class ReadData:
    def __init__(self, sqlContext, keyspaceName, tableName):
        self.sqlContext = sqlContext
        self.tableName = tableName
        self.keyspaceName = keyspaceName
        self.df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=tableName, keyspace=keyspaceName).load()
        self.df.registerTempTable("tmp_"+tableName)

    def getDataByStreamID(self, datastreamID):
        return self.sqlContext.sql("select * from tmp_"+self.tableName+" where value>1 order by key Desc")

    def getDataByStreamByParticipantID(self, datastreamID, participantID):
        return self.sqlContext.sql("select * from tmp_"+self.tableName+" where value>1 order by key Desc")

    def getDataByParticipantID(self, participantID):
        return self.sqlContext.sql("select * from tmp_"+self.tableName+" where value>1 order by key Desc")

    def getDataByDay(self, dayAndMonth):
        return self.sqlContext.sql("select * from tmp_"+self.tableName+" where value>1 order by key Desc")

    def getDataByTimeRange(self, startTime, endTime):
        return self.sqlContext.sql("select * from tmp_"+self.tableName+" where value>1 order by key Desc")

    def getDataByWindowID(self, windowID):
        return self.sqlContext.sql("select * from tmp_"+self.tableName+" where value>1 order by key Desc")
