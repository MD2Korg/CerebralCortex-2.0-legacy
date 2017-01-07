class ReadDataFromCassandra:
    def __init__(self, sqlContext, keyspaceName, tableName):
        self.sqlContext = sqlContext
        self.tableName = tableName
        self.keyspaceName = keyspaceName
        self.df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=tableName,
                                                                                   keyspace=keyspaceName).load()
        self.df.registerTempTable("tmp_" + tableName)

    '''
    :param datastreamID
    Retrieve all the data for a datastream from Cassandra
    '''
    def getDataByStreamID(self, datastreamID):
        return self.sqlContext.sql("select * from tmp_" + self.tableName + " where value>1 order by key Desc")

    '''
    :param datastreamID
    :param participantID
    Retrieve a datastream by  a participant ID from Cassandra
    '''
    def getDatastreamByParticipantID(self, datastreamID, participantID):
        return self.sqlContext.sql("select * from tmp_" + self.tableName + " where value>1 order by key Desc")

    '''
    :param datastreamID
    Retrieve all the datastreams for a participant from Cassandra
    '''
    def getDataByParticipantID(self, participantID):
        return self.sqlContext.sql("select * from tmp_" + self.tableName + " where value>1 order by key Desc")

    '''
    :param dayMonthYear (e.g., 25/03/2017)
    Retrieve all the datastreams of all the participants of a day from Cassandra
    '''
    def getDataByDay(self, dayMonthYear):
        return self.sqlContext.sql("select * from tmp_" + self.tableName + " where value>1 order by key Desc")



    #NEED TO DEFINE SOME DYNAMIC METHODS TO GET ALL KIND OF DATA
    #FOR EXAMPLE, START-END TIME PER DATASTREAM-ID OR PARTICIPANT-ID

    '''
    :param datastreamID
    Retrieve all the data for a datastream ID from Cassandra
    '''
    def getDataByTimeRange(self, startTime, endTime):
        return self.sqlContext.sql("select * from tmp_" + self.tableName + " where value>1 order by key Desc")

    '''
    :param datastreamID
    Retrieve all the data for a datastream ID from Cassandra
    '''
    def getDataByWindowID(self, windowID):
        return self.sqlContext.sql("select * from tmp_" + self.tableName + " where value>1 order by key Desc")
