class ReadDataFromMySql:
    def __init__(self, sqlContext, databaseName, tableName, dbUserName, dbPassword):
        self.sqlContext = sqlContext
        self.tableName = tableName
        self.databaseName = databaseName
        self.df.sqlContext.read.format("jdbc").options(
            url ="jdbc:mysql://localhost/"+databaseName,
            driver="com.mysql.jdbc.Driver",
            dbtable=tableName,
            user=dbUserName,
            password=dbPassword
        ).load()
        self.df.registerTempTable("tmp_" + tableName)

    def getAllDataStreamIDs(self):
        pass

    def getAllConfigurationIDs(self):
        pass

    def getDatastreamInfoByID(self):
        pass

    def getConigurationInfoByID(self):
        pass