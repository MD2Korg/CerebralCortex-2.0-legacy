class WriteData:
    def __init__(self, sqlContext, keyspaceName, tableName):
        self.sqlContext = sqlContext
        self.tableName = tableName
        self.keyspaceName = keyspaceName

    def storeData(self, df):
        df.write.format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table=self.tableName, keyspace=self.keyspaceName) \
            .save()
