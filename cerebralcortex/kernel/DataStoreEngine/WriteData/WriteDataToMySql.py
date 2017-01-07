class WriteDataToMySql:
    def __init__(self, sqlContext, databaseName, tableName):
        self.tableName = tableName
        self.databaseName = databaseName

    def storeData(self, df):
        properties = {
            "user": "root",
            "password": "pass",
            "mode": "append",
            "driver": "com.mysql.jdbc.Driver"
        }

        url = "jdbc:mysql://localhost/"+self.databaseName
        df.write.jdbc(url=url, table=self.tableName, properties=properties)
