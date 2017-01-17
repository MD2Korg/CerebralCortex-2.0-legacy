import mysql.connector


class WriteDataToMySql:
    def __init__(self, databaseName="cortex", dbUserName="root", dbPassword="pass"):
        self.dbConnection = mysql.connector.connect(user=dbUserName,password=dbPassword, database=databaseName)
        self.cursor = self.dbConnection.cursor()

    def storeDatastrem(self, userID, processingModuleID, metadata, datastreamID=""):
        if(datastreamID!=""):
            qry = "UPDATE datastream set"
        else:
            qry = "INSERT INTO datastream (user_id, processing_module_id,metadata) VALUES("+userID+","+processingModuleID+","+metadata+")"
        self.executeQuery(qry)

    def storeSpanstrem(self, sourceIDs, processingModuleID, metadata):
        qry = "INSERT INTO spanstream (source_ids, processing_module_id,metadata) VALUES("+sourceIDs+","+processingModuleID+","+metadata+")"
        self.executeQuery(qry)

    def storeProcessingModule(self, metadata):
        qry = "INSERT INTO processing_module (metadata) VALUES('"+metadata+"')"
        self.executeQuery(qry)

    def executeQuery(self,qry):
        """
        :param qry: SQL Query
        :return: results of a query
        """
        self.cursor.execute(qry)
        self.dbConnection.commit()
        self.cursor.close()
        self.dbConnection.close()
