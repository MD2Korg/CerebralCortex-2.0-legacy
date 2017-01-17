import mysql.connector


class ReadDataFromMySql:
    def __init__(self, databaseName="cortex", dbUserName="root", dbPassword="pass"):
        self.dbConnection = mysql.connector.connect(user=dbUserName,password=dbPassword, database=databaseName)
        self.cursor = self.dbConnection.cursor()

    def mySQLQueryBuilder(self, jsonQueryParam):
        """
        This method accepts a json object to build a datastore query. Use the following json schema:
            jsonQueryParam = {
                "columnNames": "col1, col2",
                "tableName": "table_name"
                "whereClause": "",
                "orderedByColumnName": "",
                "sortingOrder":"",
                "limitBy": ""
            }
        Where
            "columnNames" are the names of the columns you want to retrieve. Please pass * if you want to retrieve all the columns of a table. "columnNames" is a mandatory field and cannot be empty.
            "tableName" is the name of the database table
            "whereClause" is any condition you want to put on a query. For example, "datastream_id=1 and participant_id=2". This field is option and can be empty.
            "orderedByColumnName" is the name of a column that you want data to be sorted by. This field is option and can be empty.
            "sortingOrder" is the sorting order. Available sorting arguments ASC and DESC. This field is option and can be empty.
            "limitBy" is the number of records you want to retrieve. For example, 1, 10. This field is option and can be empty.

        Note: Please look at Cassandra database schema for available column names per table.

        """

        if (jsonQueryParam["columnNames"].strip() != ""):
            columnNames = jsonQueryParam["columnNames"].strip()
        else:
            raise ValueError("No column name(s) has been defined.")

        if (jsonQueryParam["tableName"].strip() == ""):
            raise ValueError("No table name has been defined.")

        if (jsonQueryParam["whereClause"].strip() != ""):
            whereClause = "where " + jsonQueryParam["whereClause"].strip()
        else:
            whereClause = ""

        if (jsonQueryParam["orderedByColumnName"].strip() != ""):
            orderedByColumnName = "ORDER BY " + jsonQueryParam["orderedByColumnName"].strip()
        else:
            orderedByColumnName = ""

        if (jsonQueryParam["sortingOrder"].strip() != ""):
            sortingOrder = jsonQueryParam["sortingOrder"].strip()
        else:
            sortingOrder = ""

        if (jsonQueryParam["sortingOrder"].strip() != ""):
            limitBy = jsonQueryParam["limitBy"].strip()
        else:
            limitBy = ""

        qry = "Select " + columnNames + " from " + jsonQueryParam["tableName"] + " " + whereClause + " " + orderedByColumnName + " " + sortingOrder + " " + limitBy
        return qry

#SELECT metadata->"$.DataStream.Metadata.Description" FROM `users`

    #Retrieve metadata

    def getDatastreamMetadata(self, datastreamID="", userID="", processinModuleID="", limitRecords=""):
        """
        :param datastreamID:
        :param userID:
        :param processinModuleID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = ""

        if(datastreamID!=""):
            whereClause += "id="+datastreamID
        if(userID!=""):
            whereClause += " and user_id="+userID
        if(processinModuleID!=""):
            whereClause += " and processing_module_id="+processinModuleID

        jsonQueryParam = {
            "columnNames": "metadata",
            "tableName": "datastream",
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder":"ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def getSpanstreamMetadata(self, spanID="", sourceID="", processinModuleID="", limitRecords=""):
        """
        :param spanID:
        :param sourceID:
        :param processinModuleID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = ""

        if(spanID!=""):
            whereClause += "id="+spanID
        if(sourceID!=""):
            whereClause += " and source_id="+sourceID
        if(processinModuleID!=""):
            whereClause += " and processing_module_id="+processinModuleID

        jsonQueryParam = {
            "columnNames": "metadata",
            "tableName": "spanstream",
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder":"ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))

    def getUserMetadata(self, userID="", limitRecords=""):
        """
        :param userID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = ""

        if(userID!=""):
            whereClause += "id="+userID

        jsonQueryParam = {
            "columnNames": "metadata",
            "tableName": "users",
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder":"ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))


    def getProcessingModulemetadata(self, processinModuleID="", limitRecords=""):
        """
        :param processinModuleID:
        :param limitRecords: range (e.g., 1,10 or 130,200)
        :return: list
        """
        whereClause = ""

        if(processinModuleID!=""):
            whereClause += "id="+processinModuleID

        jsonQueryParam = {
            "columnNames": "metadata",
            "tableName": "processing_module",
            "whereClause": whereClause,
            "orderedByColumnName": "id",
            "sortingOrder":"ASC",
            "limitBy": limitRecords
        }
        return self.executeQuery(self.mySQLQueryBuilder(jsonQueryParam))


    def executeQuery(self,qry):
        """
        :param qry: SQL Query
        :return: results of a query
        """
        self.cursor.execute(qry)
        results = self.cursor.fetchall()
        self.cursor.close()
        self.dbConnection.close()
        return results

