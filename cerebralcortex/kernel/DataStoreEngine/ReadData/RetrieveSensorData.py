class RetrieveSensorData:
    def __init__(self, sqlContext, keyspaceName="cortex", tableName="datapoint"):
        self.sqlContext = sqlContext
        self.tableName = tableName
        self.keyspaceName = keyspaceName
        self.df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=tableName,
                                                                                   keyspace=keyspaceName).load()
        self.df.registerTempTable("tmp_" + tableName)

    def cassandraQueryBuilder(self, jsonQueryParam):
        """
        This method accepts a json object to build a datastore query. Use the following json schema:
            jsonQueryParam = {
                "columnNames": "col1, col2",
                "whereClause": "",
                "orderedByColumnName": "",
                "sortingOrder":"",
                "limitBy": ""
            }
        Where
            "columnNames" are the names of the columns you want to retrieve. Please pass * if you want to retrieve all the columns of a table. "columnNames" is a mandatory field and cannot be empty.
            "whereClause" is any condition you want to put on a query. For example, "datastream_id=1 and participant_id=2". This field is option and can be empty.
            "orderedByColumnName" is the name of a column that you want data to be sorted by. This field is option and can be empty.
            "sortingOrder" is the sorting order. Available sorting arguments ASC and DESC. This field is option and can be empty.
            "limitBy" is the number of records you want to retrieve. For example, 10. This field is option and can be empty.

        Note: Please look at Cassandra database schema for available column names per table.

        """

        if (jsonQueryParam["columnNames"].strip() != ""):
            columnNames = jsonQueryParam["columnNames"].strip()
        else:
            raise ValueError("No column name(s) has been defined.")

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

        qry = "Select " + columnNames + " from tmp_" + self.tableName + " " + whereClause + " " + orderedByColumnName + " " + sortingOrder + " " + limitBy

        return self.sqlContext.sql(qry)

    # Retrieve Datastream(s) Data
    def getDatastream(self, datastreamID, startTime="", endTime=""):
        """
        :param datastreamID:
        :param startTime:
        :param endTime:
        :return: spark dataframe
        """
        whereClause = ""
        datastreamID = str(datastreamID)
        startTime = str(startTime)
        endTime = str(endTime)
        if (datastreamID == ""):
            raise "datastreamID cannot be empty"
        if (startTime != ""):
            whereClause += " and timestamp>='" + startTime+"'"
        if (endTime != ""):
            whereClause += " and timestamp<='" + endTime+"'"

        #if(whereClause!=""):
         #   whereClause += " ALLOW FILTERING"

        jsonQueryParam = {
            "columnNames": "*",
            "whereClause": "datastream_id=" + datastreamID + " " + whereClause,
            "orderedByColumnName": "timestamp",
            "sortingOrder": "ASC",
            "limitBy": ""
        }

        return self.cassandraQueryBuilder(jsonQueryParam)

    def getSpan(self, spanID, startTime="", endTime=""):
        """
        :param spanID:
        :param startTime:
        :param endTime:
        :return: spark dataframe
        """

        whereClause = ""
        datastreamID = str(datastreamID)
        startTime = str(startTime)
        endTime = str(endTime)
        datastreamID = str(spanID)
        if (spanID == ""):
            raise "spanID cannot be empty"
        if (startTime != ""):
            whereClause += " and timestamp>=" + startTime
        if (endTime != ""):
            whereClause += " and timestamp<=" + endTime

        jsonQueryParam = {
            "columnNames": "*",
            "whereClause": "span_id=" + spanID + " " + whereClause,
            "orderedByColumnName": "timestamp",
            "sortingOrder": "ASC",
            "limitBy": ""
        }

        return self.cassandraQueryBuilder(jsonQueryParam)
