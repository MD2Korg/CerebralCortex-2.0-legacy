class ReadDataFromCassandra:
    def __init__(self, sqlContext, keyspaceName, tableName):
        self.sqlContext = sqlContext
        self.tableName = tableName
        self.keyspaceName = keyspaceName
        self.df = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=tableName,
                                                                                   keyspace=keyspaceName).load()
        self.df.registerTempTable("tmp_" + tableName)

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
    def cassandraQueryBuilder(self,jsonQueryParam):

        if(jsonQueryParam["columnNames"].strip()!= ""):
            columnNames = jsonQueryParam["columnNames"].strip()
        else:
            raise ValueError("No column name(s) has been defined.")

        if(jsonQueryParam["whereClause"].strip()!= ""):
            whereClause = "where " + jsonQueryParam["whereClause"].strip()
        else:
            whereClause = ""

        if(jsonQueryParam["orderedByColumnName"].strip()!= ""):
            orderedByColumnName = "ordered by " + jsonQueryParam["orderedByColumnName"].strip()
        else:
            orderedByColumnName = ""

        if(jsonQueryParam["sortingOrder"].strip()!= ""):
            sortingOrder = jsonQueryParam["sortingOrder"].strip()
        else:
            sortingOrder = ""

        if(jsonQueryParam["sortingOrder"].strip()!= ""):
            limitBy = jsonQueryParam["limitBy"].strip()
        else:
            limitBy = ""

        return "Select " + columnNames + " from " + self.tableName + " "+ whereClause +" "+orderedByColumnName+" "+sortingOrder+" "+limitBy