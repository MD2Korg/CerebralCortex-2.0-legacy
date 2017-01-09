
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when
from pyspark.sql.functions import lit

from cerebralcortex.kernel.DataStoreEngine.ReadData.ReadDataFromCassandra import ReadDataFromCassandra
from cerebralcortex.kernel.DataStoreEngine.WriteData.WriteDataToCassandra import WriteDataToCassandra

from cerebralcortex.kernel.DataStoreEngine.ReadData.ReadDataFromMySql import ReadDataFromMySql
from cerebralcortex.kernel.DataStoreEngine.WriteData.WriteDataToMySql import WriteDataToMySql


def main():

    conf = SparkConf().setAppName("DataStore")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    jsonQueryParam = {
        "columnNames": "*",
        "whereClause": "",
        "orderedByColumnName": "",
        "sortingOrder":"",
        "limitBy": ""
    }

    #df = ReadDataFromCassandra(sqlContext,"test","kv").executeQuery(jsonQueryParam)
   # df.show()
   # WriteDataToCassandra(sqlContext,"test","kv2").storeData(df)

    df = ReadDataFromMySql(sqlContext,"cortex","configuration","root","pass").mySqlQueryBuilder(jsonQueryParam)
    df.show()

    new_df = df.withColumn('id', lit(0))
    new_df.show()
    WriteDataToMySql(sqlContext, "cortex", "configuration").storeData(new_df)

if __name__ == "__main__":
    main()