

# --packages datastax:spark-cassandra-connector:1.6.3-s_2.11
# from pyspark import SparkContext, SparkConf


from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when
#from .WriteData import WriteDataToCassandra
from .ReadData import ReadDataFromCassandra



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

    df = ReadDataFromCassandra(sqlContext,"test","kv").cassandraQueryBuilder(jsonQueryParam)

    #WriteDataToCassandra(sqlContext,"test","kv2").storeData(df)

if __name__ == "__main__":
    main()